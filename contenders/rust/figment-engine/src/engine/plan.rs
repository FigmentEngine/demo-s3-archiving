//! Pure planning arithmetic. NO `aws_sdk_s3` IMPORTS — pure engine, testable with no AWS.
//!
//! This file currently contains the offset fold and its types; the full planner
//! (chain construction, viability, part numbering) builds on top of these.

use crate::engine::zip_format;

/// Stable identity for a file, independent of its position in any collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u32);

/// A source object from the S3 listing — the planner's only input.
#[derive(Debug, Clone)]
pub struct SourceFile {
	pub id: FileId,
	pub key: String,
	pub name: String, // ZIP entry name (key minus prefix)
	pub size: u64,
}

/// Compute the local-header offset of every entry, in canonical `order`.
///
/// Offset of entry i = sum over all preceding entries of (local_header_len + body_size).
/// Expressed as a running fold so the accumulation lives in exactly one place; this is
/// the only spot the offset arithmetic exists, which keeps off-by-ones contained and
/// makes the central directory's local-header pointers correct by construction.
pub fn offsets<F>(order: &[FileId], lookup: F) -> Vec<u64>
where
	F: Fn(FileId) -> (String, u64),
{
	order
		.iter()
		.scan(0u64, |acc, &id| {
			let here = *acc;
			let (name, size) = lookup(id);
			*acc += zip_format::entry_total_len(&name, size);
			Some(here)
		})
		.collect()
}

/// Where the central directory begins = total bytes of all entry (header+body) records.
pub fn central_directory_offset<F>(order: &[FileId], lookup: F) -> u64
where
	F: Fn(FileId) -> (String, u64),
{
	order.iter().fold(0u64, |acc, &id| {
		let (name, size) = lookup(id);
		acc + zip_format::entry_total_len(&name, size)
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	// Owned-String fixture rows so the returned closure has a single lifetime.
	fn lk(files: &[(FileId, String, u64)]) -> impl Fn(FileId) -> (String, u64) + '_ {
		move |id| {
			let f = files.iter().find(|(fid, _, _)| *fid == id).unwrap();
			(f.1.clone(), f.2)
		}
	}

	/// Pure arithmetic guard: the offset fold accumulates header+body lengths correctly.
	#[test]
	fn offsets_accumulate() {
		let files: Vec<(FileId, String, u64)> = vec![
			(FileId(0), "a".to_string(), 10),
			(FileId(1), "bb".to_string(), 20),
			(FileId(2), "ccc".to_string(), 30),
		];
		let order = [FileId(0), FileId(1), FileId(2)];
		let offs = offsets(&order, lk(&files));
		assert_eq!(offs[0], 0);
		assert_eq!(offs[1], 41); // (30+1)+10
		assert_eq!(offs[2], 93); // 41 + (30+2)+20
		assert_eq!(central_directory_offset(&order, lk(&files)), 156); // 93 + (30+3)+30
	}

	// ===================================================================================
	// CORRECTNESS ANCHOR — mirror the control Lambda.
	//
	// The benchmark validator does NOT compare our bytes to any reference encoder. It:
	//   1. opens the archive with `zip::ZipArchive` (so our central dir + ZIP64 end
	//      records must PARSE),
	//   2. asserts each entry name is flat (no '/'),
	//   3. extracts each entry and asserts SHA256(content) == entry name,
	//   4. asserts the expected-name set is exactly covered.
	//
	// So our test assembles entries with OUR zip_format code, then runs that exact
	// validation. Entry content is arbitrary bytes; the entry NAME is the SHA256 hex of
	// that content (mirroring the real data). The CRC we embed is the true CRC32 of the
	// content — we ASSUME THE CRC MUST BE RIGHT, and `zip`'s reader verifies it on
	// extraction, so a wrong CRC fails here.
	//
	// Needs dev-dependencies: `zip`, `sha2`, `crc32fast`. Feature-gated so the default
	// `cargo test` (and CI) stays dependency-light; run with --features zip_validate.
	// ===================================================================================
	#[cfg(feature = "zip_validate")]
	#[test]
	fn assembled_archive_passes_validator() {
		use crate::engine::zip_format::EntryMeta;
		use sha2::{Digest, Sha256};
		use std::collections::HashSet;
		use std::io::{Cursor, Read};

		fn sha256_hex(bytes: &[u8]) -> String {
			let mut h = Sha256::new();
			h.update(bytes);
			let d = h.finalize();
			let mut s = String::with_capacity(64);
			for b in d {
				use core::fmt::Write;
				let _ = write!(s, "{:02x}", b);
			}
			s
		}
		fn crc32(bytes: &[u8]) -> u32 {
			let mut h = crc32fast::Hasher::new();
			h.update(bytes);
			h.finalize()
		}

		// ---- build some entries; name == sha256(content), like the real objects ----
		let contents: Vec<Vec<u8>> = vec![
			b"hello world".to_vec(),
			Vec::new(), // empty entry, exercises 0-length body
			vec![0xABu8; 1000],
			b"the quick brown fox".to_vec(),
		];

		// Compute offsets via the SAME fold the planner uses.
		let order: Vec<FileId> = (0..contents.len() as u32).map(FileId).collect();
		let names: Vec<String> = contents.iter().map(|c| sha256_hex(c)).collect();
		let offs = {
			let names_ref = &names;
			let contents_ref = &contents;
			let lookup = move |id: FileId| {
				let i = id.0 as usize;
				(names_ref[i].clone(), contents_ref[i].len() as u64)
			};
			offsets(&order, lookup)
		};
		let entries: Vec<EntryMeta> = (0..contents.len())
			.map(|i| EntryMeta {
				name: names[i].clone(),
				size: contents[i].len() as u64,
				crc: crc32(&contents[i]), // CRC MUST be right
				local_header_offset: offs[i],
			})
			.collect();

		// ---- assemble the archive bytes with OUR code ----
		let mut archive: Vec<u8> = Vec::new();
		for (e, body) in entries.iter().zip(contents.iter()) {
			archive.extend_from_slice(&zip_format::local_header(e));
			archive.extend_from_slice(body);
		}
		let cd_offset = archive.len() as u64;
		let mut cd_size = 0u64;
		for e in &entries {
			let rec = zip_format::central_dir_entry(e);
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		archive.extend_from_slice(&zip_format::end_records(
			entries.len() as u64,
			cd_offset,
			cd_size,
		));

		// ---- run the validator's logic against our bytes ----
		let mut expected: HashSet<String> = entries.iter().map(|e| e.name.clone()).collect();

		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("our archive must parse with the standard zip reader");
		assert_eq!(za.len(), entries.len());

		let archive_names: Vec<String> = za.file_names().map(ToOwned::to_owned).collect();
		for name in &archive_names {
			assert!(!name.contains('/'), "flat layout required, got {name}");
			assert!(expected.remove(name), "unknown/duplicate entry {name}");
		}
		assert!(
			expected.is_empty(),
			"archive missing expected entries: {expected:?}"
		);

		for name in &archive_names {
			let mut entry = za.by_name(name).unwrap();
			let mut buf = Vec::new();
			entry
				.read_to_end(&mut buf)
				.expect("entry must extract (CRC verified by reader)");
			assert_eq!(
				&sha256_hex(&buf),
				name,
				"content hash must equal entry name"
			);
		}
	}
}
