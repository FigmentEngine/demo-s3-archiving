//! Pure planning. NO `aws_sdk_s3` IMPORTS — pure engine, testable with no AWS.
//!
//! `plan(files)` turns the S3 listing into a fully-ordered, fully-numbered `Plan` (or
//! routes to the streaming fallback). All ordering, chain construction, part numbering
//! and offset computation happen here, deterministically, from sizes alone. CRC *values*
//! are filled in later (phase 1 for copyables, stream-time for streamables); they do not
//! affect order, numbering, or offsets.

use std::collections::HashMap;

use crate::engine::zip_format;

/// S3 multipart upload minimum non-last part size (5 MiB).
pub const PART_FLOOR: u64 = 5 * 1024 * 1024;

/// Total archive must comfortably exceed the floor to use the copy-part fast path.
pub const VIABILITY_MIN_TOTAL: u64 = 4 * PART_FLOOR;

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

/// One archive entry, in canonical order. Offsets are computed by the planner; `crc` is
/// filled later (None until then).
#[derive(Debug, Clone)]
pub struct Entry {
	pub id: FileId,
	pub name: String,
	pub size: u64,
	pub local_header_offset: u64,
	pub crc: Option<u32>,
	pub streamed: bool, // true: header written inline by a Stream part; false: COPIED body
}

/// A piece of a Stream part.
#[derive(Debug, Clone)]
pub enum Segment {
	/// A streamed file: GET it, emit its (inline) header + body. CRC self-computed from body.
	StreamedFile { id: FileId },
	/// A standalone local header for a COPIED file (the trailing-header handoff). No body here.
	CopiedFileHeader { id: FileId },
}

/// One MPU part within a chain.
#[derive(Debug, Clone)]
pub enum PartSpec {
	/// Big body moved server-side. Off-ENI.
	Copy { part_number: u32, id: FileId },
	/// Lambda-materialised bytes. On-ENI (GETs) + upload.
	Stream {
		part_number: u32,
		segments: Vec<Segment>,
	},
}

/// A chain = its own MPU. Normal chain: [Copy(big), Stream(small + next-header)].
/// First chain: all Stream parts (bin-packed; bootstraps entry-0 header).
#[derive(Debug, Clone)]
pub struct Chain {
	pub parts: Vec<PartSpec>,
	pub final_merge_part_number: u32,
}

/// The planner's complete output for the fast path.
#[derive(Debug, Clone)]
pub struct Plan {
	pub order: Vec<FileId>, // canonical ZIP order — source of truth for sequence
	pub entries: HashMap<FileId, Entry>,
	pub chains: Vec<Chain>,
	pub copyable: Vec<FileId>, // need a phase-1 CRC HEAD (chain order)
}

#[derive(Debug, Clone)]
pub enum Routing {
	CopyPart(Plan),
	Fallback,
}

/// Compute local-header offsets across `order` via a single fold (the only place offset
/// arithmetic lives).
fn compute_offsets(order: &[FileId], size_of: &HashMap<FileId, (String, u64)>) -> Vec<u64> {
	order
		.iter()
		.scan(0u64, |acc, id| {
			let here = *acc;
			let (name, size) = &size_of[id];
			*acc += zip_format::entry_total_len(name, *size);
			Some(here)
		})
		.collect()
}

/// The planner. Pure, total: `files -> Routing`.
pub fn plan(files: Vec<SourceFile>) -> Routing {
	let total: u64 = files.iter().map(|f| f.size).sum();

	// Split copyable (>= floor) vs streamable (< floor).
	let mut copyable: Vec<SourceFile> = Vec::new();
	let mut streamable: Vec<SourceFile> = Vec::new();
	for f in files {
		if f.size >= PART_FLOOR {
			copyable.push(f);
		} else {
			streamable.push(f);
		}
	}

	// Pair each copyable with one streamable -> normal chains. Leftover streamables -> first chain.
	let mut normal_pairs: Vec<(SourceFile, Option<SourceFile>)> = Vec::new();
	let mut sq = streamable.into_iter();
	let mut leftover: Vec<SourceFile> = Vec::new();
	for big in copyable {
		match sq.next() {
			Some(small) => normal_pairs.push((big, Some(small))),
			None => normal_pairs.push((big, None)),
		}
	}
	leftover.extend(sq);

	// Viability: need >=2 chains (>=1 normal + a first chain), copyable mass, total big enough.
	// If fewer than 2 normal pairs or total too small, fall back to plain streaming.
	if normal_pairs.len() < 2 || total < VIABILITY_MIN_TOTAL {
		return Routing::Fallback;
	}

	// First chain = leftover streamables (all streamed). If too small / empty, borrow the
	// smallest normal pair's files (both become streamed members of the first chain).
	let mut first_chain_files: Vec<SourceFile> = leftover;
	let first_sum: u64 = first_chain_files.iter().map(|f| f.size).sum();
	if first_sum < PART_FLOOR {
		// borrow the pair with the smallest big
		let idx = normal_pairs
			.iter()
			.enumerate()
			.min_by_key(|(_, (big, _))| big.size)
			.map(|(i, _)| i)
			.expect("normal_pairs non-empty (checked >=2)");
		let (big, small) = normal_pairs.remove(idx);
		first_chain_files.push(big);
		if let Some(s) = small {
			first_chain_files.push(s);
		}
	}

	// After a possible borrow we may have dropped below 2 normal pairs; if so, fall back.
	if normal_pairs.is_empty() {
		return Routing::Fallback;
	}

	// --- Build canonical order: first-chain files, then each normal chain's big then small ---
	let mut order: Vec<FileId> = Vec::new();
	for f in &first_chain_files {
		order.push(f.id);
	}
	for (big, small) in &normal_pairs {
		order.push(big.id);
		if let Some(s) = small {
			order.push(s.id);
		}
	}

	// size/name lookup for offsets + entries
	let mut size_of: HashMap<FileId, (String, u64)> = HashMap::new();
	for f in first_chain_files.iter() {
		size_of.insert(f.id, (f.name.clone(), f.size));
	}
	for (big, small) in normal_pairs.iter() {
		size_of.insert(big.id, (big.name.clone(), big.size));
		if let Some(s) = small {
			size_of.insert(s.id, (s.name.clone(), s.size));
		}
	}

	let offs = compute_offsets(&order, &size_of);

	// streamed-ness: first-chain files are streamed; normal big = copied, normal small = streamed.
	let mut streamed: HashMap<FileId, bool> = HashMap::new();
	for f in &first_chain_files {
		streamed.insert(f.id, true);
	}
	for (big, small) in &normal_pairs {
		streamed.insert(big.id, false);
		if let Some(s) = small {
			streamed.insert(s.id, true);
		}
	}

	let mut entries: HashMap<FileId, Entry> = HashMap::new();
	for (i, id) in order.iter().enumerate() {
		let (name, size) = size_of[id].clone();
		entries.insert(
			*id,
			Entry {
				id: *id,
				name,
				size,
				local_header_offset: offs[i],
				crc: None,
				streamed: streamed[id],
			},
		);
	}

	// copyable ids needing a CRC HEAD = the normal-chain bigs (copied), in chain order.
	let copyable_ids: Vec<FileId> = normal_pairs.iter().map(|(b, _)| b.id).collect();

	// --- Build chains with part numbers and trailing-header handoffs ---
	// First chain (final_merge_part_number = 1): bin-pack streamed files into >= floor parts.
	// Its tail carries the header for normal chain 1's big.
	let mut chains: Vec<Chain> = Vec::new();

	let first_big_id = normal_pairs[0].0.id;
	let first_chain = build_first_chain(&first_chain_files, &size_of, first_big_id);
	chains.push(first_chain);

	// Normal chains: each its own MPU. Copy(big) + Stream(small + Header(next big)).
	for (k, (big, small)) in normal_pairs.iter().enumerate() {
		let next_big = normal_pairs.get(k + 1).map(|(b, _)| b.id);
		let mut segments: Vec<Segment> = Vec::new();
		if let Some(s) = small {
			segments.push(Segment::StreamedFile { id: s.id });
		}
		if let Some(nb) = next_big {
			segments.push(Segment::CopiedFileHeader { id: nb });
		}
		let parts = vec![
			PartSpec::Copy {
				part_number: 1,
				id: big.id,
			},
			PartSpec::Stream {
				part_number: 2,
				segments,
			},
		];
		chains.push(Chain {
			parts,
			final_merge_part_number: (k as u32) + 2, // first chain is slot 1
		});
	}

	Routing::CopyPart(Plan {
		order,
		entries,
		chains,
		copyable: copyable_ids,
	})
}

/// First chain: all streamed files, bin-packed into >= PART_FLOOR parts (last part exempt),
/// with the trailing CopiedFileHeader(first_big) appended to the final part.
fn build_first_chain(
	files: &[SourceFile],
	size_of: &HashMap<FileId, (String, u64)>,
	first_big_id: FileId,
) -> Chain {
	let mut parts: Vec<PartSpec> = Vec::new();
	let mut cur: Vec<Segment> = Vec::new();
	let mut cur_bytes: u64 = 0;
	let mut part_number: u32 = 1;

	for f in files {
		cur.push(Segment::StreamedFile { id: f.id });
		let (name, size) = &size_of[&f.id];
		cur_bytes += zip_format::entry_total_len(name, *size);
		// close the part once it clears the floor, but keep at least one part open for the tail
		if cur_bytes >= PART_FLOOR {
			parts.push(PartSpec::Stream {
				part_number,
				segments: std::mem::take(&mut cur),
			});
			part_number += 1;
			cur_bytes = 0;
		}
	}
	// append the trailing handoff header to the last (open or new) part — this is the exempt last part
	cur.push(Segment::CopiedFileHeader { id: first_big_id });
	parts.push(PartSpec::Stream {
		part_number,
		segments: cur,
	});

	Chain {
		parts,
		final_merge_part_number: 1,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sf(id: u32, name: &str, size: u64) -> SourceFile {
		SourceFile {
			id: FileId(id),
			key: format!("files/{name}"),
			name: name.to_string(),
			size,
		}
	}

	#[test]
	fn tiny_input_routes_to_fallback() {
		let files = vec![sf(0, "a", 10), sf(1, "b", 20)];
		assert!(matches!(plan(files), Routing::Fallback));
	}

	#[test]
	fn no_copyable_routes_to_fallback() {
		// all below floor
		let files = (0..10).map(|i| sf(i, &format!("f{i}"), 1000)).collect();
		assert!(matches!(plan(files), Routing::Fallback));
	}

	#[test]
	fn viable_input_produces_plan_with_first_chain_slot_1() {
		let big = PART_FLOOR + 100;
		// 5 copyable => ~25 MiB total, clears viability.
		let files = vec![
			sf(0, "big0", big),
			sf(1, "small1", 1000),
			sf(2, "big2", big),
			sf(3, "small3", 1000),
			sf(4, "big4", big),
			sf(5, "small5", 2000),
			sf(6, "big6", big),
			sf(7, "small7", 3000),
			sf(8, "big8", big),
			sf(9, "small9", 4000),
		];
		match plan(files) {
			Routing::CopyPart(p) => {
				assert_eq!(
					p.chains[0].final_merge_part_number, 1,
					"first chain is slot 1"
				);
				// every entry has an offset and appears once in order
				assert_eq!(p.order.len(), p.entries.len());
				// chain final-merge numbers are 1..=chains.len(), unique & contiguous
				let mut nums: Vec<u32> =
					p.chains.iter().map(|c| c.final_merge_part_number).collect();
				nums.sort();
				assert_eq!(nums, (1..=p.chains.len() as u32).collect::<Vec<_>>());
			}
			Routing::Fallback => panic!("expected fast path"),
		}
	}

	// The real anchor: plan a set, ASSEMBLE what the plan describes in memory (resolving
	// copy bodies and streamed bodies from a synthetic store), then run the validator
	// round-trip (open with zip, extract, SHA256==name, CRC verified).
	#[cfg(feature = "zip_validate")]
	#[test]
	fn planned_archive_passes_validator() {
		use crate::engine::zip_format::EntryMeta;
		use sha2::{Digest, Sha256};
		use std::collections::HashSet;
		use std::io::{Cursor, Read};

		fn sha256_hex(b: &[u8]) -> String {
			let mut h = Sha256::new();
			h.update(b);
			let d = h.finalize();
			let mut s = String::with_capacity(64);
			for x in d {
				use core::fmt::Write;
				let _ = write!(s, "{:02x}", x);
			}
			s
		}
		fn crc32(b: &[u8]) -> u32 {
			let mut h = crc32fast::Hasher::new();
			h.update(b);
			h.finalize()
		}

		// Synthetic store: content per file; NAME = sha256(content) (like the real objects).
		// 5 copyable (~5MiB each => ~25MiB total, clears viability) + streamables.
		// With 5 pairs and zero leftover, the first chain is empty -> exercises the
		// borrow-smallest-pair branch.
		let mut contents: Vec<Vec<u8>> = Vec::new();
		let big = PART_FLOOR as usize + 123;
		contents.push(vec![1u8; big]); // copyable
		contents.push(vec![2u8; 1000]); // streamable
		contents.push(vec![3u8; big]); // copyable
		contents.push(vec![4u8; 2000]); // streamable
		contents.push(vec![5u8; big]); // copyable
		contents.push(vec![6u8; 3000]); // streamable
		contents.push(vec![7u8; big]); // copyable
		contents.push(vec![8u8; 4000]); // streamable
		contents.push(vec![9u8; big]); // copyable
		contents.push(vec![10u8; 5000]); // streamable

		let names: Vec<String> = contents.iter().map(|c| sha256_hex(c)).collect();
		let files: Vec<SourceFile> = (0..contents.len())
			.map(|i| SourceFile {
				id: FileId(i as u32),
				key: format!("files/{}", names[i]),
				name: names[i].clone(),
				size: contents[i].len() as u64,
			})
			.collect();

		let plan = match plan(files) {
			Routing::CopyPart(p) => p,
			Routing::Fallback => panic!("expected fast path for this input"),
		};

		// Fill CRCs for copyable entries (phase 1 stand-in) from the synthetic content.
		// Build the EntryMeta list in canonical order with offsets from the plan.
		let by_id = |id: FileId| -> usize { id.0 as usize };
		let metas: Vec<EntryMeta> = plan
			.order
			.iter()
			.map(|id| {
				let e = &plan.entries[id];
				EntryMeta {
					name: e.name.clone(),
					size: e.size,
					crc: crc32(&contents[by_id(*id)]),
					local_header_offset: e.local_header_offset,
				}
			})
			.collect();

		// Assemble archive bytes in canonical order: [local header][body] per entry.
		// (We assemble by ENTRY ORDER, which is what the byte layout is; the chain/part
		//  structure governs HOW bytes get there in AWS, not the final byte sequence.)
		let mut archive: Vec<u8> = Vec::new();
		for (e, id) in metas.iter().zip(plan.order.iter()) {
			archive.extend_from_slice(&zip_format::local_header(e));
			archive.extend_from_slice(&contents[by_id(*id)]);
		}
		let cd_offset = archive.len() as u64;
		let mut cd_size = 0u64;
		for e in &metas {
			let rec = zip_format::central_dir_entry(e);
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		archive.extend_from_slice(&zip_format::end_records(
			metas.len() as u64,
			cd_offset,
			cd_size,
		));

		// Validate exactly like the control Lambda.
		let mut expected: HashSet<String> = metas.iter().map(|m| m.name.clone()).collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("planned archive must parse with the standard zip reader");
		assert_eq!(za.len(), metas.len());
		let arch_names: Vec<String> = za.file_names().map(ToOwned::to_owned).collect();
		for n in &arch_names {
			assert!(!n.contains('/'), "flat layout required");
			assert!(expected.remove(n), "unknown/duplicate {n}");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
		for n in &arch_names {
			let mut entry = za.by_name(n).unwrap();
			let mut buf = Vec::new();
			entry.read_to_end(&mut buf).expect("extract (CRC verified)");
			assert_eq!(&sha256_hex(&buf), n, "content hash == name");
		}
	}
}
