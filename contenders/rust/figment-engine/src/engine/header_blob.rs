//! The headers blob `H`: a single object holding every entry's ZIP local header, laid out
//! contiguously, so the assembler can place any header into a chain via a server-side
//! byte-range UploadPartCopy instead of streaming it. NO `aws_sdk_s3` — pure + testable.
//!
//! The header BYTES are identical whether emitted inline (first chain / fallback) or copied
//! from `H` (normal chains): both come from `zip_format::local_header(&EntryMeta)`. So `H` is
//! just the concatenation of those headers, with a recorded (offset, len) range per FileId.

use std::collections::HashMap;

use crate::engine::plan::FileId;
use crate::engine::zip_format::{self, EntryMeta};

/// Layout of the headers blob: total length + each entry's byte range within it.
#[derive(Debug, Clone, Default)]
pub struct HeaderBlob {
	pub bytes_len: u64,
	pub ranges: HashMap<FileId, HeaderRange>,
}

/// A single header's location inside `H`: [offset, offset+len).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderRange {
	pub offset: u64,
	pub len: u64,
}

impl HeaderRange {
	/// Inclusive-exclusive end, i.e. the byte after the last. Useful for the copy-source
	/// range header `bytes=offset-(end-1)`.
	pub fn end(&self) -> u64 {
		self.offset + self.len
	}
}

/// Build `H` for the given entries (any iteration order; the range table makes lookup
/// order-independent). Each entry contributes exactly `local_header(meta)` bytes.
///
/// `metas` must yield one `(FileId, EntryMeta)` per entry whose header belongs in `H`. The
/// caller decides which entries need a copyable header (in practice: every entry, so the
/// directory build and the layered chains can both source headers uniformly).
pub fn build_header_blob<I>(metas: I) -> (Vec<u8>, HeaderBlob)
where
	I: IntoIterator<Item = (FileId, EntryMeta)>,
{
	let mut bytes: Vec<u8> = Vec::new();
	let mut ranges: HashMap<FileId, HeaderRange> = HashMap::new();
	for (id, meta) in metas {
		let header = zip_format::local_header(&meta);
		let offset = bytes.len() as u64;
		let len = header.len() as u64;
		bytes.extend_from_slice(&header);
		ranges.insert(id, HeaderRange { offset, len });
	}
	let bytes_len = bytes.len() as u64;
	(bytes, HeaderBlob { bytes_len, ranges })
}

#[cfg(test)]
mod tests {
	use super::*;

	fn meta(id: u32, name: &str, size: u64, crc: u32, off: u64) -> (FileId, EntryMeta) {
		(
			FileId(id),
			EntryMeta {
				name: name.to_string(),
				size,
				crc,
				local_header_offset: off,
			},
		)
	}

	#[test]
	fn ranges_cover_blob_contiguously_and_in_order() {
		let entries = vec![
			meta(0, "a.bin", 100, 0x1111_1111, 0),
			meta(1, "bb.bin", 200, 0x2222_2222, 130),
			meta(2, "ccc.bin", 4_998_471, 0xACBC_B7E5, 333),
		];
		let (bytes, blob) = build_header_blob(entries.clone());

		// Total length equals sum of individual header lengths, contiguous, no gaps/overlaps.
		let mut expected_off = 0u64;
		for (id, m) in &entries {
			let r = blob.ranges.get(id).expect("range present");
			assert_eq!(r.offset, expected_off, "range starts where previous ended");
			assert_eq!(
				r.len,
				zip_format::local_header(m).len() as u64,
				"len == header len"
			);
			expected_off = r.end();
		}
		assert_eq!(
			blob.bytes_len, expected_off,
			"blob len == sum of header lens"
		);
		assert_eq!(bytes.len() as u64, blob.bytes_len);
	}

	#[test]
	fn each_range_slices_exactly_that_entrys_header() {
		// The CORE invariant: H[range] byte-for-byte equals local_header(meta) for that entry.
		// This is what makes a server-side byte-range copy of H place the right header.
		let entries = vec![
			meta(0, "alpha", 10, 0xDEAD_BEEF, 0),
			meta(1, "beta-longer-name", 5_000_000, 0x0102_0304, 40),
			meta(2, "g", 0, 0, 5_000_080),
		];
		let (bytes, blob) = build_header_blob(entries.clone());

		for (id, m) in &entries {
			let r = blob.ranges[id];
			let slice = &bytes[r.offset as usize..r.end() as usize];
			let expected = zip_format::local_header(m);
			assert_eq!(
				slice,
				expected.as_slice(),
				"H[range] must equal local_header(meta)"
			);
		}
	}

	#[test]
	fn empty_input_yields_empty_blob() {
		let (bytes, blob) = build_header_blob(std::iter::empty());
		assert!(bytes.is_empty());
		assert_eq!(blob.bytes_len, 0);
		assert!(blob.ranges.is_empty());
	}
}
