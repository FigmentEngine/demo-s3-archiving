use std::{io::Write, str::FromStr};

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::Error;

/// Serializes a value as raw ZIP bytes into a `Write` target, returning the number of bytes written.
///
/// All integer implementations write little-endian, matching the ZIP specification.
pub trait ZipSerialize {
    fn dump(&self, buf: impl Write) -> Result<usize, Error>;
}

/// Generates little-endian [`ZipSerialize`] impls for primitive integer types.
macro_rules! impl_int_zip_ser {
    ($($int_type:ty),+) => {
       $(
           impl ZipSerialize for $int_type {
               fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
                   Ok(buf.write(&self.to_le_bytes())?)
               }
           }
       )+
    };
}
impl_int_zip_ser!(u8, u16, u32, u64);

impl ZipSerialize for &[u8] {
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        Ok(buf.write(self)?)
    }
}

/// A ZIP magic number (signature), written big-endian so the byte sequence matches the spec
/// (e.g. `PK\x03\x04` for a Local File Header).
#[derive(Debug, Clone, Copy, Default)]
struct Magic(u32);
impl ZipSerialize for Magic {
    /// Writes the magic number in big-endian byte order (intentional — ZIP signatures are BE).
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        Ok(buf.write(&self.0.to_be_bytes())?)
    }
}

/// A CRC-32 checksum in the 4-byte little-endian form required by the ZIP specification.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CRC32([u8; 4]);
impl FromStr for CRC32 {
    type Err = Error;

    /// Parses the base64-encoded CRC32 returned by S3 into ZIP-ready little-endian bytes.
    ///
    /// S3 stores the checksum as a base64-encoded big-endian u32. The ZIP spec requires
    /// little-endian, so the decoded bytes are reversed before storing.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use base64::prelude::BASE64_STANDARD;

        let mut bytes = [0u8; 4];
        BASE64_STANDARD.decode_slice(s, &mut bytes)?;
        // S3 encodes the CRC32 big-endian; ZIP wants little-endian — reverse in place.
        bytes.reverse();
        Ok(Self(bytes))
    }
}
impl ZipSerialize for CRC32 {
    fn dump(&self, buf: impl Write) -> Result<usize, Error> {
        self.0.as_slice().dump(buf)
    }
}

/// Typestate marker for whether a [`LocalFileHeader`] has its CRC32 filled in.
pub trait CRCStatus {}

/// Typestate indicating the CRC32 is not yet known (before the S3 response is received).
#[derive(Debug, Serialize, Deserialize)]
pub struct NoCRC;
impl CRCStatus for NoCRC {}
impl CRCStatus for CRC32 {}

/// ZIP Local File Header (LOC), parameterized by whether the CRC32 is known yet.
///
/// Created as `LocalFileHeader<NoCRC>` during layout planning (CRC unknown), then promoted to
/// `LocalFileHeader<CRC32>` via [`set_crc`](LocalFileHeader::set_crc) once the S3 response
/// provides the checksum. Only the `CRC32` variant implements [`ZipSerialize`].
#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFileHeader<C: CRCStatus> {
    /// Byte offset of this LOC within the final ZIP archive (used by the Central Directory).
    offset: usize,
    name: String,
    crc32: C,
    file_size: usize,
}
impl<C: CRCStatus> LocalFileHeader<C> {
    /// ZIP Local File Header signature (`PK\x03\x04`).
    const MAGIC: Magic = Magic(0x50_4B_03_04);
    /// Minimum version needed to extract (1.0).
    const MIN_VER: u16 = 10;
    const GENERAL_PURPOSE_BIG_FLAG: u16 = 0;
    /// Stored (uncompressed).
    const COMPRESSION_METHOD: u16 = 0;

    const FILE_LAST_MOD_TIME: u16 = 0;
    const FILE_LAST_MOD_DATE: u16 = 0;

    /// Fixed LOC field bytes, excluding the variable-length name and extra field.
    const BASE_LOC_LENGTH: usize = 30;
    /// Size of the ZIP64 extra field block appended when `file_size > u32::MAX`.
    const LOC_ZIP64EXT_LENGTH: usize = 20;

    /// Returns the serialized byte size of this LOC header.
    pub fn size(&self) -> usize {
        Self::predict_size(self.file_size, self.name.len())
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    /// Computes the LOC byte size without constructing a header — used during layout planning.
    ///
    /// Includes the ZIP64 extra field when `file_size > u32::MAX`.
    pub fn predict_size(file_size: usize, name_len: usize) -> usize {
        if file_size > u32::MAX as usize {
            Self::BASE_LOC_LENGTH + Self::LOC_ZIP64EXT_LENGTH + name_len
        } else {
            Self::BASE_LOC_LENGTH + name_len
        }
    }
}
impl LocalFileHeader<NoCRC> {
    /// Creates a new LOC header at the given archive offset, with the CRC32 not yet known.
    pub fn new(offset: usize, name: String, size: usize) -> Self {
        Self {
            offset,
            name,
            crc32: NoCRC,
            file_size: size,
        }
    }
    /// Promotes this header to the serializable form once the CRC32 is available.
    pub fn set_crc(self, crc32: CRC32) -> LocalFileHeader<CRC32> {
        let Self {
            offset,
            name,
            file_size,
            ..
        } = self;
        LocalFileHeader {
            offset,
            name,
            crc32,
            file_size,
        }
    }
}
impl ZipSerialize for LocalFileHeader<CRC32> {
    /// Writes the LOC record, appending a ZIP64 extra field when `file_size > u32::MAX`.
    ///
    /// When ZIP64 is needed, the compressed/uncompressed size fields in the fixed header are
    /// set to `0xFFFFFFFF` (the ZIP64 sentinel) and the real sizes go in the extra field.
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        // Determine once whether we need the ZIP64 extension for this entry.
        let need_zip64_extension = self.file_size > u32::MAX as usize;

        written += Self::MAGIC.dump(&mut buf)?;
        written += Self::MIN_VER.dump(&mut buf)?;
        written += Self::GENERAL_PURPOSE_BIG_FLAG.dump(&mut buf)?;
        written += Self::COMPRESSION_METHOD.dump(&mut buf)?;
        written += Self::FILE_LAST_MOD_TIME.dump(&mut buf)?;
        written += Self::FILE_LAST_MOD_DATE.dump(&mut buf)?;
        // CRC
        written += self.crc32.dump(&mut buf)?;
        // COMPRESSED SIZE / UNCOMPRESSED SIZE
        // Use 0xFFFFFFFF sentinel when ZIP64 extension carries the real value.
        for _ in 0..2 {
            written += if need_zip64_extension {
                u32::MAX.dump(&mut buf)?
            } else {
                (self.file_size as u32).dump(&mut buf)?
            };
        }
        // NAME LEN
        written += (self.name.len() as u16).dump(&mut buf)?;
        // EXTRA_FIELD LEN — non-zero only when ZIP64 extension is present.
        written += if need_zip64_extension {
            (Self::LOC_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else {
            0u16.dump(&mut buf)?
        };
        // NAME
        written += self.name.as_bytes().dump(&mut buf)?;

        // ZIP64 extra field: header ID 0x0001, 16 bytes of uncompressed + compressed size.
        if need_zip64_extension {
            // HEADER ID (0x0001 = ZIP64 extended information)
            written += 1u16.dump(&mut buf)?;
            // Size of the extra field chunk (two u64s = 16 bytes)
            written += 16u16.dump(&mut buf)?;
            let size = self.file_size as u64;
            // UNCOMPRESSED SIZE / COMPRESSED SIZE (identical for STORED)
            for _ in 0..2 {
                written += size.dump(&mut buf)?;
            }
        };

        Ok(written)
    }
}

/// ZIP Central Directory File Header (CDFH), wrapping the completed [`LocalFileHeader<CRC32>`].
#[derive(Debug, Serialize, Deserialize)]
pub struct CentralDirectoryFileHeader(LocalFileHeader<CRC32>);
impl From<LocalFileHeader<CRC32>> for CentralDirectoryFileHeader {
    fn from(value: LocalFileHeader<CRC32>) -> Self {
        Self(value)
    }
}
impl CentralDirectoryFileHeader {
    /// ZIP Central Directory File Header signature (`PK\x01\x02`).
    const MAGIC: Magic = Magic(0x50_4B_01_02);
    const VER_MADE_BY: u16 = 10;

    /// Fixed CDFH field bytes, excluding the variable-length name and extra field.
    const BASE_CDFH_LENGTH: usize = 46; // Without extra-field and name
    /// Extra field size when only the file size overflows u32 (20 bytes: sizes only).
    const CDFH_SIZE_ZIP64EXT_LENGTH: usize = LocalFileHeader::<CRC32>::LOC_ZIP64EXT_LENGTH;
    /// Extra field size when the LOC offset also overflows u32 (adds 8 bytes for the u64 offset).
    const CDFH_OFFSET_ZIP64EXT_LENGTH: usize = Self::CDFH_SIZE_ZIP64EXT_LENGTH + 8;

    /// Constructs a CDFH from a completed LOC header (reuses all fields).
    pub fn from_loc(loc: LocalFileHeader<CRC32>) -> Self {
        Self(loc)
    }
    pub fn loc_offset(&self) -> usize {
        self.0.offset()
    }

    /// Computes the CDFH byte size without constructing a header — used during layout planning.
    ///
    /// The ZIP64 extra field grows in two steps: first when `file_size > u32::MAX` (adds size
    /// fields), then again when `loc_offset > u32::MAX` (also adds the offset field).
    pub fn predict_size(file_size: usize, name_len: usize, loc_offset: usize) -> usize {
        if loc_offset > u32::MAX as usize {
            Self::BASE_CDFH_LENGTH + Self::CDFH_OFFSET_ZIP64EXT_LENGTH + name_len
        } else if file_size > u32::MAX as usize {
            Self::BASE_CDFH_LENGTH + Self::CDFH_SIZE_ZIP64EXT_LENGTH + name_len
        } else {
            Self::BASE_CDFH_LENGTH + name_len
        }
    }
}
impl ZipSerialize for CentralDirectoryFileHeader {
    /// Writes the CDFH record with ZIP64 extra fields as needed.
    ///
    /// Two independent overflow conditions are checked:
    /// - `need_size_zip64_extension`: file size or offset exceeds u32 → include size fields in extra.
    /// - `need_offset_zip64_extension`: LOC offset exceeds u32 → also include offset field in extra.
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        // Offset overflow implies size overflow: if the archive is > 4 GiB, both fields need ZIP64.
        let need_offset_zip64_extension = self.0.offset > u32::MAX as usize;
        let need_size_zip64_extension =
            need_offset_zip64_extension || self.0.file_size > u32::MAX as usize;

        written += Self::MAGIC.dump(&mut buf)?;
        written += Self::VER_MADE_BY.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::MIN_VER.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::GENERAL_PURPOSE_BIG_FLAG.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::COMPRESSION_METHOD.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::FILE_LAST_MOD_TIME.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::FILE_LAST_MOD_DATE.dump(&mut buf)?;
        // CRC
        written += self.0.crc32.dump(&mut buf)?;
        // COMPRESSED SIZE / UNCOMPRESSED SIZE — sentinel when ZIP64 extension carries real values.
        for _ in 0..2 {
            written += if need_size_zip64_extension {
                u32::MAX.dump(&mut buf)?
            } else {
                (self.0.file_size as u32).dump(&mut buf)?
            };
        }
        // NAME LEN
        written += (self.0.name.len() as u16).dump(&mut buf)?;
        // EXTRA_FIELD LEN — size depends on which ZIP64 fields are needed.
        written += if need_offset_zip64_extension {
            (Self::CDFH_OFFSET_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else if need_size_zip64_extension {
            (Self::CDFH_SIZE_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else {
            0u16.dump(&mut buf)?
        };
        // FILE COMMENT LEN
        written += 0u16.dump(&mut buf)?;
        // DISK NUMBER
        written += 0u16.dump(&mut buf)?;
        // INTERNAL FILE ATTR
        written += 0u16.dump(&mut buf)?;
        // EXTERNAL FILE ATTR
        written += 0u32.dump(&mut buf)?;

        // RELATIVE OFFSET — sentinel when ZIP64 extension carries the real value.
        written += if need_offset_zip64_extension {
            u32::MAX.dump(&mut buf)?
        } else {
            (self.0.offset as u32).dump(&mut buf)?
        };

        // NAME
        written += self.0.name.as_bytes().dump(&mut buf)?;

        // ZIP64 extra field: always includes sizes; includes offset only when needed.
        if need_size_zip64_extension {
            // HEADER ID (0x0001 = ZIP64 extended information)
            written += 1u16.dump(&mut buf)?;
            // Extra field chunk size: 16 bytes (sizes only) or 24 bytes (sizes + offset).
            written += if need_offset_zip64_extension {
                24u16.dump(&mut buf)?
            } else {
                16u16.dump(&mut buf)?
            };
            let size = self.0.file_size as u64;
            // UNCOMPRESSED SIZE / COMPRESSED SIZE
            for _ in 0..2 {
                written += size.dump(&mut buf)?;
            }
            // RELATIVE OFFSET (only when the LOC offset itself overflows u32)
            if need_offset_zip64_extension {
                written += (self.0.offset as u64).dump(&mut buf)?;
            }
        };

        Ok(written)
    }
}

/// Writes the End of Central Directory records (EOCD, and optionally EOCD64 + locator).
#[derive(Debug, Clone, Copy)]
pub struct EndOfCentralDirectory64 {
    pub record_count: usize,
    pub central_directory_size: usize,
    /// Byte offset of the first CDFH within the archive.
    pub central_directory_offset: usize,
    /// Byte offset of the EOCD64 record itself (needed by the EOCD64 locator).
    pub eocd64_offset: usize,
}
impl EndOfCentralDirectory64 {
    /// ZIP End of Central Directory signature (`PK\x05\x06`).
    const EOCD_MAGIC: Magic = Magic(0x50_4B_05_06);
    /// ZIP64 End of Central Directory record signature (`PK\x06\x06`).
    const EOCD64_MAGIC: Magic = Magic(0x50_4B_06_06);
    /// ZIP64 End of Central Directory locator signature (`PK\x06\x07`).
    const EOCD64_LOCATOR_MAGIC: Magic = Magic(0x50_4B_06_07);

    /// Fixed byte size of the classic EOCD record (no comment).
    const EOCD_LENGTH: usize = 22;
    /// Fixed byte size of the EOCD64 record.
    const EOCD64_LENGTH: usize = 56;
    /// Fixed byte size of the EOCD64 locator record.
    const EOCD64_LOCATOR_LENGTH: usize = 20;

    /// Returns true if any field would overflow its classic EOCD field width.
    fn need_eocd64(
        central_directory_size: usize,
        central_directory_offset: usize,
        record_count: usize,
    ) -> bool {
        central_directory_size > u32::MAX as usize
            || central_directory_offset > u32::MAX as usize
            || record_count > u16::MAX as usize
    }

    /// Total byte size of the end-of-archive block (EOCD only, or EOCD64 + locator + EOCD).
    pub fn size(&self) -> usize {
        if Self::need_eocd64(
            self.central_directory_size,
            self.central_directory_offset,
            self.record_count,
        ) {
            Self::EOCD_LENGTH + Self::EOCD64_LENGTH + Self::EOCD64_LOCATOR_LENGTH
        } else {
            Self::EOCD_LENGTH
        }
    }

    /// Writes the EOCD64 record.
    fn dump_eocd64(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;
        written += Self::EOCD64_MAGIC.dump(&mut buf)?;

        // Per the ZIP spec (APPNOTE 4.3.14), the "size of zip64 end of central directory record"
        // field is the number of bytes remaining in the record *after* this field itself.
        // The record is 56 bytes total; the magic (4) + this size field (8) = 12 bytes precede it,
        // so the value is 56 - 12 = 44.
        written += 44u64.dump(&mut buf)?;

        written += CentralDirectoryFileHeader::VER_MADE_BY.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::MIN_VER.dump(&mut buf)?;
        // Number of this disk.
        // Disk where central directory starts.
        for _ in 0..2 {
            written += 0u32.dump(&mut buf)?;
        }
        // Number of central directory records on this disk.
        // Total number of central directory records.
        for _ in 0..2 {
            written += (self.record_count as u64).dump(&mut buf)?;
        }
        // Size of central directory in bytes.
        written += (self.central_directory_size as u64).dump(&mut buf)?;
        // Offset of start of central directory, relative to start of archive.
        written += (self.central_directory_offset as u64).dump(&mut buf)?;

        Ok(written)
    }
    /// Writes the EOCD64 locator record that points to the EOCD64 record.
    fn dump_eocd64_locator(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;
        written += Self::EOCD64_LOCATOR_MAGIC.dump(&mut buf)?;

        // Disk where EOCD64 starts.
        written += 0u32.dump(&mut buf)?;
        // Offset to start of EOCD64, relative to start of archive.
        written += (self.eocd64_offset as u64).dump(&mut buf)?;
        // Total number of disks.
        written += 0u32.dump(&mut buf)?;

        Ok(written)
    }
    /// Writes the classic EOCD record, using sentinel values (`0xFFFF`/`0xFFFFFFFF`) when ZIP64
    /// records are present so readers know to consult the EOCD64 for the real values.
    fn dump_eocd(&self, mut buf: impl Write, need_eocd64: bool) -> Result<usize, Error> {
        let mut written = 0;

        // Magic number. Must be 50 4B 05 06.
        written += Self::EOCD_MAGIC.dump(&mut buf)?;

        if need_eocd64 {
            // Number of this disk (or FF FF for ZIP64).
            // Disk where central directory starts (or FF FF for ZIP64).
            // Number of central directory records on this disk (or FF FF for ZIP64).
            // Total number of central directory records (or FF FF for ZIP64).
            for _ in 0..4 {
                written += u16::MAX.dump(&mut buf)?;
            }
            // Size of central directory in bytes (or FF FF FF FF for ZIP64).
            // Offset of start of central directory, relative to start of archive (or FF FF FF FF for ZIP64).
            for _ in 0..2 {
                written += u32::MAX.dump(&mut buf)?;
            }
        } else {
            // Number of this disk (or FF FF for ZIP64).
            // Disk where central directory starts (or FF FF for ZIP64).
            for _ in 0..2 {
                written += 0u16.dump(&mut buf)?;
            }
            // Number of central directory records on this disk (or FF FF for ZIP64).
            // Total number of central directory records (or FF FF for ZIP64).
            for _ in 0..2 {
                written += (self.record_count as u16).dump(&mut buf)?;
            }
            // Size of central directory in bytes (or FF FF FF FF for ZIP64).
            written += (self.central_directory_size as u32).dump(&mut buf)?;
            // Offset of start of central directory, relative to start of archive (or FF FF FF FF for ZIP64).
            written += (self.central_directory_offset as u32).dump(&mut buf)?;
        }
        // Comment length (n).
        written += 0u16.dump(&mut buf)?;

        Ok(written)
    }
}
impl ZipSerialize for EndOfCentralDirectory64 {
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        let need_eocd64 = Self::need_eocd64(
            self.central_directory_size,
            self.central_directory_offset,
            self.record_count,
        );

        if need_eocd64 {
            written += self.dump_eocd64(&mut buf)?;
            written += self.dump_eocd64_locator(&mut buf)?;
        }
        written += self.dump_eocd(&mut buf, need_eocd64)?;

        Ok(written)
    }
}
