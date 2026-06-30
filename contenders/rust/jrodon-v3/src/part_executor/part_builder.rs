use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{debug, info, instrument};

use crate::{
    part_executor::{
        EntryFromS3, PartJobType, S3ObjectOperation, SerializableEntryFromS3, UploadPartElement,
    },
    s3_ops::FileInfo,
    zip_format::{CentralDirectoryFileHeader, EndOfCentralDirectory64, LocalFileHeader},
};

use super::PartJob;

/// Incrementally builds the ordered list of [`PartJob`]s while tracking the running archive offset.
///
/// Call [`add_files`](Self::add_files) / [`copy_file`](Self::copy_file) /
/// [`partial_copy_file`](Self::partial_copy_file) in layout order, then [`finalize`](Self::finalize)
/// to append the Central Directory element and retrieve the completed job list.
#[derive(Debug)]
pub struct PartJobsBuilder {
    /// Running byte offset into the final ZIP archive; advanced as each entry is registered.
    current_archive_offset: usize,
    /// Predicted byte size of each Central Directory File Header, accumulated for the EOCD.
    cdfh_sizes: Vec<usize>,
    /// The built parts list.
    parts: Vec<PartJob>,
    /// Sender half of the channel through which entry tasks deliver their completed CDFHs.
    cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
    /// Receiver half consumed by the Central Directory element during execution.
    cdfh_receiver: UnboundedReceiver<CentralDirectoryFileHeader>,
}

impl PartJobsBuilder {
    /// Creates a new builder with a fresh CDFH channel and zero archive offset.
    pub fn new() -> Self {
        let (cdfh_sender, cdfh_receiver) = unbounded_channel();
        Self {
            current_archive_offset: 0,
            cdfh_sizes: vec![],
            parts: vec![],
            cdfh_sender,
            cdfh_receiver,
        }
    }

    /// Appends a sequence of regular files (full `GetObject` download) to the current upload part.
    ///
    /// For each file, a LOC header is created at the current archive offset, the offset is
    /// advanced by `loc_size + file_size`, and a CDFH size prediction is recorded. If the last
    /// existing part is already an `Upload` part, the new elements are appended to it; otherwise
    /// a new `Upload` part is created.
    #[instrument(skip_all, fields(current_archive_offset=%self.current_archive_offset, part_count=%self.parts.len(), entry_count=%self.cdfh_sizes.len()))]
    pub fn add_files(&mut self, file_infos: impl Iterator<Item = FileInfo>) {
        let elem_iterator = file_infos.map(|file_info| {
            // Build the LOC at the current offset, then advance the offset past LOC + payload.
            let name_len = file_info.name.len();
            let loc =
                LocalFileHeader::new(self.current_archive_offset, file_info.name, file_info.size);
            self.cdfh_sizes
                .push(CentralDirectoryFileHeader::predict_size(
                    file_info.size,
                    name_len,
                    loc.offset(),
                ));
            self.current_archive_offset += loc.size() + file_info.size;

            UploadPartElement::EntryFromS3(EntryFromS3 {
                entry: SerializableEntryFromS3 {
                    loc,
                    operation: S3ObjectOperation::Get,
                    bucket_name: file_info.bucket_name,
                    key: file_info.key,
                },
                cdfh_sender: self.cdfh_sender.clone(),
            })
        });

        // Reuse the last Upload part if possible to avoid creating unnecessary part boundaries.
        match self.parts.last_mut() {
            Some(PartJob {
                part_size,
                part_job_type: PartJobType::Upload(part_elements),
                ..
            }) => {
                part_elements.extend(elem_iterator);
                *part_size = part_elements.iter().map(|upe| upe.elem_size()).sum();
            }
            _ => {
                let part_number = self.parts.len() as i32 + 1;
                let upload_part_elems = elem_iterator.collect::<Vec<_>>();
                let part_size = upload_part_elems.iter().map(|upe| upe.elem_size()).sum();
                debug!(part_number, part_size, "Creating a new Upload PartJob");
                self.parts.push(PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(upload_part_elems),
                });
            }
        };
    }

    /// Appends a full server-side copy entry (LOC via `HeadObject`, body via `UploadPartCopy`).
    pub fn copy_file(&mut self, file_info: FileInfo) {
        self.internal_copy_file(file_info, None);
    }

    /// Appends a partial server-side copy entry: first `start_byte` bytes are downloaded into
    /// the current upload part; the remainder is transferred via `UploadPartCopy`.
    pub fn partial_copy_file(&mut self, file_info: FileInfo, start_byte: usize) {
        self.internal_copy_file(file_info, Some(start_byte));
    }

    /// Shared implementation for [`copy_file`](Self::copy_file) and
    /// [`partial_copy_file`](Self::partial_copy_file).
    ///
    /// Appends the LOC element (and optionally a ranged GET element) to the current upload part,
    /// then creates a new `Copy` part for the server-side-copied body.
    #[instrument(skip_all, fields(file_info, start_byte))]
    fn internal_copy_file(&mut self, file_info: FileInfo, start_byte: Option<usize>) {
        // A Copy part must always follow an Upload part (the LOC lives in the Upload part).
        let last_part = self.parts.last_mut().expect("CopyPart cannot be first");
        let PartJobType::Upload(part_elements) = &mut last_part.part_job_type else {
            panic!("CopyPart cannot follow another CopyPart");
        };

        // Register the LOC at the current offset and advance past it.
        let name_len = file_info.name.len();
        let loc = LocalFileHeader::new(self.current_archive_offset, file_info.name, file_info.size);
        self.cdfh_sizes
            .push(CentralDirectoryFileHeader::predict_size(
                file_info.size,
                name_len,
                loc.offset(),
            ));
        self.current_archive_offset += loc.size();

        // For a FullCopy, only the LOC is in the upload part (HeadObject fetches the CRC32).
        // For a PartialCopy, the first `start_byte` bytes are also downloaded (ranged GetObject),
        // so the archive offset must advance past those bytes too before the Copy part begins.
        let operation = match start_byte {
            Some(start_byte) => {
                self.current_archive_offset += start_byte;
                S3ObjectOperation::PartialGet { size: start_byte }
            }
            None => S3ObjectOperation::Head,
        };

        let copy_source = format!("{}/{}", file_info.bucket_name, file_info.key);

        let last_upload_part_element = UploadPartElement::EntryFromS3(EntryFromS3 {
            entry: SerializableEntryFromS3 {
                loc,
                operation,
                bucket_name: file_info.bucket_name,
                key: file_info.key,
            },
            cdfh_sender: self.cdfh_sender.clone(),
        });

        last_part.part_size += last_upload_part_element.elem_size();
        debug!(
            last_part.part_number,
            last_part.part_size, "Updated Upload part size"
        );
        part_elements.push(last_upload_part_element);
        debug!(
            last_part.part_number,
            part_element_count = part_elements.len(),
            "Upload Part finalized"
        );

        // Create the Copy part that will carry the server-side-copied body bytes.
        let part_number = self.parts.len() as i32 + 1;
        let part_offset = self.current_archive_offset;
        let part_size = match start_byte {
            Some(start_byte) => file_info.size - start_byte,
            None => file_info.size,
        };
        debug!(
            part_number,
            part_offset, part_size, "Creating a new Copy PartJob"
        );
        self.parts.push(PartJob {
            part_number,
            part_size,
            part_job_type: PartJobType::Copy {
                copy_source,
                range: start_byte
                    .map(|start_byte| format!("bytes={start_byte}-{}", file_info.size - 1)),
            },
        });
        self.current_archive_offset += part_size;
    }

    /// Appends the Central Directory element to the last upload part and returns the job list.
    ///
    /// The EOCD64 is computed from the accumulated CDFH sizes and the current archive offset.
    pub fn finalize(mut self) -> Vec<PartJob> {
        // Compute the Central Directory layout from the sizes accumulated during add_files/_copy_file.
        let record_count = self.cdfh_sizes.len();
        let central_directory_size = self.cdfh_sizes.into_iter().sum();
        let eocd64 = EndOfCentralDirectory64 {
            record_count,
            central_directory_size,
            central_directory_offset: self.current_archive_offset,
            // The EOCD/EOCD64 will directly follow the last Central Directory File Header
            eocd64_offset: self.current_archive_offset + central_directory_size,
        };
        let total_size = central_directory_size + eocd64.size();

        let cd = UploadPartElement::CentralDirectory {
            total_size,
            eocd64,
            cdfh_receiver: self.cdfh_receiver,
        };
        debug!(total_size, ?eocd64, "CentralDirectory element created");

        // If the last part is an UploadPart, extend it, else create one
        match self.parts.last_mut() {
            Some(PartJob {
                part_number,
                part_size,
                part_job_type: PartJobType::Upload(part_elements),
                ..
            }) => {
                *part_size += cd.elem_size();
                part_elements.push(cd);
                debug!(
                    part_number,
                    part_size,
                    part_element_count = part_elements.len(),
                    "Last Upload Part finalized"
                );
            }
            _ => {
                let part_number = self.parts.len() as i32 + 1;
                let part_offset = self.current_archive_offset;
                let part_size = cd.elem_size();
                debug!(
                    part_number,
                    part_offset, part_size, "Pushing Last Upload Part with only CentralDirectory"
                );
                self.parts.push(PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(vec![cd]),
                });
            }
        };

        // Log info about the jobs to be done
        let (upload_part_count, copy_part_count, download_size, upload_size, copy_size) = self
            .parts
            .iter()
            .fold((0usize, 0usize, 0, 0, 0), |mut counters, part| {
                match &part.part_job_type {
                    PartJobType::Upload(upload_part_elements) => {
                        // upload_part_count
                        counters.0 += 1;
                        // download_size
                        counters.2 += upload_part_elements
                            .iter()
                            .map(|e| match e {
                                UploadPartElement::EntryFromS3(EntryFromS3 {
                                    entry: SerializableEntryFromS3 { loc, operation, .. },
                                    ..
                                }) => match operation {
                                    S3ObjectOperation::Get => loc.file_size(),
                                    S3ObjectOperation::PartialGet { size } => *size,
                                    S3ObjectOperation::Head => 0,
                                },
                                UploadPartElement::CentralDirectory { .. } => 0,
                            })
                            .sum::<usize>();
                        // upload_size
                        counters.3 += part.part_size;
                    }
                    PartJobType::Copy { .. } => {
                        // copy_part_count
                        counters.1 += 1;
                        // copy_size
                        counters.4 += part.part_size;
                    }
                }
                counters
            });

        info!(
            part_count = self.parts.len(),
            upload_part_count,
            copy_part_count,
            download_size,
            upload_size,
            copy_size,
            "Part Jobs done"
        );

        self.parts
    }
}
