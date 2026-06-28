mod part_builder;

use std::{collections::BTreeMap, sync::Arc};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{ChecksumMode, CompletedMultipartUpload, CompletedPart},
};

use tokio::{
    sync::{
        Semaphore,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinSet,
};
use tracing::{debug, info, instrument};

use crate::{
    Error, s3,
    shared_buffer::SharedBuf,
    zip_format::{
        CentralDirectoryFileHeader, EndOfCentralDirectory64, LocalFileHeader, NoCRC, ZipSerialize,
    },
    zip_layout::ZipLayout,
};

pub use part_builder::PartJobsBuilder;

// ---------- Tunables ----------

/// Total memory that all concurrently running part-job tasks may allocate for their upload buffers.
///
/// Each `UploadPart` task acquires permits equal to its buffer size before spawning, so this
/// semaphore acts as a backpressure valve. `UploadPartCopy` tasks claim a fixed 512 KiB token
/// (they hold no buffer) just to prevent all copy jobs from launching simultaneously.
/// Central Directory headers are excluded from this budget — they are negligible (~48 KiB per
/// 1 000 entries).
const MAX_PART_JOB_TASKS_MEMORY: usize = 50 * 1024 * 1024; // 50MB

// ---------- MultiPart execution ----------

/// Drives the S3 multipart upload: creates the upload, runs all part jobs, then completes it.
pub struct PartJobExecutor {
    bucket_name: Arc<str>,
    archive_key: Arc<str>,
    jobs: Vec<PartJob>,
}

impl PartJobExecutor {
    /// Converts a [`ZipLayout`] into an executor ready to run against the given bucket and key.
    pub fn new(layout: ZipLayout, bucket_name: Arc<str>, archive_key: Arc<str>) -> Self {
        Self {
            bucket_name,
            archive_key,
            jobs: layout.into_part_jobs(),
        }
    }

    /// Runs the full multipart upload: create → spawn parts (memory-gated) → complete.
    ///
    /// Parts are spawned sequentially upon acquisition of memory permits from a semaphore,
    /// preventing unbounded buffer allocation for many parts.
    #[instrument(skip_all, fields(archive_key = %self.archive_key))]
    pub async fn execute(self) -> Result<(), Error> {
        let part_count = self.jobs.len();
        info!(part_count, "Starting multipart upload");

        let response = s3()
            .create_multipart_upload()
            .bucket(&*self.bucket_name)
            .key(&*self.archive_key)
            .content_type("application/zip")
            .send()
            .await?;

        let upload_id = response
            .upload_id
            .ok_or("No upload ID in multipart upload response")?;
        info!(
            upload_id,
            part_count, "Multipart upload created, dispatching part jobs"
        );

        let memory_semaphore = Arc::new(Semaphore::new(MAX_PART_JOB_TASKS_MEMORY));

        let mut join_set = JoinSet::new();
        for job in self.jobs {
            let budget = job.memory_budget_needed();
            debug!(
                part_number = job.part_number,
                budget,
                available_memory = memory_semaphore.available_permits(),
                "Acquiring memory budget before spawning part job"
            );
            // Block here so we apply backpressure before spawning.
            // The permit is moved into the task and dropped when the task finishes,
            // freeing capacity for the next job.
            let permit = memory_semaphore
                .clone()
                .acquire_many_owned(budget)
                .await
                .map_err(|_| "Semaphore closed")?;
            let bucket_name = (*self.bucket_name).to_owned();
            let archive_key = (*self.archive_key).to_owned();
            let upload_id = upload_id.clone();
            intermittent_tracing!(job.part_number, ?job, "Spawning Part job");
            join_set.spawn(async move {
                let _permit = permit;
                job.execute(bucket_name, archive_key, upload_id).await
            });
        }

        info!(part_count, "All part jobs dispatched, awaiting completion");
        let mut completed_parts = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<CompletedPart>, _>>()?;
        info!(
            completed_parts = completed_parts.len(),
            "All parts uploaded, completing multipart upload"
        );

        // Sort parts by part number
        completed_parts.sort_by_key(|part| part.part_number());

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        s3().complete_multipart_upload()
            .bucket(&*self.bucket_name)
            .key(&*self.archive_key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        info!("Multipart upload completed");
        Ok(())
    }
}

/// One unit of work in the multipart upload: either an `UploadPart` or an `UploadPartCopy`.
#[derive(Debug)]
pub struct PartJob {
    /// Part number in the MultiPart upload, begins at 1.
    part_number: i32,
    /// Exact size of this part.
    part_size: usize,
    part_job_type: PartJobType,
}

impl PartJob {
    /// Returns the number of semaphore permits this job must hold while running.
    ///
    /// `Upload` jobs claim their full buffer size. `Copy` jobs claim a fixed 512 KiB token
    /// (they allocate no buffer) to prevent all copy jobs from running simultaneously.
    fn memory_budget_needed(&self) -> u32 {
        match self.part_job_type {
            PartJobType::Upload(_) => self.part_size.min(u32::MAX as usize) as u32,
            PartJobType::Copy { .. } => 512 * 1024, // 512KiB so we don't launch all of these jobs at once
        }
    }

    /// Executes the part job and returns the completed part (ETag + part number) for the manifest.
    async fn execute(
        self,
        bucket_name: String,
        archive_key: String,
        upload_id: String,
    ) -> Result<CompletedPart, Error> {
        let PartJob {
            part_number,
            part_size,
            part_job_type,
        } = self;

        let etag = match part_job_type {
            PartJobType::Copy { copy_source, range } => {
                let copy_part_request = s3().upload_part_copy();

                let copy_part_request = if let Some(range) = range {
                    debug!(
                        bucket_name,
                        archive_key,
                        upload_id,
                        part_number,
                        copy_source,
                        range,
                        "UploadPartCopy (ranged)"
                    );
                    copy_part_request.copy_source_range(range)
                } else {
                    debug!(
                        bucket_name,
                        archive_key, upload_id, part_number, copy_source, "UploadPartCopy"
                    );
                    copy_part_request
                };
                let result = copy_part_request
                    .bucket(bucket_name)
                    .key(archive_key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .copy_source(copy_source)
                    .send()
                    .await?;
                result
                    .copy_part_result
                    .and_then(|r| r.e_tag)
                    .ok_or("No ETag in UploadPartCopy response")?
            }
            PartJobType::Upload(upload_part_elems) => {
                // Allocate one contiguous buffer for the entire part, then carve it into
                // non-overlapping slices — one per element — so tasks can write in parallel
                // without any locking.
                let buffer = SharedBuf::with_capacity(part_size);

                let mut remain_buf = buffer.slice()?;
                let mut tasks = JoinSet::new();
                for elem in upload_part_elems {
                    let elem_size = elem.elem_size();

                    // Split off exactly `elem_size` bytes for this element; `remain` covers
                    // the rest of the buffer for subsequent elements.
                    let (mut buf, remain) = remain_buf.split(elem_size);
                    remain_buf = remain;
                    tasks.spawn(async move { elem.resolve_and_write(&mut buf).await });
                }

                // Drop the now-empty remainder so the Arc refcount can reach 1 when all
                // element slices are also dropped, allowing `into_inner` to succeed.
                drop(remain_buf);

                tasks
                    .join_all()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                let buffer = buffer.into_inner().expect("All slices have been dropped");

                debug!(
                    bucket_name,
                    archive_key,
                    upload_id,
                    part_number,
                    "buffer.len()" = buffer.len(),
                    "UploadPart"
                );
                let result = s3()
                    .upload_part()
                    .bucket(bucket_name)
                    .key(archive_key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(buffer))
                    .send()
                    .await?;

                result.e_tag.ok_or("No ETag in UploadPart response")?
            }
        };

        intermittent_tracing!(
            part_number,
            part_number,
            part_size,
            etag = %etag,
            "Part uploaded"
        );

        Ok(CompletedPart::builder()
            .part_number(part_number)
            .e_tag(etag)
            .build())
    }
}

/// Distinguishes between a locally-built upload buffer and a server-side copy.
#[derive(Debug)]
enum PartJobType {
    /// Build a buffer from [`UploadPartElement`]s and upload it via `UploadPart`.
    Upload(Vec<UploadPartElement>),
    /// Transfer bytes directly between S3 objects via `UploadPartCopy`.
    Copy {
        /// `"bucket/key"` string required by the S3 `copy_source` parameter.
        copy_source: String,
        /// Optional `"bytes=start-end"` range for partial copies.
        range: Option<String>,
    },
}

/// The S3 operation used to fetch a file's content and CRC32 for an upload-part element.
#[derive(Debug)]
enum S3ObjectOperation {
    /// Full `GetObject` — downloads the entire file body.
    Get,
    /// Ranged `GetObject` for the first `size` bytes, plus a `HeadObject` for the full CRC32.
    PartialGet { size: usize },
    /// `HeadObject` only — no body download; used when the body is server-side copied.
    Head,
}

/// One logical segment within an `UploadPart` buffer.
#[derive(Debug)]
enum UploadPartElement {
    /// A ZIP entry: LOC header + (optionally) file body fetched from S3.
    EntryFromS3 {
        loc: LocalFileHeader<NoCRC>,
        operation: S3ObjectOperation,
        bucket_name: Arc<str>,
        key: String,
        /// Channel used to deliver the completed [`CentralDirectoryFileHeader`] after the CRC32 is known.
        cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
    },
    /// The Central Directory block: drains all CDFHs from the channel, then writes EOCD.
    CentralDirectory {
        total_size: usize,
        eocd64: EndOfCentralDirectory64,
        cdfh_receiver: UnboundedReceiver<CentralDirectoryFileHeader>,
    },
}
impl UploadPartElement {
    /// Resolves the element's S3 data and writes ZIP bytes into `buf`.
    ///
    /// For `EntryFromS3`: fetches the CRC32 from S3 (S3 stores it as a base64-encoded,
    /// big-endian u32; [`CRC32::from_str`] decodes and byte-reverses it to little-endian for
    /// the ZIP field), writes the LOC header, sends the completed CDFH to the channel, then
    /// streams the file body (if any) into the remaining buffer space.
    ///
    /// For `CentralDirectory`: drains the CDFH channel until all entries are received, sorts
    /// them by archive offset via a `BTreeMap` (entries arrive in completion order, not layout
    /// order), then writes all CDFHs followed by the EOCD record(s).
    async fn resolve_and_write(self, buf: &mut [u8]) -> Result<(), Error> {
        match self {
            UploadPartElement::EntryFromS3 {
                loc,
                operation,
                bucket_name,
                key,
                cdfh_sender,
            } => {
                debug!(%bucket_name, %key, ?operation, loc_offset = loc.offset(), buf_size = buf.len(), "Resolving entry from S3");
                let (crc, byte_stream) = match operation {
                    S3ObjectOperation::Get => {
                        // S3 returns the CRC32 as a base64-encoded big-endian u32 in the
                        // response header when the object was stored with a checksum.
                        let resp = s3()
                            .get_object()
                            .bucket(&*bucket_name)
                            .key(key)
                            .send()
                            .await?;

                        (
                            resp.checksum_crc32
                                .ok_or("No CRC32 in the GetObject response")?
                                .parse()?,
                            Some(resp.body),
                        )
                    }
                    S3ObjectOperation::PartialGet { size } => {
                        // Issue the ranged GET and the HeadObject concurrently: the GET fetches
                        // the partial body while the HEAD retrieves the full-file CRC32 (S3 only
                        // returns the checksum for the full object, not for byte ranges unfortunately).
                        let (get_obj_resp, head_obj_resp) = tokio::join!(
                            s3().get_object()
                                .bucket(&*bucket_name)
                                .key(key.clone())
                                .range(format!("bytes=0-{}", size - 1))
                                .send(),
                            s3().head_object()
                                .bucket(&*bucket_name)
                                .key(key)
                                .checksum_mode(ChecksumMode::Enabled)
                                .send()
                        );
                        (
                            head_obj_resp?
                                .checksum_crc32
                                .ok_or("No CRC32 in the (partial) GetObject response")?
                                .parse()?,
                            Some(get_obj_resp?.body),
                        )
                    }
                    S3ObjectOperation::Head => (
                        // no body to download; only the CRC32 is needed for the LOC of a subsequent FullCopy.
                        s3().head_object()
                            .bucket(&*bucket_name)
                            .key(key)
                            .checksum_mode(ChecksumMode::Enabled)
                            .send()
                            .await?
                            .checksum_crc32
                            .ok_or("No CRC32 in the HeadObject response")?
                            .parse()?,
                        None,
                    ),
                };

                // Promote the LOC from NoCRC to CRC32, write it, then notify the Central
                // Directory element that this entry's CDFH is ready.
                let loc = loc.set_crc(crc);
                let mut offset = loc.dump(buf.as_mut())?;
                cdfh_sender
                    .send(CentralDirectoryFileHeader::from_loc(loc))
                    .map_err(|_| "Could not send the CentralDirectoryFileHeader: Channel closed")?;
                if let Some(mut byte_stream) = byte_stream {
                    while let Some(chunk_result) = byte_stream.next().await {
                        let chunk = chunk_result?;
                        offset += (&chunk as &[u8]).dump(&mut buf[offset..])?;
                    }
                }
                debug!(bytes_written = offset, "Entry written to buffer");
                Ok(())
            }
            UploadPartElement::CentralDirectory {
                eocd64,
                mut cdfh_receiver,
                ..
            } => {
                debug!(
                    record_count = eocd64.record_count,
                    "Assembling Central Directory (waiting for all entry CRCs)"
                );
                // Collect CDFHs from all entry tasks. Tasks run concurrently so they arrive
                // out of order; a BTreeMap keyed by archive offset restores layout order.
                let mut entries: BTreeMap<usize, CentralDirectoryFileHeader> = BTreeMap::new();

                while let Some(cdfh) = cdfh_receiver.recv().await {
                    entries.insert(cdfh.loc_offset(), cdfh);
                    // Stop as soon as all expected entries have arrived.
                    if eocd64.record_count == entries.len() {
                        break;
                    }
                }

                let mut offset = 0;
                for cdfh in entries.into_values() {
                    offset += cdfh.dump(&mut buf[offset..])?;
                }

                // Then write the End Of Central Directory records
                let eocd_written = eocd64.dump(&mut buf[offset..])?;
                debug!(
                    cd_bytes = offset,
                    eocd_bytes = eocd_written,
                    "Central Directory written"
                );

                Ok(())
            }
        }
    }
    /// Returns the number of bytes this element will write into the upload buffer.
    fn elem_size(&self) -> usize {
        match self {
            UploadPartElement::EntryFromS3 { loc, operation, .. } => match operation {
                S3ObjectOperation::Get => loc.size() + loc.file_size(),
                S3ObjectOperation::PartialGet { size } => loc.size() + *size,
                S3ObjectOperation::Head => loc.size(),
            },
            UploadPartElement::CentralDirectory { total_size, .. } => *total_size,
        }
    }
}
