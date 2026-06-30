mod part_builder;

use std::{collections::BTreeMap, sync::Arc};

use aws_sdk_s3::{primitives::ByteStream, types::ChecksumMode};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{
        Semaphore,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinSet,
};
use tracing::{debug, info, instrument};

use crate::{
    MAX_PART_JOB_TASKS_MEMORY,
    error::Error,
    lambda_executor::LambdaExecutor,
    s3,
    s3_ops::{CompletedPartInfo, MultipartUpload},
    shared_buffer::SharedBuf,
    zip_format::{
        CentralDirectoryFileHeader, EndOfCentralDirectory64, LocalFileHeader, NoCRC, ZipSerialize,
    },
    zip_layout::ZipLayout,
};

pub use part_builder::PartJobsBuilder;

// ---------- MultiPart execution ----------

/// Drives the S3 multipart upload: creates the upload, runs all part jobs, then completes it.
pub struct PartJobExecutor {
    multipart_upload: MultipartUpload,
    jobs: Vec<PartJob>,
    lambda_executor: Option<LambdaExecutor>,
}

impl PartJobExecutor {
    /// Converts a [`ZipLayout`] into an executor ready to run against the given bucket and key.
    pub fn new(layout: ZipLayout, multipart_upload: MultipartUpload) -> Self {
        Self::new_from_jobs(multipart_upload, layout.into_part_jobs())
    }
    /// Creates an executor from a pre-built list of [`PartJob`]s (used by sub-workers).
    pub fn new_from_jobs(multipart_upload: MultipartUpload, jobs: Vec<PartJob>) -> Self {
        Self {
            multipart_upload,
            jobs,
            lambda_executor: None,
        }
    }

    /// Attaches a [`LambdaExecutor`] so that eligible jobs are fanned out to sub-workers.
    pub fn with_lambda_executor(self, lambda_executor: LambdaExecutor) -> Self {
        Self {
            lambda_executor: Some(lambda_executor),
            ..self
        }
    }

    /// Runs all part jobs, returning the completed part descriptors for the multipart manifest.
    ///
    /// Delegates to [`execute_with_lambda_subworkers`](Self::execute_with_lambda_subworkers) when
    /// a [`LambdaExecutor`] is present, otherwise runs locally via [`execute_local`](Self::execute_local).
    #[instrument(skip_all, fields(multipart_upload = ?self.multipart_upload))]
    pub async fn execute(self) -> Result<Vec<CompletedPartInfo>, Error> {
        let Self {
            multipart_upload,

            jobs,
            lambda_executor,
        } = self;
        info!(part_count = jobs.len(), "Executing multipart upload");

        match lambda_executor {
            Some(lambda_executor) => {
                Self::execute_with_lambda_subworkers(lambda_executor, multipart_upload, jobs).await
            }
            None => Self::execute_local(multipart_upload, jobs).await,
        }
    }

    /// Executes all part jobs locally within this Lambda invocation.
    ///
    /// Parts are spawned sequentially upon acquisition of memory permits from a semaphore,
    /// preventing unbounded buffer allocation for many concurrent parts.
    async fn execute_local(
        MultipartUpload {
            bucket_name,
            archive_key,
            upload_id,
        }: MultipartUpload,
        jobs: Vec<PartJob>,
    ) -> Result<Vec<CompletedPartInfo>, Error> {
        info!("job.len()" = jobs.len(), "Local job execution");
        let memory_semaphore = Arc::new(Semaphore::new(MAX_PART_JOB_TASKS_MEMORY));
        let mut join_set = JoinSet::new();
        let part_count = jobs.len();
        for job in jobs {
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
            let bucket_name = (*bucket_name).to_owned();
            let archive_key = (*archive_key).to_owned();
            let upload_id = (*upload_id).to_owned();
            intermittent_tracing!(job.part_number, ?job, "Spawning Part job");
            join_set.spawn(async move {
                let _permit = permit;
                job.execute(bucket_name, archive_key, upload_id).await
            });
        }
        info!(part_count, "All part jobs dispatched, awaiting completion");
        join_set.join_all().await.into_iter().collect()
    }
    /// Fans out eligible part jobs to sub-worker Lambda invocations via `lambda_executor`.
    ///
    /// `Copy` jobs and jobs containing a `CentralDirectory` element are not delegatable and
    /// run locally. All `Upload`-only jobs are serialized as [`DelegatedPartJob`]s and sent
    /// to the [`LambdaExecutor`]. Both local and delegated execution run concurrently.
    async fn execute_with_lambda_subworkers(
        lambda_executor: LambdaExecutor,
        multipart_upload: MultipartUpload,
        jobs: Vec<PartJob>,
    ) -> Result<Vec<CompletedPartInfo>, Error> {
        let (mut to_delegate, local) =
            jobs.into_iter()
                .partition::<Vec<_>, _>(|job| match &job.part_job_type {
                    PartJobType::Upload(upload_part_elements) => upload_part_elements
                        .iter()
                        .all(|elem| matches!(elem, UploadPartElement::EntryFromS3(_))),
                    PartJobType::Copy { .. } => false,
                });

        let (mut local_exec, delegated_exec) = tokio::try_join!(
            Self::execute_local(multipart_upload.clone(), local),
            async move {
                info!(
                    "to_delegate.len()" = to_delegate.len(),
                    "Delegate job execution"
                );
                let (last_delegated_job, cdfh_sender) = if let Some(part_job) = to_delegate.pop() {
                    DelegatedPartJob::new_from_part_job(part_job)?
                } else {
                    return Ok(vec![]);
                };

                info!("Set Multipart Upload");
                lambda_executor
                    .set_multipart_upload(multipart_upload)
                    .await?;

                info!("Submit jobs");
                lambda_executor
                    .submit_jobs(
                        to_delegate
                            .into_iter()
                            .map(|job| Ok(DelegatedPartJob::new_from_part_job(job)?.0))
                            .chain(Some(Ok(last_delegated_job)))
                            .collect::<Result<Vec<_>, Error>>()?,
                    )
                    .await?;

                info!("Wait jobs");
                let (part_infos, cdfhs) = lambda_executor.wait_all().await?;

                for cdfh in cdfhs {
                    cdfh_sender.send(cdfh).map_err(
                        |_| "Could not send the CentralDirectoryFileHeader: Channel closed",
                    )?;
                }

                Ok(part_infos)
            }
        )?;
        local_exec.extend(delegated_exec);
        Ok(local_exec)
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
    /// Reconstructs a `PartJob` from a [`DelegatedPartJob`] received by a sub-worker.
    ///
    /// Wraps each [`SerializableEntryFromS3`] in an [`EntryFromS3`] with the provided
    /// `cdfh_sender` so completed Central Directory headers can be forwarded after execution.
    pub fn new(
        delegated_part_job: DelegatedPartJob,
        cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
    ) -> Self {
        let DelegatedPartJob {
            part_number,
            part_size,
            entries_from_s3,
        } = delegated_part_job;
        Self {
            part_number,
            part_size,
            part_job_type: PartJobType::Upload(
                entries_from_s3
                    .into_iter()
                    .map(|e| {
                        UploadPartElement::EntryFromS3(EntryFromS3 {
                            entry: e,
                            cdfh_sender: cdfh_sender.clone(),
                        })
                    })
                    .collect(),
            ),
        }
    }

    /// Returns the number of semaphore permits this job must hold while running.
    ///
    /// `Upload` jobs claim their full buffer size. `Copy` jobs claim a fixed 256 KiB token
    /// (they allocate no buffer) to prevent all copy jobs from running simultaneously.
    fn memory_budget_needed(&self) -> u32 {
        match self.part_job_type {
            PartJobType::Upload(_) => self.part_size.min(u32::MAX as usize) as u32,
            PartJobType::Copy { .. } => 256 * 1024, // 256KiB so we don't launch all of these jobs at once
        }
    }

    /// Executes the part job and returns the completed part (ETag + part number) for the manifest.
    async fn execute(
        self,
        bucket_name: String,
        archive_key: String,
        upload_id: String,
    ) -> Result<CompletedPartInfo, Error> {
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

        Ok(CompletedPartInfo { part_number, etag })
    }
}

/// Serializable form of a [`PartJob`] that can be sent to a sub-worker Lambda invocation.
///
/// Only `Upload`-type jobs composed entirely of [`SerializableEntryFromS3`] entries can be
/// delegated; `Copy` jobs and jobs containing a `CentralDirectory` element cannot.
#[derive(Debug, Serialize, Deserialize)]
pub struct DelegatedPartJob {
    /// Part number in the MultiPart upload, begins at 1.
    part_number: i32,
    /// Exact size of this part.
    part_size: usize,
    entries_from_s3: Vec<SerializableEntryFromS3>,
}

impl DelegatedPartJob {
    /// Converts an `Upload`-only [`PartJob`] into a [`DelegatedPartJob`] for cross-Lambda serialization.
    ///
    /// Extracts the shared `cdfh_sender` from the last entry (all entries share the same sender)
    /// and strips the non-serializable channel handle, returning it alongside the delegatable job.
    /// Returns [`Error::InvalidPartJobConversion`] if the job is a `Copy` type, contains
    /// non-`EntryFromS3` elements, or has no entries.
    pub fn new_from_part_job(
        part_job: PartJob,
    ) -> Result<(Self, UnboundedSender<CentralDirectoryFileHeader>), Error> {
        let PartJob {
            part_number,
            part_size,
            part_job_type: PartJobType::Upload(mut elems),
        } = part_job
        else {
            return Err(Error::InvalidPartJobConversion {
                message: "Only \"Upload\" PartJob can be delegated",
                part_job,
            });
        };

        if !elems
            .iter()
            .all(|e| matches!(e, UploadPartElement::EntryFromS3(_)))
        {
            return Err(Error::InvalidPartJobConversion {
                message: "\"Upload\" PartJob must only contain EntryFromS3 to be delegated",
                part_job: PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(elems),
                },
            });
        }

        // The CDFH channel is the same for every PartJob, so we just use the last of the list
        let Some(UploadPartElement::EntryFromS3(EntryFromS3 {
            entry: last_simple_entry,
            cdfh_sender,
        })) = elems.pop()
        else {
            return Err(Error::InvalidPartJobConversion {
                message: "\"Upload\" PartJob must contain at least one EntryFromS3 to be delegated",
                part_job: PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(vec![]),
                },
            });
        };

        let entries_from_s3 = elems
            .into_iter()
            .map(|elem| match elem {
                UploadPartElement::EntryFromS3(entry_from_s3) => entry_from_s3.entry,
                _ => unreachable!(),
            })
            .chain(Some(last_simple_entry))
            .collect();

        Ok((
            DelegatedPartJob {
                part_number,
                part_size,
                entries_from_s3,
            },
            cdfh_sender,
        ))
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
#[derive(Debug, Serialize, Deserialize)]
enum S3ObjectOperation {
    /// Full `GetObject` — downloads the entire file body.
    Get,
    /// Ranged `GetObject` for the first `size` bytes, plus a `HeadObject` for the full CRC32.
    PartialGet { size: usize },
    /// `HeadObject` only — no body download; used when the body is server-side copied.
    Head,
}

/// A ZIP entry that fetches its content from S3 and writes it into an upload buffer.
///
/// Combines the serializable entry data with the non-serializable `cdfh_sender` channel handle.
#[derive(Debug)]
pub struct EntryFromS3 {
    entry: SerializableEntryFromS3,
    /// Channel used to deliver the completed [`CentralDirectoryFileHeader`] after the CRC32 is known.
    cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
}

/// The serializable core of an [`EntryFromS3`]: the pre-planned LOC header, the S3 operation
/// to use for fetching the content and CRC32, and the source object coordinates.
#[derive(Debug, Serialize, Deserialize)]
pub struct SerializableEntryFromS3 {
    loc: LocalFileHeader<NoCRC>,
    operation: S3ObjectOperation,
    bucket_name: Arc<str>,
    key: String,
}

impl EntryFromS3 {
    /// Resolves the element's S3 data and writes ZIP bytes into `buf`.
    ///
    /// For `EntryFromS3`: fetches the CRC32 from S3 (S3 stores it as a base64-encoded,
    /// big-endian u32; [`CRC32::from_str`] decodes and byte-reverses it to little-endian for
    /// the ZIP field), writes the LOC header, sends the completed CDFH to the channel, then
    /// streams the file body (if any) into the remaining buffer space.
    async fn resolve_and_write(self, buf: &mut [u8]) -> Result<(), Error> {
        let EntryFromS3 {
            entry:
                SerializableEntryFromS3 {
                    loc,
                    operation,
                    bucket_name,
                    key,
                },
            cdfh_sender,
        } = self;

        debug!(%bucket_name, %key, ?operation, ?loc, buf_size = buf.len(), "Resolving entry from S3");
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
    /// Returns the number of bytes this element will write into the upload buffer.
    fn elem_size(&self) -> usize {
        let SerializableEntryFromS3 { operation, loc, .. } = &self.entry;
        match operation {
            S3ObjectOperation::Get => loc.size() + loc.file_size(),
            S3ObjectOperation::PartialGet { size } => loc.size() + *size,
            S3ObjectOperation::Head => loc.size(),
        }
    }
}

/// One logical segment within an `UploadPart` buffer.
#[derive(Debug)]
enum UploadPartElement {
    /// A ZIP entry: LOC header + (optionally) file body fetched from S3.
    EntryFromS3(EntryFromS3),
    /// The Central Directory block: drains all CDFHs from the channel, then writes EOCD.
    CentralDirectory {
        total_size: usize,
        eocd64: EndOfCentralDirectory64,
        cdfh_receiver: UnboundedReceiver<CentralDirectoryFileHeader>,
    },
}
impl UploadPartElement {
    /// Writes this element's ZIP bytes into `buf`.
    ///
    /// For `EntryFromS3`: delegates to [`EntryFromS3::resolve_and_write`].
    ///
    /// For `CentralDirectory`: drains the CDFH channel until all entries are received, sorts
    /// them by archive offset via a `BTreeMap` (entries arrive in completion order, not layout
    /// order), then writes all CDFHs followed by the EOCD record(s).
    async fn resolve_and_write(self, buf: &mut [u8]) -> Result<(), Error> {
        match self {
            UploadPartElement::EntryFromS3(entry_from_s3) => {
                entry_from_s3.resolve_and_write(buf).await
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
            UploadPartElement::EntryFromS3(entry_from_s3) => entry_from_s3.elem_size(),
            UploadPartElement::CentralDirectory { total_size, .. } => *total_size,
        }
    }
}
