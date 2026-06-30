use std::sync::Arc;

use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::{error::Error, events::JobInfo, s3};

/// Identifies an open S3 multipart upload: the target bucket, destination key, and the
/// upload ID returned by `CreateMultipartUpload`. Serializable so it can be passed to
/// sub-worker Lambda invocations as part of a [`DelegatedJobs`](crate::events::DelegatedJobs) payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub bucket_name: Arc<str>,
    pub archive_key: Arc<str>,
    pub upload_id: Arc<str>,
}

/// The ETag and part number returned by a successful `UploadPart` or `UploadPartCopy` call.
///
/// Serializable so sub-workers can return completed part descriptors to the orchestrator.
/// Converts into [`CompletedPart`](aws_sdk_s3::types::CompletedPart) for the final
/// `CompleteMultipartUpload` call.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompletedPartInfo {
    pub part_number: i32,
    pub etag: String,
}
impl From<CompletedPartInfo> for CompletedPart {
    fn from(value: CompletedPartInfo) -> Self {
        Self::builder()
            .part_number(value.part_number)
            .e_tag(value.etag)
            .build()
    }
}

/// One source S3 object to be archived: its bare filename (ZIP entry name), full S3 bucket/key, and byte size.
#[derive(Debug, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub bucket_name: Arc<str>,
    pub key: String,
    pub size: usize,
}
/// Lists all objects under `{key_prefix}/` using the SDK paginator and returns one [`FileInfo`] per object.
///
/// Strips the prefix from each key to obtain the bare filename used as the ZIP entry name.
#[instrument]
pub async fn list_files(bucket: Arc<str>, key_prefix: &str) -> Result<Vec<FileInfo>, Error> {
    info!("Listing files");

    // Ensure the prefix passed to S3 ends with `/` so we list only objects
    // under the directory and can cleanly strip it to get the filename.
    let s3_prefix = format!("{key_prefix}/");

    let mut paginator = s3()
        .list_objects_v2()
        .bucket(&*bucket)
        .prefix(&s3_prefix)
        .into_paginator()
        .send();

    let mut file_infos = Vec::new();

    while let Some(page) = paginator.next().await {
        for object in page?.contents.unwrap_or_default() {
            let Some(key) = object.key else {
                continue;
            };
            let Some(size) = object.size.map(|s| s as usize) else {
                continue;
            };
            // Skip the prefix "directory marker" itself, if any.
            if key == s3_prefix {
                continue;
            }
            if let Some(filename) = key.strip_prefix(&s3_prefix) {
                if filename.is_empty() {
                    continue;
                }
                file_infos.push(FileInfo {
                    name: filename.to_owned(),
                    bucket_name: bucket.clone(),
                    key,
                    size,
                });
            }
        }
    }

    info!("Listed {} files", file_infos.len());
    Ok(file_infos)
}

/// Initiates an S3 multipart upload for the archive key and returns the resulting upload_id.
#[instrument]
pub async fn start_multipart_upload(job_info: JobInfo) -> Result<MultipartUpload, Error> {
    let JobInfo {
        bucket_name,
        archive_key,
        ..
    } = job_info;

    info!("Starting multipart upload");

    let response = s3()
        .create_multipart_upload()
        .bucket(&*bucket_name)
        .key(&*archive_key)
        .content_type("application/zip")
        .send()
        .await?;

    let upload_id = response
        .upload_id
        .ok_or("No upload ID in multipart upload response")?;

    info!(upload_id, "Multipart upload opened");

    Ok(MultipartUpload {
        bucket_name,
        archive_key,
        upload_id: Arc::from(upload_id),
    })
}

/// Sorts parts by part number and finalizes the multipart upload.
#[instrument(skip(completed_parts))]
pub async fn complete_multipart_upload(
    multipart_upload: MultipartUpload,
    mut completed_parts: Vec<CompletedPartInfo>,
) -> Result<(), Error> {
    info!("Finalizing multipart upload");

    let MultipartUpload {
        bucket_name,
        archive_key,
        upload_id,
    } = multipart_upload;

    // Sort parts by part number
    completed_parts.sort_by_key(|part| part.part_number);

    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts.into_iter().map(From::from).collect()))
        .build();

    s3().complete_multipart_upload()
        .bucket(&*bucket_name)
        .key(&*archive_key)
        .upload_id(&*upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    info!("Multipart upload completed");
    Ok(())
}
