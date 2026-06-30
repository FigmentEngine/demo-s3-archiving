use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    part_executor::DelegatedPartJob,
    s3_ops::{CompletedPartInfo, MultipartUpload},
    zip_format::CentralDirectoryFileHeader,
};

/// Top-level Lambda event payload, deserialized from the raw JSON invocation body.
///
/// Uses `#[serde(untagged)]` so the variant is inferred from the JSON structure:
/// a payload with a `jobs` array is `DelegatedJobs`, a payload with `bucket_name` /
/// `files_prefix` / `archive_key` is `Archive`, and an empty object `{}` is `Warm`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Invocation {
    /// A batch of pre-planned part jobs delegated by an orchestrating Lambda invocation.
    DelegatedJobs(DelegatedJobs),
    /// The primary archiving job: list, plan, and upload the ZIP.
    Archive(JobInfo),
    /// Cold-start warm-up: initialize the S3 client and return immediately.
    Warm,
}

/// Payload for the `DelegatedJobs` invocation variant: the open multipart upload context and
/// the list of part jobs to execute.
#[derive(Debug, Serialize, Deserialize)]
pub struct DelegatedJobs {
    #[serde(flatten)]
    pub multipart_upload: MultipartUpload,
    pub jobs: Vec<DelegatedPartJob>,
}

/// Response payload returned by a sub-worker Lambda after executing a `DelegatedJobs` batch.
///
/// Contains the completed S3 multipart part descriptors and the Central Directory File Headers
/// produced by the executed entries, both of which the orchestrator merges into its own state.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobsResults {
    pub cdfhs: Vec<CentralDirectoryFileHeader>,
    pub completed_parts: Vec<CompletedPartInfo>,
}

/// Return value of the Lambda handler, serialized as the invocation response payload.
///
/// Uses `#[serde(untagged)]`: `JobsResults` serializes to a JSON object with `cdfhs` and
/// `completed_parts` fields; `NoResult` serializes to `null`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InvocationResult {
    /// Returned by sub-workers after executing a `DelegatedJobs` batch.
    JobsResults(JobsResults),
    /// Returned by `Archive` and `Warm` invocations (no data to return).
    NoResult,
}

/// Lambda event payload describing one archiving job.
///
/// All three fields are `Arc<str>` so they can be cheaply cloned into every spawned task.
/// - `bucket_name` — source and destination bucket.
/// - `files_prefix` — S3 key prefix (without trailing slash) for the source objects.
/// - `archive_key` — destination S3 key for the produced ZIP (e.g. `archives/rust-jrodon.zip`).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobInfo {
    pub bucket_name: Arc<str>,
    pub files_prefix: Arc<str>,
    pub archive_key: Arc<str>,
}
