//! Benchmarked contender Lambda that archives S3 files into a flat, STORED (uncompressed) ZIP.
//!
//! The Lambda handler dispatches on an [`Invocation`] enum with three variants:
//!
//! - **`Archive(JobInfo)`** — the primary archiving job: lists every object under
//!   `{bucket}/{files_prefix}/`, plans a byte-exact ZIP layout upfront so each multipart part
//!   can be produced independently and in parallel, then executes the S3 multipart upload.
//!   Large files are copied S3-side via `UploadPartCopy` (no download); smaller files are
//!   streamed into `UploadPart` buffers. The Central Directory is written last, after all entry
//!   tasks have reported their CRC32s through a channel.
//! - **`DelegatedJobs(DelegatedJobs)`** — sub-worker mode: executes a batch of pre-planned
//!   part jobs delegated by a parent invocation acting as orchestrator, then returns the
//!   completed parts and Central Directory headers in the response payload.
//! - **`Warm`** — cold-start warm-up: initializes the S3 client and returns immediately,
//!   so the Lambda container is ready before real work arrives.
//!
//! ## Fan-out mode (v3 default)
//!
//! When the `FORCE_V2_MODE` environment variable is **unset**, the `Archive` path creates a
//! [`LambdaExecutor`] that fans out part jobs across up to `MAX_LAMBDA_WORKERS` (default 50)
//! concurrent sub-worker invocations of this same Lambda function. When `FORCE_V2_MODE` is
//! **set**, all jobs run locally in the same invocation (the v2 behavior).

// The macro must be declared (textually) before the modules that use it so it is
// in scope for `part_job`. `#[macro_use]` keeps it visible to the child modules.
#[macro_use]
mod tracing_macros {
    /// Emits an `INFO` event every [`TRACING_INFO_FREQUENCY`](crate::TRACING_INFO_FREQUENCY)
    /// calls and `DEBUG` otherwise.
    ///
    /// Used on hot paths (e.g. the per-part execution loop) to keep periodic visibility at
    /// `INFO` without drowning the logs with thousands of lines per run.
    macro_rules! intermittent_tracing {
        ($index:expr, $($tt:tt)+) => {
            if $index as usize % $crate::TRACING_INFO_FREQUENCY == 0 {
                tracing::event!(tracing::Level::INFO, $($tt)+);
            } else {
                tracing::event!(tracing::Level::DEBUG, $($tt)+);
            }
        };
    }
}

mod error;
mod events;
mod lambda_executor;
mod part_executor;
mod s3_ops;
mod shared_buffer;
mod zip_format;
mod zip_layout;

use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};

use tokio::sync::mpsc::unbounded_channel;
use tracing::{info, instrument};

use crate::{
    error::Error,
    events::{DelegatedJobs, Invocation, InvocationResult, JobInfo, JobsResults},
    lambda_executor::LambdaExecutor,
    part_executor::{PartJob, PartJobExecutor},
    s3_ops::{complete_multipart_upload, list_files, start_multipart_upload},
    zip_layout::ZipLayout,
};

// ---------- Tunables ----------

/// Total memory that all concurrently running part-job tasks may allocate for their upload buffers.
///
/// Each `UploadPart` task acquires permits equal to its buffer size before spawning, so this
/// semaphore acts as a backpressure valve. `UploadPartCopy` tasks claim a fixed 256 KiB token
/// (they hold no buffer) just to prevent all copy jobs from launching simultaneously.
/// Central Directory headers are excluded from this budget — they are negligible (~48 KiB per
/// 1 000 entries).
const MAX_PART_JOB_TASKS_MEMORY: usize = 50 * 1024 * 1024; // 50MB

/// Maximum number of delegated parts to give a Lambda sub worker
const LAMBDA_WORKER_PART_JOB_MAX_CHUNK_SIZE: usize = 500;

/// Switch `intermitent_tracing!` to `INFO` once every this many calls (`DEBUG` in between).
const TRACING_INFO_FREQUENCY: usize = 50;

/// Minimum size accepted for a non-final part in an S3 multipart upload (S3 hard limit).
///
/// Both the `UploadPart` and the `UploadPartCopy` halves of a Duo must individually meet this.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB

/// Preferred part size when there are no S3 minimum-size constraints to satisfy.
///
/// Used to drive how much files we pack into a single part before moving on.
const TARGET_PART_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// Lambda entry point: dispatches on the [`Invocation`] variant.
///
/// - `Archive` — lists source objects, plans the ZIP layout, then executes the multipart upload.
///   In v3 mode (default), fans out part jobs to sub-worker Lambda invocations via
///   [`LambdaExecutor`]; in v2 mode (`FORCE_V2_MODE` set), runs all jobs locally.
/// - `DelegatedJobs` — executes a pre-planned batch of part jobs as a sub-worker and returns
///   the completed parts and Central Directory headers in the response.
/// - `Warm` — initializes the S3 client for cold-start warm-up and returns immediately.
#[instrument(skip_all, fields(job_info = ?event.payload))]
async fn handler(event: LambdaEvent<Invocation>) -> Result<InvocationResult, LambdaError> {
    info!("Start processing");

    match event.payload {
        Invocation::DelegatedJobs(DelegatedJobs {
            multipart_upload,
            jobs,
        }) => {
            let (cdfh_sender, mut cdfh_receiver) = unbounded_channel();

            let job_executor = PartJobExecutor::new_from_jobs(
                multipart_upload,
                jobs.into_iter()
                    .map(|delegated_part_job| PartJob::new(delegated_part_job, cdfh_sender.clone()))
                    .collect(),
            );
            // Drop the sender so the cdfh_receiver can return None
            drop(cdfh_sender);

            let (completed_parts, cdfhs) = tokio::join!(job_executor.execute(), async {
                let mut cdfhs = vec![];
                while let Some(cdfh) = cdfh_receiver.recv().await {
                    cdfhs.push(cdfh);
                }
                cdfhs
            });

            Ok(InvocationResult::JobsResults(JobsResults {
                cdfhs,
                completed_parts: completed_parts?,
            }))
        }
        Invocation::Archive(job_info) => {
            // Is it V2 or V3 mode ?
            let lambda_executor = if std::env::var("FORCE_V2_MODE").is_err() {
                let max_lamdba_workers: usize = std::env::var("MAX_LAMBDA_WORKERS")
                    .map(|s| s.parse().ok())
                    .ok()
                    .flatten()
                    .unwrap_or(50);
                Some(LambdaExecutor::new(
                    event.context.invoked_function_arn.into(),
                    max_lamdba_workers,
                ))
            } else {
                None
            };

            // Launch a task to open the Multipart Upload
            let mp_upload = tokio::spawn(start_multipart_upload(job_info.clone()));

            let JobInfo {
                bucket_name,
                files_prefix,
                archive_key,
            } = job_info;

            // List the files from S3
            let files_info = list_files(bucket_name.clone(), &files_prefix).await?;
            let total_bytes: usize = files_info.iter().map(|fi| fi.size).sum();
            info!(
                file_count = files_info.len(),
                total_bytes, %archive_key, "Creating archive"
            );

            // Plan the ZIP layout (decides Single vs Duo parts, copy ranges, etc.)
            let layout = ZipLayout::from_files_info(files_info);

            // Retrieve the Multipart Upload
            let multipart_upload = mp_upload.await??;

            // Turn the layout into concrete S3 multipart jobs and run them.
            let job_executor = PartJobExecutor::new(layout, multipart_upload.clone());

            let job_executor = if let Some(lambda_executor) = lambda_executor {
                job_executor.with_lambda_executor(lambda_executor)
            } else {
                job_executor
            };

            let completed_parts = job_executor.execute().await?;

            // Complete multipart upload
            complete_multipart_upload(multipart_upload, completed_parts).await?;

            info!("Archive created successfully");
            Ok(InvocationResult::NoResult)
        }
        Invocation::Warm => {
            // Create the S3 client
            let _ = s3();
            Ok(InvocationResult::NoResult)
        }
    }
}

// This macro from `awssdk-instrumentation` generates the entire main() function:
// - Initializes the OTel tracer provider with X-Ray exporter
// - Sets up tracing-subscriber with JSON console output and OTel bridge
// - Loads AWS SDK config from environment
// - Creates an instrumented S3 client accessible via `s3()`
// - Wraps the Lambda runtime with the OTel Tower layer
// - Starts the Lambda runtime
//
// See: https://docs.rs/awssdk-instrumentation/latest/awssdk_instrumentation/macro.make_lambda_runtime.html
awssdk_instrumentation::make_lambda_runtime!(
    handler,
    trigger = awssdk_instrumentation::lambda::layer::OTelFaasTrigger::Other,
    s3() -> aws_sdk_s3::Client,
    lambda() -> aws_sdk_lambda::Client
);
