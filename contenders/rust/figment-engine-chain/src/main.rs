//! figment-engine-chain — the copy-only segment-chain contender.
//!
//! Speed-focused sibling of `figment-engine`: every image body reaches the
//! archive by server-side UploadPartCopy (bar one 5 MiB bootstrap read), so the
//! design escapes the ENI bandwidth floor and is bound only by S3 control-plane
//! call rate.
//!
//! This file is the wiring skeleton: it lists the source objects and reuses the
//! shipped contender's shared modules via the `figment_engine` lib (ZIP layout,
//! CRC decode, planner vocabulary). The planner (`plan_chain`) and executor
//! (`assemble_chain`) are stubbed until implemented — the binary builds and runs,
//! but currently returns `unimplemented` at invoke time.

mod assemble_chain;
mod plan_chain;
mod rate_limit;

use std::sync::Arc;

use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use serde::Deserialize;
use tracing::info;

// Shared from the shipped contender's lib target — no duplication.
use figment_engine::{FileId, SourceFile};

/// Lambda event payload describing one archiving job (same shape as the shipped
/// contender, so the benchmark harness invokes it identically).
#[derive(Debug, Deserialize, Clone)]
struct JobInfo {
	bucket_name: Arc<str>,
	files_prefix: Arc<str>,
	archive_key: Arc<str>,
}

/// List every object under `{files_prefix}/`, returning a `SourceFile` per object.
/// (The shipped contender's lister lives in its `main.rs`/bin, not its lib, so the
/// chain carries its own — it's the only non-shared piece, ~20 lines.)
async fn list_source_files(
	bucket: &str,
	files_prefix: &str,
) -> Result<Vec<SourceFile>, LambdaError> {
	let s3_prefix = format!("{files_prefix}/");
	let mut paginator = s3()
		.list_objects_v2()
		.bucket(bucket)
		.prefix(&s3_prefix)
		.into_paginator()
		.send();

	let mut out = Vec::new();
	let mut next_id: u32 = 0;
	while let Some(page) = paginator.next().await {
		let page = page?;
		for obj in page.contents() {
			let Some(key) = obj.key() else { continue };
			let Some(size) = obj.size() else { continue };
			if key == s3_prefix {
				continue;
			}
			if let Some(name) = key.strip_prefix(&s3_prefix) {
				if name.is_empty() {
					continue;
				}
				out.push(SourceFile {
					id: FileId(next_id),
					key: key.to_string(),
					name: name.to_string(),
					size: size as u64,
				});
				next_id += 1;
			}
		}
	}
	Ok(out)
}

async fn handler(event: LambdaEvent<JobInfo>) -> Result<(), LambdaError> {
	let JobInfo {
		bucket_name,
		files_prefix,
		archive_key,
	} = event.payload;

	info!(%bucket_name, %files_prefix, %archive_key, "figment-engine-chain invoked");

	let files = list_source_files(&bucket_name, &files_prefix).await?;
	info!(count = files.len(), "listed source files");

	// ---- planner + executor: stubbed until implemented ----
	let plan = plan_chain::plan_segment_chain(files);
	assemble_chain::run(&s3(), &bucket_name, &files_prefix, &archive_key, plan).await?;

	info!("archive complete");
	Ok(())
}

awssdk_instrumentation::make_lambda_runtime!(
	handler,
	trigger = awssdk_instrumentation::lambda::layer::OTelFaasTrigger::Other,
	s3() -> aws_sdk_s3::Client
);
