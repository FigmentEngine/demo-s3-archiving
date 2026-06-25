//! Segment-chain executor (stub).
//!
//! Will drive: per-segment link chains (create + copy-forward + append + complete)
//! under the rate limiter, the entry-0 bootstrap self-steal, completion-driven
//! overlapped stitch (fire each stitch copy-part when its segment object finishes),
//! two-tier priority (segment links high, stitch copies low), and the single final
//! CompleteMultipartUpload. See segment-chain-wiring.md.

use aws_sdk_s3::Client;

use crate::plan_chain::ChainPlan;

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
	#[error("segment-chain executor not yet implemented")]
	Unimplemented,
}

/// Execute the plan: build segment objects, stitch into the final archive. STUB.
pub async fn run(
	_s3: &Client,
	_bucket: &str,
	_files_prefix: &str,
	_archive_key: &str,
	_plan: ChainPlan,
) -> Result<(), ChainError> {
	// TODO: implement per copy-only-plan.md.
	Err(ChainError::Unimplemented)
}
