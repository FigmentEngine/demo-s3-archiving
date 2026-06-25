//! Segment-chain planner (stub).
//!
//! Will compute: largest-first big sort, one segment per big, spread-thin small
//! allocation, per-segment link sequences (bootstrap self-steal for entry 0,
//! trailing-header links), exact final-archive offsets for the central directory,
//! and the final-stitch descriptor. See the design spec (copy-only-plan.md) and
//! wiring doc (segment-chain-wiring.md) for the full algorithm.
//!
//! Shares `zip_format`, `crc` and the `SourceFile`/`FileId` vocabulary from the
//! `figment_engine` lib — those are NOT reimplemented here.

use figment_engine::SourceFile;

/// The assembled plan the executor consumes. Fields TBD as the planner lands.
#[derive(Debug, Default)]
pub struct ChainPlan {
	pub segment_count: usize,
	// segments, stitch descriptor, entry layout table, stats — to come.
}

/// Build the segment-chain plan from the listed objects. STUB.
pub fn plan_segment_chain(_files: Vec<SourceFile>) -> ChainPlan {
	// TODO: implement per copy-only-plan.md. Round-trip test first.
	ChainPlan::default()
}
