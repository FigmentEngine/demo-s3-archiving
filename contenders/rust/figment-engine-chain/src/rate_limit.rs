//! Issue-rate limiter (stub).
//!
//! The single speed dial: a `const` max API calls/second, pushed to just under
//! the S3 503 knee, governing how fast links and stitch copies fire. Two-tier
//! priority (segment links high, stitch copies low) prevents the overlapped
//! stitch from starving the segments it depends on. See copy-only-plan.md.

/// Max S3 control-plane calls per second. Start conservative; raise in the bench
/// loop until 503 retries claw back the wall-clock gain (knee ~3,000–4,000/s).
pub const MAX_CALLS_PER_SEC: u32 = 1_000;

// TODO: token-bucket limiter with high/low priority acquire. Placeholder const
// only for now so the executor can reference the dial.
