//! Adaptive issue-rate governor for the copy-only chain.
//!
//! ## Why this exists
//!
//! A single chain invocation issues ~22k S3 control-plane calls but, because the
//! links within each segment are serial, it only sustains ~1,500–1,600 calls/s
//! in isolation — well under S3's ~3,500/s per-bucket SlowDown knee. So *solo*
//! it never throttles. The benchmark, however, runs several copies of the
//! contender concurrently (repeat-runs), and N invocations × ~1,570/s crosses
//! the knee even though one alone does not. A single instance cannot see how
//! many siblings share the bucket, so a *fixed* cap is wrong both ways: high
//! enough for the solo case is unsafe when N>1; low enough for N=3 needlessly
//! crawls when alone.
//!
//! ## The approach: AIMD, driven by the SlowDown signal
//!
//! Like TCP congestion control, the governor adapts to the *observed* capacity
//! rather than guessing N:
//!   - start optimistic (near the solo rate),
//!   - on a SlowDown, **multiplicatively decrease** the rate (×BACKOFF),
//!   - on sustained success, **additively increase** it (+RECOVER_PER_SEC),
//!     capped at CEIL.
//! Solo, no 503s ever fire, so the rate stays high and the ~15s wall-clock is
//! preserved. With N concurrent invocations, all of them feel the throttle, all
//! back off, and they self-organise toward a shared sustainable rate — slower
//! under contention, but they *survive* instead of exhausting retries and
//! crashing. No instance needs to know N.
//!
//! ## Two-tier priority
//!
//! The overlapped stitch fires a copy for each segment as it completes, sharing
//! the call budget with the segment links still being built. Under contention,
//! stitch copies must not starve the segment links they depend on, or the run
//! can deadlock (stitch waiting on segments that can't get tokens). So link
//! acquisitions take strict precedence: a low-priority (stitch) acquire yields
//! to any waiting high-priority (link) acquire.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// Starting / recovery-ceiling rate. Near the solo sustainable rate so a lone
/// invocation runs effectively ungoverned.
const START_RATE: f64 = 3_000.0;
const CEIL_RATE: f64 = 3_500.0;
/// Floor: never throttle below this even under heavy contention, or progress
/// stalls. Several concurrent instances at this floor still sum to a modest rate.
const FLOOR_RATE: f64 = 200.0;
/// Multiplicative decrease on a throttle signal.
const BACKOFF: f64 = 0.5;
/// Additive recovery, applied once per RECOVER_INTERVAL of unthrottled running.
const RECOVER_PER_STEP: f64 = 150.0;
const RECOVER_INTERVAL: Duration = Duration::from_millis(500);
/// Token bucket burst ceiling (tokens). Small — we want a smooth rate, not bursts.
const MAX_BURST: f64 = 64.0;

/// Call class for two-tier priority.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Priority {
	/// Segment-link calls — the critical path. Served first.
	High,
	/// Stitch copies and other deferrable calls — yield to High.
	Low,
}

/// Shared, cheaply-cloneable handle to the governor.
#[derive(Clone)]
pub struct RateLimiter {
	inner: Arc<Inner>,
}

struct Inner {
	/// Current refill rate in tokens/sec, stored as bits of an f64 for atomic CAS.
	rate_bits: AtomicU64,
	/// Available tokens, ×1000 fixed-point in an atomic for lock-free acquire.
	tokens_milli: AtomicU64,
	/// Last refill instant, as millis-since-start fixed point.
	last_refill_ms: AtomicU64,
	/// Count of high-priority waiters; a Low acquire defers while this is > 0.
	high_waiters: AtomicU64,
	/// Wakeups for waiters when tokens are added.
	notify: Notify,
	/// Last time we applied an additive recovery step.
	last_recover: std::sync::Mutex<Instant>,
	start: Instant,
}

impl RateLimiter {
	pub fn new() -> Self {
		let now = Instant::now();
		RateLimiter {
			inner: Arc::new(Inner {
				rate_bits: AtomicU64::new(START_RATE.to_bits()),
				tokens_milli: AtomicU64::new((MAX_BURST * 1000.0) as u64),
				last_refill_ms: AtomicU64::new(0),
				high_waiters: AtomicU64::new(0),
				notify: Notify::new(),
				last_recover: std::sync::Mutex::new(now),
				start: now,
			}),
		}
	}

	fn rate(&self) -> f64 {
		f64::from_bits(self.inner.rate_bits.load(Ordering::Relaxed))
	}

	fn set_rate(&self, r: f64) {
		let r = r.clamp(FLOOR_RATE, CEIL_RATE);
		self.inner.rate_bits.store(r.to_bits(), Ordering::Relaxed);
	}

	/// Refill the bucket based on elapsed time at the current rate, and apply an
	/// additive recovery step if we've run clean for RECOVER_INTERVAL.
	fn refill(&self) {
		let now_ms = self.inner.start.elapsed().as_millis() as u64;
		let last = self.inner.last_refill_ms.swap(now_ms, Ordering::Relaxed);
		if now_ms > last {
			let elapsed_s = (now_ms - last) as f64 / 1000.0;
			let add = elapsed_s * self.rate() * 1000.0; // ×1000 fixed point
			let cap = (MAX_BURST * 1000.0) as u64;
			let mut cur = self.inner.tokens_milli.load(Ordering::Relaxed);
			loop {
				let next = ((cur as f64 + add) as u64).min(cap);
				match self.inner.tokens_milli.compare_exchange_weak(
					cur,
					next,
					Ordering::Relaxed,
					Ordering::Relaxed,
				) {
					Ok(_) => break,
					Err(actual) => cur = actual,
				}
			}
		}

		// Additive recovery toward the ceiling on sustained success.
		if let Ok(mut last_rec) = self.inner.last_recover.try_lock() {
			if last_rec.elapsed() >= RECOVER_INTERVAL {
				*last_rec = Instant::now();
				let r = self.rate();
				if r < CEIL_RATE {
					self.set_rate(r + RECOVER_PER_STEP);
				}
			}
		}
	}

	/// Try to take one token without blocking. Returns true on success.
	fn try_take(&self) -> bool {
		self.refill();
		let mut cur = self.inner.tokens_milli.load(Ordering::Relaxed);
		loop {
			if cur < 1000 {
				return false;
			}
			match self.inner.tokens_milli.compare_exchange_weak(
				cur,
				cur - 1000,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return true,
				Err(actual) => cur = actual,
			}
		}
	}

	/// Acquire one token, awaiting if necessary. High-priority acquires are
	/// served ahead of Low: a Low acquire yields while any High waiter is queued.
	pub async fn acquire(&self, prio: Priority) {
		if prio == Priority::High {
			self.inner.high_waiters.fetch_add(1, Ordering::Relaxed);
		}
		loop {
			// Low priority defers to any waiting high-priority acquire.
			if prio == Priority::Low && self.inner.high_waiters.load(Ordering::Relaxed) > 0 {
				self.inner.notify.notified().await;
				continue;
			}
			if self.try_take() {
				if prio == Priority::High {
					self.inner.high_waiters.fetch_sub(1, Ordering::Relaxed);
					// Wake any low waiters that were deferring to us.
					self.inner.notify.notify_waiters();
				}
				return;
			}
			// Not enough tokens yet — wait for a refill/notify, then retry. Use a
			// short timeout so we re-check even without an explicit notification.
			let _ =
				tokio::time::timeout(Duration::from_millis(2), self.inner.notify.notified()).await;
		}
	}

	/// Signal that a call was throttled (SlowDown / 503). Multiplicatively
	/// decreases the rate so concurrent instances converge under the shared knee.
	pub fn on_throttle(&self) {
		let r = self.rate();
		self.set_rate(r * BACKOFF);
		// Drain the bucket so the backoff takes effect immediately rather than
		// letting an accumulated burst keep firing at the old rate.
		self.inner.tokens_milli.store(0, Ordering::Relaxed);
	}

	/// Current rate, for logging.
	pub fn current_rate(&self) -> f64 {
		self.rate()
	}
}

impl Default for RateLimiter {
	fn default() -> Self {
		Self::new()
	}
}
