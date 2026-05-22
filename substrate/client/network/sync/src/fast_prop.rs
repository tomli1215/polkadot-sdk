//! Fast propagation: fire pooled `(extrinsic, peer_id, offset_ms)` after a best block is available.
//!
//! Default trigger is **block download complete** (block response processed in chain sync), not
//! the initial block announce. If the node already had the block at announce time, fire runs
//! immediately (no download needed).

use crate::fast_prop_pool::{take_pool, FastPropEntry};
use chrono::{SecondsFormat, Utc};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// Context for the block that triggered a fast-prop fire.
#[derive(Clone, Debug)]
pub struct FastPropBlockContext {
	pub block_number: u64,
	pub block_hash: String,
	/// RFC3339 UTC when the best block announce was received.
	pub announce_utc: String,
	/// Milliseconds since Unix epoch at announce receipt.
	pub announce_unix_ms: i64,
	/// RFC3339 UTC when the block became available locally (download finished or already had block).
	pub download_utc: String,
	/// Milliseconds since Unix epoch at download / local-availability time.
	pub download_unix_ms: i64,
}

/// Pending best-head from the latest `is_best` announce (single slot, matches pool).
#[derive(Clone, Debug)]
struct PendingBestAnnounce {
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
}

struct FastPropState {
	handler: Option<Arc<FastPropFireHandler>>,
	pending: Option<PendingBestAnnounce>,
}

static STATE: OnceLock<RwLock<FastPropState>> = OnceLock::new();

fn state() -> &'static RwLock<FastPropState> {
	STATE.get_or_init(|| {
		RwLock::new(FastPropState { handler: None, pending: None })
	})
}

pub type FastPropFireHandler =
	Box<dyn Fn(FastPropEntry, FastPropBlockContext) + Send + Sync>;

/// Register the node callback (propagate → notify → mempool). Call once at startup.
pub fn set_fire_handler(handler: Arc<FastPropFireHandler>) {
	state().write().expect("fast prop lock").handler = Some(handler);
}

/// Record a best block announce and optionally fire immediately if we already have the block.
pub fn on_best_block_announced(
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
	have_block: bool,
) {
	{
		let mut guard = state().write().expect("fast prop lock");
		guard.pending = Some(PendingBestAnnounce {
			block_number,
			block_hash: block_hash.clone(),
			announce_utc,
			announce_unix_ms,
		});
	}

	if have_block {
		on_block_downloaded(block_number, block_hash);
	}
}

/// Called when block data for a downloaded block is available (before import queue).
pub fn on_block_downloaded(block_number: u64, block_hash: String) {
	let pending = {
		let mut guard = state().write().expect("fast prop lock");
		match guard.pending.take() {
			Some(p) if p.block_hash == block_hash && p.block_number == block_number => p,
			Some(p) => {
				guard.pending = Some(p);
				return;
			},
			None => return,
		}
	};

	let download_time = Utc::now();
	let download_utc = download_time.to_rfc3339_opts(SecondsFormat::Millis, true);
	let download_unix_ms = download_time.timestamp_millis();

	try_fire(
		pending,
		download_utc,
		download_unix_ms,
	);
}

fn try_fire(
	pending: PendingBestAnnounce,
	download_utc: String,
	download_unix_ms: i64,
) {
	let Some(entry) = take_pool() else {
		return;
	};

	let ctx = FastPropBlockContext {
		block_number: pending.block_number,
		block_hash: pending.block_hash,
		announce_utc: pending.announce_utc,
		announce_unix_ms: pending.announce_unix_ms,
		download_utc,
		download_unix_ms,
	};

	let handler = state().read().expect("fast prop lock").handler.clone();
	let Some(handler) = handler else {
		log::warn!(
			target: crate::LOG_TARGET,
			"fast prop pool entry dropped: no fire handler registered"
		);
		return;
	};

	if entry.offset_ms == 0 {
		handler(entry, ctx);
		return;
	}

	let offset_ms = entry.offset_ms;
	log::debug!(
		target: crate::LOG_TARGET,
		"fast prop: scheduling fire {}ms after block #{} download",
		offset_ms,
		ctx.block_number,
	);
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(offset_ms)).await;
		handler(entry, ctx);
	});
}
