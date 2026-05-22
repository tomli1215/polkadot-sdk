//! Fast propagation: fire pooled `(extrinsic, peer_id, offset_ms)` after a best block is executed.
//!
//! Trigger: client import notification → `SyncingService::new_best_block_imported` →
//! `on_best_block_executed` (Wasm `execute_block` and DB commit finished).
//! Announce only records pending metadata (`announce_utc`) for latency reporting.
//! Optional `target_block_number` on the pool entry restricts fires to that height.

use crate::fast_prop_pool::{pool_accepts_block, take_pool, FastPropEntry};
use chrono::{SecondsFormat, Utc};
use log::{debug, trace};
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
	/// RFC3339 UTC when the block finished import (execution + state commit).
	pub executed_utc: String,
	/// Milliseconds since Unix epoch at import completion.
	pub executed_unix_ms: i64,
}

/// Pending best-head from the latest matching `is_best` announce (single slot).
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

fn normalize_hash(hash: &str) -> String {
	let h = hash.trim().trim_start_matches("0x").to_lowercase();
	if h.is_empty() {
		hash.trim().to_string()
	} else {
		format!("0x{h}")
	}
}

/// Register the node callback (propagate → notify → mempool). Call once at startup.
pub fn set_fire_handler(handler: Arc<FastPropFireHandler>) {
	state().write().expect("fast prop lock").handler = Some(handler);
}

/// Record a best block announce (metadata only; does not fire).
pub fn on_best_block_announced(
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
) {
	if !pool_accepts_block(block_number) {
		trace!(
			target: crate::LOG_TARGET,
			"fast prop: ignoring best announce #{block_number} (pool target mismatch or empty)"
		);
		return;
	}

	let mut guard = state().write().expect("fast prop lock");
	guard.pending = Some(PendingBestAnnounce {
		block_number,
		block_hash: normalize_hash(&block_hash),
		announce_utc,
		announce_unix_ms,
	});
	debug!(
		target: crate::LOG_TARGET,
		"fast prop: pending best announce registered #{block_number}"
	);
}

/// Called when a block is imported as the new best (execution finished).
pub fn on_best_block_executed(block_number: u64, block_hash: String) {
	if !pool_accepts_block(block_number) {
		return;
	}

	let hash_norm = normalize_hash(&block_hash);
	let pending = {
		let mut guard = state().write().expect("fast prop lock");
		match guard.pending.take() {
			Some(p) if p.block_number == block_number => PendingBestAnnounce {
				block_hash: hash_norm,
				..p
			},
			Some(p) => {
				if p.block_number != block_number {
					trace!(
						target: crate::LOG_TARGET,
						"fast prop: executed #{block_number} stale pending #{}",
						p.block_number,
					);
					guard.pending = Some(p);
				}
				trace!(
					target: crate::LOG_TARGET,
					"fast prop: executed #{block_number} (pool armed, no matching announce)"
				);
				PendingBestAnnounce {
					block_number,
					block_hash: hash_norm,
					announce_utc: String::new(),
					announce_unix_ms: 0,
				}
			},
			None => {
				trace!(
					target: crate::LOG_TARGET,
					"fast prop: executed #{block_number} with no prior announce (pool armed)"
				);
				PendingBestAnnounce {
					block_number,
					block_hash: hash_norm,
					announce_utc: String::new(),
					announce_unix_ms: 0,
				}
			},
		}
	};

	let executed_time = Utc::now();
	let executed_utc = executed_time.to_rfc3339_opts(SecondsFormat::Millis, true);
	let executed_unix_ms = executed_time.timestamp_millis();

	debug!(
		target: crate::LOG_TARGET,
		"fast prop: block #{block_number} executed (new best), firing"
	);

	try_fire(pending, executed_utc, executed_unix_ms);
}

fn try_fire(
	pending: PendingBestAnnounce,
	executed_utc: String,
	executed_unix_ms: i64,
) {
	let Some(entry) = take_pool() else {
		debug!(
			target: crate::LOG_TARGET,
			"fast prop: pool empty at fire time for #{}",
			pending.block_number,
		);
		return;
	};

	let ctx = FastPropBlockContext {
		block_number: pending.block_number,
		block_hash: pending.block_hash,
		announce_utc: pending.announce_utc,
		announce_unix_ms: pending.announce_unix_ms,
		executed_utc,
		executed_unix_ms,
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
	debug!(
		target: crate::LOG_TARGET,
		"fast prop: scheduling fire {}ms after block #{} execution",
		offset_ms,
		ctx.block_number,
	);
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(offset_ms)).await;
		handler(entry, ctx);
	});
}
