//! Fast propagation: fire pooled `(extrinsic, peer_id, offset_ms)` on best block-head signals.

use crate::fast_prop_pool::{take_pool, FastPropEntry};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// Context for the block-head signal that triggered a fast-prop fire.
#[derive(Clone, Debug)]
pub struct FastPropBlockContext {
	pub block_number: u64,
	pub block_hash: String,
	/// RFC3339 UTC when the best block announce was published (same instant as block-announce RPC).
	pub announce_utc: String,
	/// Milliseconds since Unix epoch at announce receipt (for latency to fire).
	pub announce_unix_ms: i64,
}

pub type FastPropFireHandler =
	Box<dyn Fn(FastPropEntry, FastPropBlockContext) + Send + Sync>;

struct HandlerSlot {
	inner: Option<Arc<FastPropFireHandler>>,
}

static HANDLER: OnceLock<RwLock<HandlerSlot>> = OnceLock::new();

fn handler_slot() -> &'static RwLock<HandlerSlot> {
	HANDLER.get_or_init(|| RwLock::new(HandlerSlot { inner: None }))
}

/// Register the node callback (propagate → notify → mempool). Call once at startup.
pub fn set_fire_handler(handler: Arc<FastPropFireHandler>) {
	handler_slot().write().expect("fast prop handler lock").inner = Some(handler);
}

fn invoke_handler(handler: Arc<FastPropFireHandler>, entry: FastPropEntry, ctx: FastPropBlockContext) {
	handler(entry, ctx);
}

/// Called when a best block announce is received (same timing as block-announce RPC).
pub fn try_fire_on_best_block_head(
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
) {
	let Some(entry) = take_pool() else {
		return;
	};
	let ctx = FastPropBlockContext {
		block_number,
		block_hash,
		announce_utc,
		announce_unix_ms,
	};
	let handler = handler_slot().read().expect("fast prop handler lock").inner.clone();
	let Some(handler) = handler else {
		log::warn!(
			target: crate::LOG_TARGET,
			"fast prop pool entry dropped: no fire handler registered"
		);
		return;
	};

	if entry.offset_ms == 0 {
		invoke_handler(handler, entry, ctx);
		return;
	}

	let offset_ms = entry.offset_ms;
	log::debug!(
		target: crate::LOG_TARGET,
		"fast prop: scheduling fire {}ms after block #{} announce",
		offset_ms,
		ctx.block_number,
	);
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(offset_ms)).await;
		invoke_handler(handler, entry, ctx);
	});
}
