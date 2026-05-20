//! Fast propagation: fire pooled `(extrinsic, peer_id)` on best block-head signals.

use crate::fast_prop_pool::{take_pool, FastPropEntry};
use std::sync::{Arc, OnceLock, RwLock};

/// Context for the block-head signal that triggered a fast-prop fire.
#[derive(Clone, Debug)]
pub struct FastPropBlockContext {
	pub block_number: u64,
	pub block_hash: String,
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

/// Called when a best block announce is received (same timing as block-announce RPC).
pub fn try_fire_on_best_block_head(block_number: u64, block_hash: String) {
	let Some(entry) = take_pool() else {
		return;
	};
	let ctx = FastPropBlockContext { block_number, block_hash };
	let handler = handler_slot().read().expect("fast prop handler lock").inner.clone();
	match handler {
		Some(h) => h(entry, ctx),
		None => {
			log::warn!(
				target: crate::LOG_TARGET,
				"fast prop pool entry dropped: no fire handler registered"
			);
		},
	}
}
