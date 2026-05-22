//! Fast propagation: fire pooled `(extrinsic, peer_id, offset_ms)` when that peer sends a
//! best block announce for the target height (header only; no wait for download or import).

use crate::fast_prop_pool::{pool_accepts_peer_announce, take_pool, FastPropEntry};
use log::debug;
use sc_network_types::PeerId;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// Context for the block announce that triggered a fast-prop fire.
#[derive(Clone, Debug)]
pub struct FastPropBlockContext {
	pub block_number: u64,
	pub block_hash: String,
	/// RFC3339 UTC when the triggering best block announce was received.
	pub announce_utc: String,
	/// Milliseconds since Unix epoch at announce receipt.
	pub announce_unix_ms: i64,
	/// Same instant as announce (kept for downstream RPC / handler compatibility).
	pub executed_utc: String,
	pub executed_unix_ms: i64,
}

struct FastPropState {
	handler: Option<Arc<FastPropFireHandler>>,
}

static STATE: OnceLock<RwLock<FastPropState>> = OnceLock::new();

fn state() -> &'static RwLock<FastPropState> {
	STATE.get_or_init(|| RwLock::new(FastPropState { handler: None }))
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

/// Best block announce from `peer`: fire immediately when it matches the armed pool entry.
pub fn on_target_peer_block_announced(
	peer: &PeerId,
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
) {
	if !pool_accepts_peer_announce(peer, block_number) {
		return;
	}

	let hash_norm = normalize_hash(&block_hash);
	debug!(
		target: crate::LOG_TARGET,
		"fast prop: best announce #{block_number} from {peer}, firing"
	);

	let ctx = FastPropBlockContext {
		block_number,
		block_hash: hash_norm,
		announce_utc: announce_utc.clone(),
		announce_unix_ms,
		executed_utc: announce_utc,
		executed_unix_ms: announce_unix_ms,
	};

	try_fire(ctx);
}

fn try_fire(ctx: FastPropBlockContext) {
	let Some(entry) = take_pool() else {
		debug!(
			target: crate::LOG_TARGET,
			"fast prop: pool empty at fire time for #{}",
			ctx.block_number,
		);
		return;
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
		"fast prop: scheduling fire {}ms after announce for block #{}",
		offset_ms,
		ctx.block_number,
	);
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(offset_ms)).await;
		handler(entry, ctx);
	});
}
