//! Fast propagation: fire pooled `(extrinsic, peer_id, offset_ms, fire_mode)` when the pool
//! trigger matches (mode 0: announce peer best announce; mode 1/2: any peer announce then
//! local import; mode 2: or matching `Ethereum.transact` at target height; mode 3: transact match
//! only after the first best announce at `N-1` opens the gate (baseline = ready pool at that announce).

use crate::fast_prop_pool::{
	clear_pending_import, pool_accepts_peer_announce, record_announce_ready_pool_baseline,
	set_pending_import, take_pending_import_if_matches,
	take_pending_import_on_mixed_transact_match, take_pool, take_pool_on_mixed_transact_match,
	try_open_transact_gate_on_announce, FastPropEntry, FastPropFireMode,
};
use log::debug;
use sc_network_types::PeerId;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// What caused the pool to fire (reflected in RPC `fireTrigger`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FastPropFireTrigger {
	/// Mode 0: announce peer best block announce at target height.
	OnAnnounce,
	/// Mode 1 / mode 2 import path: local block import matching pending announce.
	BlockImport,
	/// Mode 2: new `Ethereum.transact` with EVM `to` in `watch_call_addresses`.
	TransactMatch,
}

impl FastPropFireTrigger {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::OnAnnounce => "onAnnounce",
			Self::BlockImport => "blockImport",
			Self::TransactMatch => "transactMatch",
		}
	}

	/// Mode 2 transact match fires immediately; import/announce honor `offset_ms`.
	pub fn applies_offset_ms(self) -> bool {
		!matches!(self, Self::TransactMatch)
	}
}

/// Context for the block announce / import that triggered a fast-prop fire.
#[derive(Clone, Debug)]
pub struct FastPropBlockContext {
	pub block_number: u64,
	pub block_hash: String,
	/// RFC3339 UTC when the triggering best block announce was received.
	pub announce_utc: String,
	/// Milliseconds since Unix epoch at announce receipt.
	pub announce_unix_ms: i64,
	/// RFC3339 UTC when the fire trigger fired (announce or local import per `fire_mode`).
	pub executed_utc: String,
	/// Milliseconds since Unix epoch at fire trigger.
	pub executed_unix_ms: i64,
	pub trigger: FastPropFireTrigger,
	/// Set when `trigger` is [`FastPropFireTrigger::TransactMatch`].
	pub matched_call_address: Option<String>,
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

/// Best block announce from `peer`: arm or fire when it matches the pool entry.
pub fn on_target_peer_block_announced(
	peer: &PeerId,
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
	local_have_block: bool,
) {
	record_announce_ready_pool_baseline(block_number);
	if try_open_transact_gate_on_announce(block_number) {
		debug!(
			target: crate::LOG_TARGET,
			"fast prop: mode 3 transact gate opened on best announce #{block_number} from {peer}"
		);
	}

	if !pool_accepts_peer_announce(peer, block_number) {
		return;
	}

	let Some(entry) = take_pool() else {
		return;
	};

	let fire_mode = entry.fire_mode;
	let hash_norm = normalize_hash(&block_hash);

	if FastPropFireMode::from_u8(fire_mode)
		.is_some_and(|m| m.uses_import_pending_from_announce())
	{
		debug!(
			target: crate::LOG_TARGET,
			"fast prop: best announce #{block_number} from {peer}, waiting for local import (mode={fire_mode})"
		);
		set_pending_import(
			entry,
			block_number,
			hash_norm.clone(),
			announce_utc.clone(),
			announce_unix_ms,
		);
		if local_have_block {
			on_block_imported(block_number, &hash_norm);
		}
		return;
	}

	debug!(
		target: crate::LOG_TARGET,
		"fast prop: best announce #{block_number} from {peer}, firing (mode=announce)"
	);

	let ctx = FastPropBlockContext {
		block_number,
		block_hash: hash_norm,
		announce_utc: announce_utc.clone(),
		announce_unix_ms,
		executed_utc: announce_utc,
		executed_unix_ms: announce_unix_ms,
		trigger: FastPropFireTrigger::OnAnnounce,
		matched_call_address: None,
	};

	schedule_fire(entry, ctx);
}

/// Local block import finished: fire in mode 1 when it matches pending pool peer announce.
pub fn on_block_imported(block_number: u64, block_hash: &str) {
	let hash_norm = normalize_hash(block_hash);
	let Some((entry, _pending_number, _pending_hash, announce_utc, announce_unix_ms)) =
		take_pending_import_if_matches(block_number, &hash_norm)
	else {
		return;
	};

	let import_time = chrono::Utc::now();
	let executed_utc = import_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
	let executed_unix_ms = import_time.timestamp_millis();

	debug!(
		target: crate::LOG_TARGET,
		"fast prop: block #{block_number} imported locally, firing (mode=import)"
	);

	let ctx = FastPropBlockContext {
		block_number,
		block_hash: hash_norm,
		announce_utc,
		announce_unix_ms,
		executed_utc,
		executed_unix_ms,
		trigger: FastPropFireTrigger::BlockImport,
		matched_call_address: None,
	};

	schedule_fire(entry, ctx);
}

/// Mode 2: a new `Ethereum.transact` entered the ready pool with a matching EVM `to` address.
pub fn on_mixed_mode_ethereum_transact(
	call_to: [u8; 20],
	block_number: u64,
	block_hash: String,
) {
	let matched_call_address =
		Some(crate::fast_prop_pool::normalize_call_address_bytes(&call_to));

	if let Some(entry) = take_pool_on_mixed_transact_match(&call_to, block_number) {
		clear_pending_import();
		let now = chrono::Utc::now();
		let executed_utc = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
		let executed_unix_ms = now.timestamp_millis();
		let notify_block = if entry.target_block_number > 0 {
			entry.target_block_number
		} else {
			block_number
		};
		debug!(
			target: crate::LOG_TARGET,
			"fast prop: matching Ethereum.transact in ready pool, firing (mode=mixed/transact) notify_block=#{notify_block} import_best=#{block_number}"
		);
		let ctx = FastPropBlockContext {
			block_number: notify_block,
			block_hash: normalize_hash(&block_hash),
			announce_utc: executed_utc.clone(),
			announce_unix_ms: executed_unix_ms,
			executed_utc,
			executed_unix_ms,
			trigger: FastPropFireTrigger::TransactMatch,
			matched_call_address: matched_call_address.clone(),
		};
		schedule_fire(entry, ctx);
		return;
	}

	let Some((entry, pending_number, pending_hash, announce_utc, announce_unix_ms)) =
		take_pending_import_on_mixed_transact_match(&call_to, block_number)
	else {
		return;
	};

	let now = chrono::Utc::now();
	let executed_utc = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
	let executed_unix_ms = now.timestamp_millis();
	debug!(
		target: crate::LOG_TARGET,
		"fast prop: matching Ethereum.transact while pending import, firing (mode=mixed/transact)"
	);
	let ctx = FastPropBlockContext {
		block_number: pending_number,
		block_hash: pending_hash,
		announce_utc,
		announce_unix_ms,
		executed_utc,
		executed_unix_ms,
		trigger: FastPropFireTrigger::TransactMatch,
		matched_call_address,
	};
	schedule_fire(entry, ctx);
}

fn schedule_fire(entry: FastPropEntry, ctx: FastPropBlockContext) {
	let handler = state().read().expect("fast prop lock").handler.clone();
	let Some(handler) = handler else {
		log::warn!(
			target: crate::LOG_TARGET,
			"fast prop pool entry dropped: no fire handler registered"
		);
		clear_pending_import();
		return;
	};

	let offset_ms = if ctx.trigger.applies_offset_ms() {
		entry.offset_ms
	} else {
		0
	};

	if offset_ms == 0 {
		handler(entry, ctx);
		return;
	}

	debug!(
		target: crate::LOG_TARGET,
		"fast prop: scheduling fire {}ms after trigger ({}) for block #{}",
		offset_ms,
		ctx.trigger.as_str(),
		ctx.block_number,
	);
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(offset_ms)).await;
		handler(entry, ctx);
	});
}
