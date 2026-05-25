//! Single-slot fast propagation pool: at most one `(extrinsic, peer_id)` entry.

use sc_network_types::PeerId;
use serde::{Deserialize, Serialize};
use std::sync::{OnceLock, RwLock};

/// When to fire after the pool peer's best block announce at the target height.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum FastPropFireMode {
	/// Fire `offset_ms` after the pool peer's best block announce (default).
	#[default]
	OnAnnounce = 0,
	/// Fire `offset_ms` after this node finishes importing that block locally.
	OnBlockImport = 1,
}

/// One pending fast-propagation transaction and its target peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastPropEntry {
	/// SCALE-encoded extrinsic bytes.
	pub extrinsic: Vec<u8>,
	/// libp2p peer id string (e.g. `12D3KooW…`): fire when this peer announces the target block.
	pub peer_id: String,
	/// Milliseconds to wait after the fire trigger (announce or import per `fire_mode`).
	#[serde(default)]
	pub offset_ms: u64,
	/// Fire only on a best announce for this block number from `peer_id` (`0` = next matching).
	#[serde(default)]
	pub target_block_number: u64,
	/// [`FastPropFireMode`] as `u8` (`0` = on announce, `1` = on local import).
	#[serde(default)]
	pub fire_mode: u8,
}

/// Snapshot returned by RPC `get` (entry may be present without consuming the pool).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FastPropPoolView {
	pub occupied: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub extrinsic: Option<Vec<u8>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub peer_id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub offset_ms: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub target_block_number: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub fire_mode: Option<u8>,
}

struct PendingImportFire {
	entry: FastPropEntry,
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
}

static POOL: OnceLock<RwLock<Option<FastPropEntry>>> = OnceLock::new();
static PENDING_IMPORT: OnceLock<RwLock<Option<PendingImportFire>>> = OnceLock::new();

fn pool() -> &'static RwLock<Option<FastPropEntry>> {
	POOL.get_or_init(|| RwLock::new(None))
}

fn pending_import() -> &'static RwLock<Option<PendingImportFire>> {
	PENDING_IMPORT.get_or_init(|| RwLock::new(None))
}

fn peer_id_matches(expected: &str, actual: &PeerId) -> bool {
	let expected = expected.trim();
	!expected.is_empty() && actual.to_string() == expected
}

/// Insert or replace the single pool slot (later `setFastPropPool` overwrites a stale entry).
pub fn set_pool(entry: FastPropEntry) -> Result<(), &'static str> {
	clear_pending_import();
	let mut guard = pool().write().expect("fast prop pool lock");
	*guard = Some(entry);
	Ok(())
}

/// Non-destructive read of the pool.
pub fn get_pool() -> FastPropPoolView {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => FastPropPoolView {
			occupied: false,
			extrinsic: None,
			peer_id: None,
			offset_ms: None,
			target_block_number: None,
			fire_mode: None,
		},
		Some(entry) => FastPropPoolView {
			occupied: true,
			extrinsic: Some(entry.extrinsic.clone()),
			peer_id: Some(entry.peer_id.clone()),
			offset_ms: Some(entry.offset_ms),
			target_block_number: Some(entry.target_block_number),
			fire_mode: Some(entry.fire_mode),
		},
	}
}

/// Returns true if the pool is armed and this peer's best announce at `block_number` should fire.
pub fn pool_accepts_peer_announce(peer: &PeerId, block_number: u64) -> bool {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => false,
		Some(entry) => {
			if entry.target_block_number != 0 && entry.target_block_number != block_number {
				return false;
			}
			peer_id_matches(&entry.peer_id, peer)
		},
	}
}

/// Remove and return the entry, if any.
pub fn take_pool() -> Option<FastPropEntry> {
	pool().write().expect("fast prop pool lock").take()
}

/// Clear the pool (e.g. after a failed fire).
pub fn clear_pool() {
	*pool().write().expect("fast prop pool lock") = None;
}

/// Mode 1: hold pool entry until local import of the announced block.
pub fn set_pending_import(
	entry: FastPropEntry,
	block_number: u64,
	block_hash: String,
	announce_utc: String,
	announce_unix_ms: i64,
) {
	*pending_import().write().expect("fast prop pending lock") = Some(PendingImportFire {
		entry,
		block_number,
		block_hash,
		announce_utc,
		announce_unix_ms,
	});
}

/// Take pending import fire state only when `block_number` and `block_hash` match.
pub fn take_pending_import_if_matches(
	block_number: u64,
	block_hash: &str,
) -> Option<(FastPropEntry, u64, String, String, i64)> {
	let mut guard = pending_import().write().expect("fast prop pending lock");
	let pending = guard.as_ref()?;
	if pending.block_number != block_number || pending.block_hash != block_hash {
		return None;
	}
	guard.take().map(|p| {
		(
			p.entry,
			p.block_number,
			p.block_hash,
			p.announce_utc,
			p.announce_unix_ms,
		)
	})
}

pub fn clear_pending_import() {
	*pending_import().write().expect("fast prop pending lock") = None;
}
