//! Single-slot fast propagation pool: at most one `(extrinsic, peer_id)` entry.

use sc_network_types::PeerId;
use serde::{Deserialize, Serialize};
use std::sync::{OnceLock, RwLock};

/// One pending fast-propagation transaction and its target peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastPropEntry {
	/// SCALE-encoded extrinsic bytes.
	pub extrinsic: Vec<u8>,
	/// libp2p peer id string (e.g. `12D3KooW…`): fire when this peer announces the target block.
	pub peer_id: String,
	/// Milliseconds to wait after that peer's best block announce before firing (0 = immediate).
	#[serde(default)]
	pub offset_ms: u64,
	/// Fire only on a best announce for this block number from `peer_id` (`0` = next matching).
	#[serde(default)]
	pub target_block_number: u64,
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
}

static POOL: OnceLock<RwLock<Option<FastPropEntry>>> = OnceLock::new();

fn pool() -> &'static RwLock<Option<FastPropEntry>> {
	POOL.get_or_init(|| RwLock::new(None))
}

fn peer_id_matches(expected: &str, actual: &PeerId) -> bool {
	let expected = expected.trim();
	!expected.is_empty() && actual.to_string() == expected
}

/// Insert or replace the single pool slot (later `setFastPropPool` overwrites a stale entry).
pub fn set_pool(entry: FastPropEntry) -> Result<(), &'static str> {
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
		},
		Some(entry) => FastPropPoolView {
			occupied: true,
			extrinsic: Some(entry.extrinsic.clone()),
			peer_id: Some(entry.peer_id.clone()),
			offset_ms: Some(entry.offset_ms),
			target_block_number: Some(entry.target_block_number),
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
