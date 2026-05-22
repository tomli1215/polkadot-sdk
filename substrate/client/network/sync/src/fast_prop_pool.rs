//! Single-slot fast propagation pool: at most one `(extrinsic, peer_id)` entry.

use serde::{Deserialize, Serialize};
use std::sync::{OnceLock, RwLock};

/// One pending fast-propagation transaction and its target peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastPropEntry {
	/// SCALE-encoded extrinsic bytes.
	pub extrinsic: Vec<u8>,
	/// libp2p peer id string (e.g. `12D3KooW…`).
	pub peer_id: String,
	/// Milliseconds to wait after block execution (import) before firing (0 = immediate).
	#[serde(default)]
	pub offset_ms: u64,
	/// Fire only when this block number is executed as new best (`0` = next matching best).
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

/// Whether to store this best announce in the pending slot.
///
/// - Pool empty: always record (so an announce before `setFastPropPool` is not lost).
/// - Pool set with a target height: only record matching heights (do not overwrite
///   with the next block's announce while waiting to execute the target).
pub fn should_record_pending_announce(block_number: u64) -> bool {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => true,
		Some(entry) =>
			entry.target_block_number == 0 || entry.target_block_number == block_number,
	}
}

/// Returns true if the pool is set and accepts firing for `block_number`.
pub fn pool_accepts_block(block_number: u64) -> bool {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => false,
		Some(entry) =>
			entry.target_block_number == 0 || entry.target_block_number == block_number,
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
