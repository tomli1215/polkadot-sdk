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
}

static POOL: OnceLock<RwLock<Option<FastPropEntry>>> = OnceLock::new();

fn pool() -> &'static RwLock<Option<FastPropEntry>> {
	POOL.get_or_init(|| RwLock::new(None))
}

/// Insert into the pool. Fails if the pool already holds an entry.
pub fn set_pool(entry: FastPropEntry) -> Result<(), &'static str> {
	let mut guard = pool().write().expect("fast prop pool lock");
	if guard.is_some() {
		return Err("fast prop pool already occupied");
	}
	*guard = Some(entry);
	Ok(())
}

/// Non-destructive read of the pool.
pub fn get_pool() -> FastPropPoolView {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => FastPropPoolView { occupied: false, extrinsic: None, peer_id: None },
		Some(entry) => FastPropPoolView {
			occupied: true,
			extrinsic: Some(entry.extrinsic.clone()),
			peer_id: Some(entry.peer_id.clone()),
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
