//! Single-slot fast propagation pool: at most one armed extrinsic entry.

use sc_network_types::PeerId;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::{OnceLock, RwLock};

/// When to fire after the announce peer's best block announce at the target height.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum FastPropFireMode {
	/// Fire `offset_ms` after the announce peer's best block announce (default).
	#[default]
	OnAnnounce = 0,
	/// Fire `offset_ms` after this node finishes importing that block locally.
	OnBlockImport = 1,
	/// Fire on matching incoming `Ethereum.transact` **or** after local block import (mode 1 path).
	Mixed = 2,
}

impl FastPropFireMode {
	pub fn from_u8(v: u8) -> Option<Self> {
		match v {
			0 => Some(Self::OnAnnounce),
			1 => Some(Self::OnBlockImport),
			2 => Some(Self::Mixed),
			_ => None,
		}
	}
}

/// One pending fast-propagation transaction and its peer targets.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastPropEntry {
	/// SCALE-encoded extrinsic bytes.
	pub extrinsic: Vec<u8>,
	/// libp2p peer id: fire when this peer best-announces the target block.
	#[serde(alias = "peer_id")]
	pub announce_peer_id: String,
	/// libp2p peer ids: P2P propagation targets when firing (all receive the extrinsic).
	/// JSON may use legacy key `propagate_peer_id` (string) or `propagate_peer_ids` (array).
	#[serde(
		default,
		alias = "propagate_peer_id",
		deserialize_with = "deserialize_propagate_peer_ids"
	)]
	pub propagate_peer_ids: Vec<String>,
	/// Milliseconds to wait after the fire trigger (announce or import per `fire_mode`).
	#[serde(default)]
	pub offset_ms: u64,
	/// Fire only on a best announce for this block number from `announce_peer_id` (`0` = next matching).
	#[serde(default)]
	pub target_block_number: u64,
	/// [`FastPropFireMode`] as `u8` (`0` = on announce, `1` = on local import, `2` = mixed).
	#[serde(default)]
	pub fire_mode: u8,
	/// Mode 2: inner EVM `to` addresses (`0x` + 40 hex) for incoming `Ethereum.transact`.
	#[serde(default, alias = "callAddresses")]
	pub watch_call_addresses: Vec<String>,
}

/// Snapshot returned by RPC `get` (entry may be present without consuming the pool).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FastPropPoolView {
	pub occupied: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub extrinsic: Option<Vec<u8>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub announce_peer_id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub propagate_peer_id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub propagate_peer_ids: Option<Vec<String>>,
	/// Deprecated alias for `announce_peer_id`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub peer_id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub offset_ms: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub target_block_number: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub fire_mode: Option<u8>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub watch_call_addresses: Option<Vec<String>>,
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
static IMPORT_BASELINE: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();

fn pool() -> &'static RwLock<Option<FastPropEntry>> {
	POOL.get_or_init(|| RwLock::new(None))
}

fn deserialize_propagate_peer_ids<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
	D: Deserializer<'de>,
{
	struct PropagatePeerIdsVisitor;

	impl<'de> serde::de::Visitor<'de> for PropagatePeerIdsVisitor {
		type Value = Vec<String>;

		fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
			formatter.write_str("a peer id string or an array of peer id strings")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			let trimmed = value.trim();
			if trimmed.is_empty() {
				Ok(Vec::new())
			} else {
				Ok(vec![trimmed.to_string()])
			}
		}

		fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
		where
			A: serde::de::SeqAccess<'de>,
		{
			let mut out = Vec::new();
			while let Some(value) = seq.next_element::<String>()? {
				let trimmed = value.trim();
				if !trimmed.is_empty() {
					out.push(trimmed.to_string());
				}
			}
			Ok(out)
		}
	}

	deserializer.deserialize_any(PropagatePeerIdsVisitor)
}

fn dedupe_peer_id_strings(peer_ids: Vec<String>) -> Vec<String> {
	let mut seen = HashSet::new();
	peer_ids
		.into_iter()
		.filter(|id| !id.trim().is_empty())
		.filter(|id| seen.insert(id.clone()))
		.collect()
}

impl FastPropEntry {
	/// Dedupe non-empty propagate peer ids.
	pub fn normalize_propagate_peers(&mut self) {
		self.propagate_peer_ids = dedupe_peer_id_strings(std::mem::take(&mut self.propagate_peer_ids));
	}

	pub fn primary_propagate_peer_id(&self) -> &str {
		self.propagate_peer_ids
			.first()
			.map(|s| s.as_str())
			.filter(|s| !s.is_empty())
			.unwrap_or("")
	}
}

fn pending_import() -> &'static RwLock<Option<PendingImportFire>> {
	PENDING_IMPORT.get_or_init(|| RwLock::new(None))
}

fn import_baseline() -> &'static RwLock<HashSet<String>> {
	IMPORT_BASELINE.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Normalize an EVM call target to `0x` + 40 lowercase hex digits.
pub fn normalize_call_address(addr: &str) -> Option<String> {
	let hex = addr.trim().trim_start_matches("0x").to_lowercase();
	if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
		return None;
	}
	Some(format!("0x{hex}"))
}

/// Normalize 20-byte H160 to the same string form as [`normalize_call_address`].
pub fn normalize_call_address_bytes(call_to: &[u8; 20]) -> String {
	let mut hex = array_bytes::bytes2hex("", call_to);
	hex.make_ascii_lowercase();
	format!("0x{hex}")
}

fn target_block_matches(entry: &FastPropEntry, block_number: u64) -> bool {
	entry.target_block_number == 0 || entry.target_block_number == block_number
}

fn call_address_matches(entry: &FastPropEntry, call_to: &[u8; 20]) -> bool {
	if entry.watch_call_addresses.is_empty() {
		return false;
	}
	let normalized = normalize_call_address_bytes(call_to);
	entry.watch_call_addresses.iter().any(|a| a == &normalized)
}

fn peer_id_matches(expected: &str, actual: &PeerId) -> bool {
	let expected = expected.trim();
	!expected.is_empty() && actual.to_string() == expected
}

/// Snapshot ready-pool tx hashes already present when mode 2 is armed (not fired on these).
pub fn set_import_baseline(hashes: impl IntoIterator<Item = String>) {
	let mut guard = import_baseline().write().expect("fast prop baseline lock");
	guard.clear();
	guard.extend(hashes);
}

/// Clear the import baseline (modes 0/1 or after disarm).
pub fn clear_import_baseline() {
	import_baseline().write().expect("fast prop baseline lock").clear();
}

/// True when `hash` was in the ready pool at arm time (mode 2).
pub fn is_import_baseline_hash(hash: &str) -> bool {
	import_baseline().read().expect("fast prop baseline lock").contains(hash)
}

/// Mode 2 armed in the pool slot or waiting for local import after announce.
pub fn mixed_mode_active() -> bool {
	let pool_armed = pool()
		.read()
		.expect("fast prop pool lock")
		.as_ref()
		.is_some_and(|e| e.fire_mode == FastPropFireMode::Mixed as u8);
	if pool_armed {
		return true;
	}
	pending_import()
		.read()
		.expect("fast prop pending lock")
		.as_ref()
		.is_some_and(|p| p.entry.fire_mode == FastPropFireMode::Mixed as u8)
}

/// Insert or replace the single pool slot (later `setFastPropPool` overwrites a stale entry).
pub fn set_pool(mut entry: FastPropEntry) -> Result<(), &'static str> {
	entry.normalize_propagate_peers();
	if entry.propagate_peer_ids.is_empty() {
		return Err("propagate_peer_ids must not be empty");
	}
	clear_pending_import();
	if entry.fire_mode != FastPropFireMode::Mixed as u8 {
		clear_import_baseline();
	}
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
			announce_peer_id: None,
			propagate_peer_id: None,
			propagate_peer_ids: None,
			peer_id: None,
			offset_ms: None,
			target_block_number: None,
			fire_mode: None,
			watch_call_addresses: None,
		},
		Some(entry) => FastPropPoolView {
			occupied: true,
			extrinsic: Some(entry.extrinsic.clone()),
			announce_peer_id: Some(entry.announce_peer_id.clone()),
			propagate_peer_id: Some(entry.primary_propagate_peer_id().to_string()),
			propagate_peer_ids: Some(entry.propagate_peer_ids.clone()),
			peer_id: Some(entry.announce_peer_id.clone()),
			offset_ms: Some(entry.offset_ms),
			target_block_number: Some(entry.target_block_number),
			fire_mode: Some(entry.fire_mode),
			watch_call_addresses: if entry.watch_call_addresses.is_empty() {
				None
			} else {
				Some(entry.watch_call_addresses.clone())
			},
		},
	}
}

/// Returns true if the pool is armed and this peer's best announce at `block_number` should fire.
pub fn pool_accepts_peer_announce(peer: &PeerId, block_number: u64) -> bool {
	let guard = pool().read().expect("fast prop pool lock");
	match guard.as_ref() {
		None => false,
		Some(entry) => {
			if !target_block_matches(entry, block_number) {
				return false;
			}
			peer_id_matches(&entry.announce_peer_id, peer)
		},
	}
}

/// Mode 2: take the armed entry when a new `Ethereum.transact` matches `watch_call_addresses`.
pub fn take_pool_on_mixed_transact_match(
	call_to: &[u8; 20],
	block_number: u64,
) -> Option<FastPropEntry> {
	let mut guard = pool().write().expect("fast prop pool lock");
	let entry = guard.as_ref()?;
	if entry.fire_mode != FastPropFireMode::Mixed as u8 {
		return None;
	}
	if !target_block_matches(entry, block_number) {
		return None;
	}
	if !call_address_matches(entry, call_to) {
		return None;
	}
	guard.take()
}

/// Mode 2: fire while waiting for local import after a matching announce.
pub fn take_pending_import_on_mixed_transact_match(
	call_to: &[u8; 20],
	block_number: u64,
) -> Option<(FastPropEntry, u64, String, String, i64)> {
	let mut guard = pending_import().write().expect("fast prop pending lock");
	let pending = guard.as_ref()?;
	if pending.entry.fire_mode != FastPropFireMode::Mixed as u8 {
		return None;
	}
	if !target_block_matches(&pending.entry, block_number) {
		return None;
	}
	if !call_address_matches(&pending.entry, call_to) {
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

/// Remove and return the entry, if any.
pub fn take_pool() -> Option<FastPropEntry> {
	pool().write().expect("fast prop pool lock").take()
}

/// Clear the pool (e.g. after a failed fire).
pub fn clear_pool() {
	*pool().write().expect("fast prop pool lock") = None;
	clear_import_baseline();
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
