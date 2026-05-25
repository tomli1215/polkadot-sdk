//! In-process broadcast of fast-propagation events for RPC subscribers.

use chrono::Utc;
use futures::channel::mpsc;
use futures::Stream;
use serde::Serialize;
use std::sync::{OnceLock, RwLock};

/// Published after a pooled extrinsic is propagated on a best block-head signal.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FastPropFiredNotification {
	/// RFC3339 UTC when fast-prop fired (immediately before P2P propagate).
	pub utc: String,
	/// RFC3339 UTC when the triggering best block announce was received.
	pub announce_utc: String,
	/// Configured delay from trigger to fire (`FastPropEntry::offset_ms`).
	pub offset_ms: u64,
	/// Milliseconds from the announce peer's best block announce to fire (actual; includes `offset_ms`).
	pub latency_ms: i64,
	pub event: &'static str,
	/// Announce peer that triggered the fire.
	pub announce_peer_id: String,
	/// Peer that received the P2P extrinsic propagation.
	pub propagate_peer_id: String,
	/// Deprecated alias for `propagate_peer_id`.
	pub peer_id: String,
	#[serde(rename = "number")]
	pub block_number: u64,
	pub block_hash: String,
	/// Hex-encoded extrinsic (`0x…`).
	pub extrinsic: String,
	pub extrinsic_bytes: usize,
}

struct Hub {
	senders: RwLock<Vec<mpsc::UnboundedSender<FastPropFiredNotification>>>,
}

impl Hub {
	fn global() -> &'static Hub {
		static HUB: OnceLock<Hub> = OnceLock::new();
		HUB.get_or_init(|| Hub { senders: RwLock::new(Vec::new()) })
	}

	fn subscribe(&self) -> mpsc::UnboundedReceiver<FastPropFiredNotification> {
		let (tx, rx) = mpsc::unbounded();
		self.senders.write().expect("hub lock").push(tx);
		rx
	}

	fn publish(&self, notification: FastPropFiredNotification) {
		let mut senders = self.senders.write().expect("hub lock");
		senders.retain(|tx| tx.unbounded_send(notification.clone()).is_ok());
	}
}

/// Subscribe to fast-prop fired notifications (for node RPC forwarding).
pub fn subscribe_fast_prop_fired() -> impl Stream<Item = FastPropFiredNotification> {
	Hub::global().subscribe()
}

pub fn publish_fast_prop_fired(
	announce_peer_id: &str,
	propagate_peer_id: &str,
	block_number: u64,
	block_hash: &str,
	extrinsic: &[u8],
	fire_utc: &str,
	announce_utc: &str,
	announce_unix_ms: i64,
	offset_ms: u64,
) {
	let fire_time = Utc::now();
	let latency_ms = fire_time.timestamp_millis().saturating_sub(announce_unix_ms);
	let notification = FastPropFiredNotification {
		utc: fire_utc.to_string(),
		announce_utc: announce_utc.to_string(),
		offset_ms,
		latency_ms,
		event: "fast_prop_fired",
		announce_peer_id: announce_peer_id.to_string(),
		propagate_peer_id: propagate_peer_id.to_string(),
		peer_id: propagate_peer_id.to_string(),
		block_number,
		block_hash: block_hash.to_string(),
		extrinsic: format!("0x{}", array_bytes::bytes2hex("", extrinsic)),
		extrinsic_bytes: extrinsic.len(),
	};
	Hub::global().publish(notification);
}
