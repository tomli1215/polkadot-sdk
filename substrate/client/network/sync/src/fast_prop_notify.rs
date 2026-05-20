//! In-process broadcast of fast-propagation events for RPC subscribers.

use chrono::{SecondsFormat, Utc};
use futures::channel::mpsc;
use futures::Stream;
use serde::Serialize;
use std::pin::Pin;
use std::sync::{OnceLock, RwLock};

/// Published after a pooled extrinsic is propagated on a best block-head signal.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FastPropFiredNotification {
	pub utc: String,
	pub event: &'static str,
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
	peer_id: &str,
	block_number: u64,
	block_hash: &str,
	extrinsic: &[u8],
) {
	let notification = FastPropFiredNotification {
		utc: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
		event: "fast_prop_fired",
		peer_id: peer_id.to_string(),
		block_number,
		block_hash: block_hash.to_string(),
		extrinsic: format!("0x{}", array_bytes::bytes2hex("", extrinsic)),
		extrinsic_bytes: extrinsic.len(),
	};
	Hub::global().publish(notification);
}
