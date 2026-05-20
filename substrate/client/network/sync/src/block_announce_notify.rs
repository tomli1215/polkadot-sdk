//! In-process broadcast of `/block-announces/1` receipts for RPC subscribers.

use futures::channel::mpsc;
use futures::Stream;
use sc_network_types::PeerId;
use serde::Serialize;
use std::pin::Pin;
use std::sync::{OnceLock, RwLock};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockAnnounceNotification {
	pub utc: String,
	pub event: &'static str,
	pub peer: String,
	#[serde(rename = "number")]
	pub block_number: u64,
	pub hash: String,
	pub is_best: bool,
	pub local_have_block: bool,
	pub announce_data_bytes: usize,
}

struct Hub {
	senders: RwLock<Vec<mpsc::UnboundedSender<BlockAnnounceNotification>>>,
}

impl Hub {
	fn global() -> &'static Hub {
		static HUB: OnceLock<Hub> = OnceLock::new();
		HUB.get_or_init(|| Hub { senders: RwLock::new(Vec::new()) })
	}

	fn subscribe(&self) -> mpsc::UnboundedReceiver<BlockAnnounceNotification> {
		let (tx, rx) = mpsc::unbounded();
		self.senders.write().expect("hub lock").push(tx);
		rx
	}

	fn publish(&self, notification: BlockAnnounceNotification) {
		let mut senders = self.senders.write().expect("hub lock");
		senders.retain(|tx| tx.unbounded_send(notification.clone()).is_ok());
	}
}

/// Subscribe to block announce notifications (for node RPC forwarding).
pub fn subscribe_block_announces() -> impl Stream<Item = BlockAnnounceNotification> {
	Hub::global().subscribe()
}

pub fn publish_block_announce_received(
	peer_id: &PeerId,
	number: u64,
	hash: String,
	is_best: bool,
	local_have_block: bool,
	announce_data_bytes: usize,
	announce_utc: &str,
) {
	let notification = BlockAnnounceNotification {
		utc: announce_utc.to_string(),
		event: "block_announce_received",
		peer: peer_id.to_string(),
		block_number: number,
		hash,
		is_best,
		local_have_block,
		announce_data_bytes,
	};
	Hub::global().publish(notification);
}
