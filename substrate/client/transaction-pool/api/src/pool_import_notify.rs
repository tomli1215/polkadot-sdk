//! In-process broadcast when a transaction enters the pool ready queue.
//!
//! Nodes forward this hub to WebSocket subscribers (e.g. filtered stake-pool RPC).

use futures::channel::mpsc;
use futures::Stream;
use serde::Serialize;
use std::pin::Pin;
use std::sync::{OnceLock, RwLock};

/// One ready-queue import (hash + encoded extrinsic).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolImportNotification {
	/// UTC timestamp when the node observed the import.
	pub utc: String,
	pub event: &'static str,
	/// Pool extrinsic hash (hex).
	pub extrinsic_hash: String,
	/// SCALE-encoded extrinsic (`0x` hex).
	pub extrinsic_hex: String,
}

struct Hub {
	senders: RwLock<Vec<mpsc::UnboundedSender<PoolImportNotification>>>,
}

impl Hub {
	fn global() -> &'static Hub {
		static HUB: OnceLock<Hub> = OnceLock::new();
		HUB.get_or_init(|| Hub { senders: RwLock::new(Vec::new()) })
	}

	fn subscribe(&self) -> mpsc::UnboundedReceiver<PoolImportNotification> {
		let (tx, rx) = mpsc::unbounded();
		self.senders.write().expect("hub lock").push(tx);
		rx
	}

	fn publish(&self, notification: PoolImportNotification) {
		let mut senders = self.senders.write().expect("hub lock");
		senders.retain(|tx| tx.unbounded_send(notification.clone()).is_ok());
	}
}

/// Subscribe to ready-pool import notifications (for node RPC forwarding).
pub fn subscribe_pool_imports() -> impl Stream<Item = PoolImportNotification> {
	Hub::global().subscribe()
}

/// Publish one ready-pool import.
pub fn publish_pool_import(extrinsic_hash: String, extrinsic_hex: String, utc: &str) {
	let utc = if utc.is_empty() {
		utc_now_rfc3339_millis()
	} else {
		utc.to_string()
	};
	let notification = PoolImportNotification {
		utc,
		event: "pool_import_ready",
		extrinsic_hash,
		extrinsic_hex,
	};
	Hub::global().publish(notification);
}

fn utc_now_rfc3339_millis() -> String {
	use std::time::{SystemTime, UNIX_EPOCH};
	let dur = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default();
	format!(
		"{}.{:03}Z",
		dur.as_secs(),
		dur.subsec_millis()
	)
}
