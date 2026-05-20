//! Optional file log + in-process hub publish for block announces.

use sc_network_types::PeerId;

use crate::block_announce_notify::publish_block_announce_received;

use chrono::{SecondsFormat, Utc};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static LOG_FILE: OnceLock<Mutex<Option<File>>> = OnceLock::new();
static LOG_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();

fn log_path() -> Option<&'static Path> {
	let opt = LOG_PATH.get_or_init(|| {
		std::env::var("BLOCK_ANNOUNCE_LOG_PATH")
			.ok()
			.map(PathBuf::from)
	});
	opt.as_ref().map(|p| p.as_path())
}

fn open_log_file() -> Option<File> {
	let path = log_path()?;
	match OpenOptions::new().create(true).append(true).open(path) {
		Ok(file) => Some(file),
		Err(err) => {
			log::warn!(
				target: crate::BLOCK_ANNOUNCE_LOG,
				"Could not open block announce log {:?}: {err}",
				path
			);
			None
		},
	}
}

fn append_line(line: &str) {
	let Some(path) = log_path() else {
		return;
	};
	let mutex = LOG_FILE.get_or_init(|| Mutex::new(None));
	let mut guard = mutex.lock().expect("block announce log mutex poisoned");
	if guard.is_none() {
		*guard = open_log_file();
	}
	let Some(file) = guard.as_mut() else {
		return;
	};
	if writeln!(file, "{line}").is_err() || file.flush().is_err() {
		log::warn!(
			target: crate::BLOCK_ANNOUNCE_LOG,
			"Failed to write block announce log {:?}",
			path
		);
		*guard = None;
	}
}

/// Log decode instant: hub (RPC) + optional file when ``BLOCK_ANNOUNCE_LOG_PATH`` is set.
pub fn log_block_announce_received(
	peer_id: &PeerId,
	number: u64,
	hash: impl std::fmt::Display,
	is_best: bool,
	local_have_block: bool,
	announce_data_bytes: usize,
) {
	let hash = format!("{hash}");
	publish_block_announce_received(
		peer_id,
		number,
		hash.clone(),
		is_best,
		local_have_block,
		announce_data_bytes,
	);
	if log_path().is_some() {
		let line = format!(
			"utc={} event=block_announce_received peer={peer_id} number={number} hash={hash} \
			is_best={is_best} local_have_block={local_have_block} announce_data_bytes={announce_data_bytes}",
			Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
		);
		append_line(&line);
	}
}
