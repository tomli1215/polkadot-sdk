//! Skip outbound block re-announces to selected peers by block height slot.
//!
//! `by_slot` index `i` (0-based) is the **producer of landing block `#N` when
//! `(N - 1) % modulus == i`**. Equivalently, on import of block `#H`, suppress
//! re-announce to index `H % modulus` — that peer builds `#H + 1`, and skipping the outbound
//! tell lets you receive their inbound announce of `#H` (import timing for `#H + 1` authorship).
//!
//! Hot-reload (preferred): set `SYNC_SUPPRESS_REANNOUNCE_MAP_PATH` to a JSON file, e.g.
//! ```json
//! { "modulus": 20, "by_slot": ["12D3Koo…", null, "-", …] }
//! ```
//! or `{ "modulus": 20, "by_slot": "12D3Koo…,-,-,…" }`. The file is re-read when mtime changes.
//!
//! Legacy env (startup only, no hot reload):
//! - `SYNC_SUPPRESS_REANNOUNCE_MODULUS` — default `20` (Aura authority count on Finney).
//! - `SYNC_SUPPRESS_REANNOUNCE_BY_SLOT` — comma-separated peer ids; index `i` as above.
//!   Use `-` or empty field for unused slots.

use sc_network_types::PeerId;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};
use std::time::SystemTime;

const LOG_TARGET: &str = "sync";

#[derive(Clone)]
struct Config {
	modulus: u64,
	by_slot: Vec<Option<PeerId>>,
}

impl Default for Config {
	fn default() -> Self {
		Self { modulus: 20, by_slot: Vec::new() }
	}
}

struct SuppressState {
	map_path: Option<PathBuf>,
	mtime: Option<SystemTime>,
	config: Config,
	env_loaded: bool,
	load_attempted: bool,
}

impl SuppressState {
	fn new() -> Self {
		Self {
			map_path: env_map_path(),
			mtime: None,
			config: Config::default(),
			env_loaded: false,
			load_attempted: false,
		}
	}
}

static STATE: OnceLock<RwLock<SuppressState>> = OnceLock::new();

fn state() -> &'static RwLock<SuppressState> {
	STATE.get_or_init(|| RwLock::new(SuppressState::new()))
}

fn env_map_path() -> Option<PathBuf> {
	let raw = std::env::var("SYNC_SUPPRESS_REANNOUNCE_MAP_PATH").ok()?;
	let trimmed = raw.trim();
	if trimmed.is_empty() {
		None
	} else {
		Some(PathBuf::from(trimmed))
	}
}

fn parse_peer_token(token: &str) -> Option<PeerId> {
	let s = token.trim();
	if s.is_empty() || s == "-" {
		return None;
	}
	PeerId::from_str(s).ok()
}

fn parse_by_slot_csv(raw: &str) -> Vec<Option<PeerId>> {
	raw.split(',').map(parse_peer_token).collect()
}

fn parse_by_slot_json(value: &serde_json::Value) -> Vec<Option<PeerId>> {
	match value {
		serde_json::Value::String(s) => parse_by_slot_csv(s),
		serde_json::Value::Array(items) => items
			.iter()
			.map(|item| match item {
				serde_json::Value::Null => None,
				serde_json::Value::String(s) => parse_peer_token(s),
				_ => None,
			})
			.collect(),
		_ => Vec::new(),
	}
}

#[derive(Deserialize)]
struct SuppressMapFile {
	#[serde(default)]
	modulus: Option<u64>,
	#[serde(default, alias = "peers_by_slot")]
	by_slot: Option<serde_json::Value>,
}

fn load_config_from_env() -> Config {
	let modulus = std::env::var("SYNC_SUPPRESS_REANNOUNCE_MODULUS")
		.ok()
		.and_then(|v| v.trim().parse::<u64>().ok())
		.filter(|&m| m > 0)
		.unwrap_or(20);

	let raw = std::env::var("SYNC_SUPPRESS_REANNOUNCE_BY_SLOT").unwrap_or_default();
	let by_slot = parse_by_slot_csv(&raw);

	Config { modulus, by_slot }
}

fn log_config_loaded(source: &str, cfg: &Config) {
	if cfg.by_slot.iter().any(|p| p.is_some()) {
		let configured = cfg.by_slot.iter().filter(|p| p.is_some()).count();
		log::info!(
			target: LOG_TARGET,
			"suppress reannounce ({source}): modulus={} configured_slots={}/{}",
			cfg.modulus,
			configured,
			cfg.by_slot.len().max(cfg.modulus as usize),
		);
	}
}

fn load_config_from_file(path: &Path) -> Option<Config> {
	let raw = fs::read_to_string(path).map_err(|e| {
		log::warn!(
			target: LOG_TARGET,
			"suppress reannounce read {} failed: {e}",
			path.display(),
		);
		e
	}).ok()?;

	let parsed: SuppressMapFile = serde_json::from_str(&raw).map_err(|e| {
		log::warn!(
			target: LOG_TARGET,
			"suppress reannounce JSON parse failed ({}): {e}",
			path.display(),
		);
		e
	}).ok()?;

	let modulus = parsed
		.modulus
		.filter(|&m| m > 0)
		.or_else(|| {
			std::env::var("SYNC_SUPPRESS_REANNOUNCE_MODULUS")
				.ok()
				.and_then(|v| v.trim().parse::<u64>().ok())
				.filter(|&m| m > 0)
		})
		.unwrap_or(20);

	let by_slot = parsed
		.by_slot
		.as_ref()
		.map(parse_by_slot_json)
		.unwrap_or_default();

	Some(Config { modulus, by_slot })
}

fn reload_if_stale(state: &mut SuppressState) {
	if let Some(path) = state.map_path.clone() {
		let mtime = fs::metadata(&path).ok().and_then(|m| m.modified().ok());
		if !state.load_attempted || mtime != state.mtime {
			state.load_attempted = true;
			state.mtime = mtime;
			if let Some(cfg) = load_config_from_file(&path) {
				log_config_loaded(&format!("file {}", path.display()), &cfg);
				state.config = cfg;
				return;
			}
			if !state.env_loaded {
				state.config = load_config_from_env();
				state.env_loaded = true;
				log_config_loaded("env fallback", &state.config);
			}
		}
		return;
	}

	if !state.env_loaded {
		state.config = load_config_from_env();
		state.env_loaded = true;
		log_config_loaded("env", &state.config);
	}
}

fn active_config() -> Config {
	let mut guard = state()
		.write()
		.unwrap_or_else(|e| e.into_inner());
	reload_if_stale(&mut guard);
	guard.config.clone()
}

fn authority_peer_for_slot_index(cfg: &Config, slot: u64) -> Option<PeerId> {
	let idx = slot as usize;
	cfg.by_slot.get(idx).and_then(|p| p.clone())
}

/// Producer of landing block `#N`: list index `(N - 1) % modulus`.
pub fn authority_peer_for_landing_block(landing_block: u64) -> Option<PeerId> {
	let cfg = active_config();
	if cfg.by_slot.is_empty() || landing_block == 0 {
		return None;
	}
	let slot = (landing_block - 1) % cfg.modulus;
	authority_peer_for_slot_index(&cfg, slot)
}

/// True when this node should **not** send an outbound block announce for imported block
/// `imported_number` to `peer` (index `imported_number % modulus` = producer of `#imported + 1`).
pub fn should_suppress_reannounce(peer: &PeerId, imported_number: u64) -> bool {
	let cfg = active_config();
	if cfg.by_slot.is_empty() {
		return false;
	}
	let slot = imported_number % cfg.modulus;
	let Some(suppressed) = authority_peer_for_slot_index(&cfg, slot) else {
		return false;
	};
	suppressed == *peer
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn suppress_on_import_to_next_block_author() {
		let peer_jtqr =
			PeerId::from_str("12D3KooWJTQrTJAnhs7Bbj3MHq8upuJDuv7XQekPTMktywApJzGs").unwrap();
		let cfg = Config {
			modulus: 20,
			by_slot: {
				let mut v = vec![None; 20];
				v[7] = Some(peer_jtqr.clone());
				v
			},
		};
		// Index 7 = producer of #8336048; suppress on import of parent #8336047.
		assert!(suppresses_import(&cfg, &peer_jtqr, 8336047));
		assert!(!suppresses_import(&cfg, &peer_jtqr, 8336046));
		assert_eq!(
			landing_authority(&cfg, 8336048).as_ref(),
			Some(&peer_jtqr)
		);
	}

	#[test]
	fn landing_authority_slot_index() {
		let peer_a = PeerId::from_str("12D3KooWK1g872Z4BkiMMiyV8eEk47MLkxbMk23GB5JgVrH6fv3g").unwrap();
		let cfg = Config {
			modulus: 20,
			by_slot: {
				let mut v = vec![None; 20];
				v[15] = Some(peer_a.clone());
				v
			},
		};
		// Index 15 produces #8335436 ((8335436-1) % 20 == 15).
		assert_eq!(
			landing_authority(&cfg, 8335436).as_ref(),
			Some(&peer_a)
		);
		assert!(landing_authority(&cfg, 8335435).is_none());
	}

	#[test]
	fn parse_json_by_slot_array_and_csv() {
		let peer =
			PeerId::from_str("12D3KooWJTQrTJAnhs7Bbj3MHq8upuJDuv7XQekPTMktywApJzGs").unwrap();
		let json: serde_json::Value = serde_json::json!([
			"12D3KooWJTQrTJAnhs7Bbj3MHq8upuJDuv7XQekPTMktywApJzGs",
			null,
			"-"
		]);
		let slots = parse_by_slot_json(&json);
		assert_eq!(slots.len(), 3);
		assert_eq!(slots[0], Some(peer));
		assert!(slots[1].is_none());
		assert!(slots[2].is_none());

		let csv = parse_by_slot_csv("12D3KooWJTQrTJAnhs7Bbj3MHq8upuJDuv7XQekPTMktywApJzGs,-");
		assert_eq!(csv.len(), 2);
		assert_eq!(csv[0], Some(peer));
		assert!(csv[1].is_none());
	}

	fn suppresses_import(cfg: &Config, peer: &PeerId, imported: u64) -> bool {
		let slot = imported % cfg.modulus;
		cfg.by_slot
			.get(slot as usize)
			.and_then(|p| p.as_ref())
			.map_or(false, |p| p == peer)
	}

	fn landing_authority(cfg: &Config, landing: u64) -> Option<PeerId> {
		if landing == 0 {
			return None;
		}
		let slot = (landing - 1) % cfg.modulus;
		cfg.by_slot.get(slot as usize).and_then(|p| p.clone())
	}
}
