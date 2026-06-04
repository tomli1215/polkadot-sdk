//! Skip outbound block re-announces to selected peers by block height slot.
//!
//! `SYNC_SUPPRESS_REANNOUNCE_BY_SLOT` index `i` (0-based) is the **producer of landing block
//! `#N` when `(N - 1) % modulus == i`**. Equivalently, on import of block `#H`, suppress
//! re-announce to index `H % modulus` — that peer builds `#H + 1`, and skipping the outbound
//! tell lets you receive their inbound announce of `#H` (import timing for `#H + 1` authorship).
//!
//! Env:
//! - `SYNC_SUPPRESS_REANNOUNCE_MODULUS` — default `20` (Aura authority count on Finney).
//! - `SYNC_SUPPRESS_REANNOUNCE_BY_SLOT` — comma-separated peer ids; index `i` as above.
//!   Use `-` or empty field for unused slots.

use sc_network_types::PeerId;
use std::str::FromStr;
use std::sync::OnceLock;

const LOG_TARGET: &str = "sync";

struct Config {
	modulus: u64,
	by_slot: Vec<Option<PeerId>>,
}

fn parse_peer_token(token: &str) -> Option<PeerId> {
	let s = token.trim();
	if s.is_empty() || s == "-" {
		return None;
	}
	PeerId::from_str(s).ok()
}

fn load_config() -> Config {
	let modulus = std::env::var("SYNC_SUPPRESS_REANNOUNCE_MODULUS")
		.ok()
		.and_then(|v| v.trim().parse::<u64>().ok())
		.filter(|&m| m > 0)
		.unwrap_or(20);

	let raw = std::env::var("SYNC_SUPPRESS_REANNOUNCE_BY_SLOT")
		.unwrap_or_default();
	let mut by_slot: Vec<Option<PeerId>> = Vec::new();
	for part in raw.split(',') {
		by_slot.push(parse_peer_token(part));
	}

	if by_slot.iter().any(|p| p.is_some()) {
		let configured = by_slot.iter().filter(|p| p.is_some()).count();
		log::info!(
			target: LOG_TARGET,
			"suppress reannounce: modulus={modulus} configured_slots={configured}/{}",
			by_slot.len().max(modulus as usize),
		);
	}

	Config { modulus, by_slot }
}

fn config() -> &'static Config {
	static CONFIG: OnceLock<Config> = OnceLock::new();
	CONFIG.get_or_init(load_config)
}

fn authority_peer_for_slot_index(slot: u64) -> Option<PeerId> {
	let cfg = config();
	let idx = slot as usize;
	cfg.by_slot.get(idx).and_then(|p| p.clone())
}

/// Producer of landing block `#N`: list index `(N - 1) % modulus`.
pub fn authority_peer_for_landing_block(landing_block: u64) -> Option<PeerId> {
	let cfg = config();
	if cfg.by_slot.is_empty() || landing_block == 0 {
		return None;
	}
	let slot = (landing_block - 1) % cfg.modulus;
	authority_peer_for_slot_index(slot)
}

/// True when this node should **not** send an outbound block announce for imported block
/// `imported_number` to `peer` (index `imported_number % modulus` = producer of `#imported + 1`).
pub fn should_suppress_reannounce(peer: &PeerId, imported_number: u64) -> bool {
	let cfg = config();
	if cfg.by_slot.is_empty() {
		return false;
	}
	let slot = imported_number % cfg.modulus;
	let Some(suppressed) = authority_peer_for_slot_index(slot) else {
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
