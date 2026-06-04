//! Skip outbound block re-announces to selected peers by block height slot.
//!
//! Used to avoid telling an authority we already know block `H` when `H % modulus`
//! matches that authority's slot, so we can still receive their inbound announce and
//! estimate when they imported `H`.
//!
//! Env:
//! - `SYNC_SUPPRESS_REANNOUNCE_MODULUS` — default `20` (Aura authority count on Finney).
//! - `SYNC_SUPPRESS_REANNOUNCE_BY_SLOT` — comma-separated peer ids, index `i` = slot
//!   `i % modulus`. Use `-` or empty field for slots with no suppression.

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

/// Authority peer configured for `block_number % modulus` (producer slot for block `#N`).
pub fn authority_peer_for_block(block_number: u64) -> Option<PeerId> {
	let cfg = config();
	if cfg.by_slot.is_empty() {
		return None;
	}
	let slot = block_number % cfg.modulus;
	authority_peer_for_slot_index(slot)
}

fn authority_peer_for_slot_index(slot: u64) -> Option<PeerId> {
	let cfg = config();
	let idx = slot as usize;
	cfg.by_slot.get(idx).and_then(|p| p.clone())
}

/// True when this node should **not** send an outbound block announce for `block_number`
/// to `peer` (so the peer is not told we already know the block).
pub fn should_suppress_reannounce(peer: &PeerId, block_number: u64) -> bool {
	let cfg = config();
	if cfg.by_slot.is_empty() {
		return false;
	}
	let slot = block_number % cfg.modulus;
	let Some(suppressed) = authority_peer_for_slot_index(slot) else {
		return false;
	};
	suppressed == *peer
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn suppress_matches_slot_peer() {
		let peer_a = PeerId::from_str("12D3KooWK1g872Z4BkiMMiyV8eEk47MLkxbMk23GB5JgVrH6fv3g").unwrap();
		let cfg = Config {
			modulus: 20,
			by_slot: {
				let mut v = vec![None; 20];
				v[15] = Some(peer_a);
				v
			},
		};
		assert!(slot_matches(&cfg, &peer_a, 8335435)); // 8335435 % 20 == 15
		assert!(!slot_matches(&cfg, &peer_a, 8335436));
	}

	fn slot_matches(cfg: &Config, peer: &PeerId, block_number: u64) -> bool {
		slot_authority(cfg, block_number).as_ref() == Some(peer)
	}

	#[test]
	fn authority_peer_for_block_slot() {
		let peer_a = PeerId::from_str("12D3KooWK1g872Z4BkiMMiyV8eEk47MLkxbMk23GB5JgVrH6fv3g").unwrap();
		let cfg = Config {
			modulus: 20,
			by_slot: {
				let mut v = vec![None; 20];
				v[15] = Some(peer_a.clone());
				v
			},
		};
		assert_eq!(
			slot_authority(&cfg, 8335435).as_ref(),
			Some(&peer_a)
		);
		assert!(slot_authority(&cfg, 8335436).is_none());
	}

	fn slot_authority(cfg: &Config, block_number: u64) -> Option<PeerId> {
		let slot = block_number % cfg.modulus;
		cfg.by_slot
			.get(slot as usize)
			.and_then(|p| p.clone())
	}
}
