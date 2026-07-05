//! Server-brokered authorization tickets for direct iroh connections.
//!
//! iroh authenticates the *transport* cryptographically (each endpoint has a
//! node keypair), but it knows nothing about m87 *authorization* — "is this
//! user allowed to control this device?". Over the server relay the server
//! answers that and the device simply trusts the relayed stream. A direct
//! CLI→device iroh connection has no server in the middle, so the device needs
//! an equivalent server-vouched proof.
//!
//! An [`IrohTicket`] is that proof: the server (which already authenticated the
//! CLI and enforced access) signs a short-lived ticket pinned to one device.
//! The device verifies the server's Ed25519 signature — it never re-checks the
//! CLI's credentials, so this works for any caller the server accepts (OAuth
//! users *and* API keys). The device learns the server's public key over its
//! existing, already-trusted heartbeat channel.

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Authorization payload granting a CLI the right to open a direct iroh
/// connection to one specific device for a short window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IrohTicket {
    /// Short id of the device this ticket authorizes. Pins the ticket to one
    /// device so it cannot be replayed against another.
    pub device_short_id: String,
    /// Identity the server authenticated (email / sub / org id). Informational
    /// and for audit — authorization was already enforced at issue time.
    pub subject: String,
    /// Issue time (unix ms).
    pub issued_at_ms: u64,
    /// Expiry (unix ms). Keep short; the CLI fetches a fresh ticket per connect.
    pub expires_at_ms: u64,
}

/// A signed envelope: the exact payload bytes that were signed, plus the
/// Ed25519 signature over them. Verification runs over the transmitted bytes,
/// so no canonical-JSON dance is needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedIrohTicket {
    /// base64 of the JSON-serialized [`IrohTicket`].
    pub payload: String,
    /// base64 of the Ed25519 signature over the decoded `payload` bytes.
    pub sig: String,
}

/// Why a ticket was rejected.
#[derive(Debug)]
pub enum TicketError {
    /// Malformed base64 / JSON / key length.
    Decode(String),
    /// Signature did not verify against the expected key.
    BadSignature,
    /// `now` is past the ticket's expiry.
    Expired,
    /// Ticket was issued for a different device than the one verifying it.
    WrongDevice { expected: String, got: String },
}

impl fmt::Display for TicketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TicketError::Decode(e) => write!(f, "malformed iroh ticket: {e}"),
            TicketError::BadSignature => write!(f, "iroh ticket signature did not verify"),
            TicketError::Expired => write!(f, "iroh ticket expired"),
            TicketError::WrongDevice { expected, got } => {
                write!(f, "iroh ticket is for device {got}, not {expected}")
            }
        }
    }
}

impl std::error::Error for TicketError {}

impl SignedIrohTicket {
    /// Verify the server signature, the device binding and the expiry, and
    /// return the inner ticket on success.
    pub fn verify(
        &self,
        verifying_key: &VerifyingKey,
        expected_device_short_id: &str,
        now_ms: u64,
    ) -> Result<IrohTicket, TicketError> {
        let payload = B64
            .decode(self.payload.as_bytes())
            .map_err(|e| TicketError::Decode(e.to_string()))?;
        let sig_bytes = B64
            .decode(self.sig.as_bytes())
            .map_err(|e| TicketError::Decode(e.to_string()))?;
        let sig = Signature::from_slice(&sig_bytes).map_err(|_| TicketError::BadSignature)?;

        verifying_key
            .verify(&payload, &sig)
            .map_err(|_| TicketError::BadSignature)?;

        let ticket: IrohTicket =
            serde_json::from_slice(&payload).map_err(|e| TicketError::Decode(e.to_string()))?;

        if ticket.device_short_id != expected_device_short_id {
            return Err(TicketError::WrongDevice {
                expected: expected_device_short_id.to_string(),
                got: ticket.device_short_id,
            });
        }
        if now_ms > ticket.expires_at_ms {
            return Err(TicketError::Expired);
        }
        Ok(ticket)
    }
}

/// Server-side signer. Holds the private signing key; the CLI never sees it.
pub struct IrohTicketSigner {
    key: SigningKey,
}

impl IrohTicketSigner {
    /// Generate a fresh random signing key (dev / e2e; production should load a
    /// fleet-wide key via [`from_seed_b64`](Self::from_seed_b64) so any server
    /// instance signs tickets the device will trust).
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).expect("OS RNG must be available");
        Self {
            key: SigningKey::from_bytes(&seed),
        }
    }

    /// Load a signing key from a base64-encoded 32-byte seed.
    pub fn from_seed_b64(seed_b64: &str) -> Result<Self, TicketError> {
        let bytes = B64
            .decode(seed_b64.trim().as_bytes())
            .map_err(|e| TicketError::Decode(e.to_string()))?;
        let seed: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| TicketError::Decode("signing seed must be 32 bytes".into()))?;
        Ok(Self {
            key: SigningKey::from_bytes(&seed),
        })
    }

    /// base64 of the 32-byte seed — for persisting / sharing the key.
    pub fn seed_b64(&self) -> String {
        B64.encode(self.key.to_bytes())
    }

    /// base64 of the public verifying key — advertised to devices.
    pub fn public_b64(&self) -> String {
        B64.encode(self.key.verifying_key().to_bytes())
    }

    /// Sign a ticket.
    pub fn sign(&self, ticket: &IrohTicket) -> SignedIrohTicket {
        let payload = serde_json::to_vec(ticket).expect("IrohTicket serializes");
        let sig = self.key.sign(&payload);
        SignedIrohTicket {
            payload: B64.encode(&payload),
            sig: B64.encode(sig.to_bytes()),
        }
    }
}

/// Parse a base64-encoded Ed25519 public key (as advertised in the heartbeat).
pub fn verifying_key_from_b64(public_b64: &str) -> Result<VerifyingKey, TicketError> {
    let bytes = B64
        .decode(public_b64.trim().as_bytes())
        .map_err(|e| TicketError::Decode(e.to_string()))?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| TicketError::Decode("public key must be 32 bytes".into()))?;
    VerifyingKey::from_bytes(&arr).map_err(|_| TicketError::Decode("invalid public key".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket(device: &str, exp: u64) -> IrohTicket {
        IrohTicket {
            device_short_id: device.to_string(),
            subject: "user@example.com".to_string(),
            issued_at_ms: 1_000,
            expires_at_ms: exp,
        }
    }

    #[test]
    fn round_trips_and_verifies() {
        let signer = IrohTicketSigner::generate();
        let vk = verifying_key_from_b64(&signer.public_b64()).unwrap();
        let signed = signer.sign(&ticket("abc123", 10_000));

        let out = signed.verify(&vk, "abc123", 5_000).unwrap();
        assert_eq!(out.device_short_id, "abc123");
        assert_eq!(out.subject, "user@example.com");
    }

    #[test]
    fn rejects_expired() {
        let signer = IrohTicketSigner::generate();
        let vk = verifying_key_from_b64(&signer.public_b64()).unwrap();
        let signed = signer.sign(&ticket("abc123", 10_000));
        assert!(matches!(
            signed.verify(&vk, "abc123", 10_001),
            Err(TicketError::Expired)
        ));
    }

    #[test]
    fn rejects_wrong_device() {
        let signer = IrohTicketSigner::generate();
        let vk = verifying_key_from_b64(&signer.public_b64()).unwrap();
        let signed = signer.sign(&ticket("abc123", 10_000));
        assert!(matches!(
            signed.verify(&vk, "other", 5_000),
            Err(TicketError::WrongDevice { .. })
        ));
    }

    #[test]
    fn rejects_signature_from_another_key() {
        let signer = IrohTicketSigner::generate();
        let attacker = IrohTicketSigner::generate();
        let attacker_vk = verifying_key_from_b64(&attacker.public_b64()).unwrap();
        let signed = signer.sign(&ticket("abc123", 10_000));
        assert!(matches!(
            signed.verify(&attacker_vk, "abc123", 5_000),
            Err(TicketError::BadSignature)
        ));
    }

    #[test]
    fn rejects_tampered_payload() {
        let signer = IrohTicketSigner::generate();
        let vk = verifying_key_from_b64(&signer.public_b64()).unwrap();
        let mut signed = signer.sign(&ticket("abc123", 10_000));
        // Swap the payload for a different device while keeping the old sig.
        signed.payload = B64.encode(serde_json::to_vec(&ticket("evil", 10_000)).unwrap());
        assert!(matches!(
            signed.verify(&vk, "evil", 5_000),
            Err(TicketError::BadSignature)
        ));
    }

    #[test]
    fn seed_b64_round_trips() {
        let signer = IrohTicketSigner::generate();
        let restored = IrohTicketSigner::from_seed_b64(&signer.seed_b64()).unwrap();
        assert_eq!(signer.public_b64(), restored.public_b64());
    }
}
