use std::fs;
use std::io;
use std::num::NonZeroU32;
use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

const VAULT_SCHEMA: &str = "agent-harness.encrypted-vault.v1";
const KDF_ITERATIONS: u32 = 210_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultPutOptions {
    pub vault_file: PathBuf,
    pub passphrase: String,
    pub name: String,
    pub secret: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultGetOptions {
    pub vault_file: PathBuf,
    pub passphrase: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptedVaultFile {
    pub schema: String,
    pub kdf: String,
    pub kdf_iterations: u32,
    pub salt_b64: String,
    pub records: Vec<EncryptedVaultRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptedVaultRecord {
    pub name: String,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

pub fn put_vault_secret(options: VaultPutOptions) -> io::Result<EncryptedVaultFile> {
    let mut vault = match fs::read_to_string(&options.vault_file) {
        Ok(text) => serde_json::from_str::<EncryptedVaultFile>(&text).map_err(io::Error::other)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => new_vault()?,
        Err(error) => return Err(error),
    };
    let key = derive_key(&options.passphrase, &vault)?;
    let nonce = random_bytes::<12>()?;
    let mut in_out = options.secret;
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::empty(),
        &mut in_out,
    )
    .map_err(|_| io::Error::other("vault encryption failed"))?;
    let record = EncryptedVaultRecord {
        name: options.name,
        nonce_b64: BASE64.encode(nonce),
        ciphertext_b64: BASE64.encode(in_out),
    };
    vault.records.retain(|item| item.name != record.name);
    vault.records.push(record);
    vault
        .records
        .sort_by(|left, right| left.name.cmp(&right.name));
    if let Some(parent) = options.vault_file.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::write_json_atomic(&options.vault_file, &vault)?;
    Ok(vault)
}

pub fn get_vault_secret(options: VaultGetOptions) -> io::Result<Option<Vec<u8>>> {
    let text = match fs::read_to_string(&options.vault_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let vault = serde_json::from_str::<EncryptedVaultFile>(&text).map_err(io::Error::other)?;
    let Some(record) = vault
        .records
        .iter()
        .find(|record| record.name == options.name)
    else {
        return Ok(None);
    };
    let key = derive_key(&options.passphrase, &vault)?;
    let nonce_bytes = BASE64
        .decode(&record.nonce_b64)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let nonce: [u8; 12] = nonce_bytes
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid vault nonce length"))?;
    let mut in_out = BASE64
        .decode(&record.ciphertext_b64)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let plain = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::empty(),
            &mut in_out,
        )
        .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "vault unlock failed"))?;
    Ok(Some(plain.to_vec()))
}

fn new_vault() -> io::Result<EncryptedVaultFile> {
    Ok(EncryptedVaultFile {
        schema: VAULT_SCHEMA.to_string(),
        kdf: "pbkdf2-hmac-sha256".to_string(),
        kdf_iterations: KDF_ITERATIONS,
        salt_b64: BASE64.encode(random_bytes::<16>()?),
        records: Vec::new(),
    })
}

fn derive_key(passphrase: &str, vault: &EncryptedVaultFile) -> io::Result<LessSafeKey> {
    let salt = BASE64
        .decode(&vault.salt_b64)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut key_bytes = [0u8; 32];
    let iterations = NonZeroU32::new(vault.kdf_iterations)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "kdf iterations must be > 0"))?;
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        &salt,
        passphrase.as_bytes(),
        &mut key_bytes,
    );
    let key = UnboundKey::new(&CHACHA20_POLY1305, &key_bytes)
        .map_err(|_| io::Error::other("vault key creation failed"))?;
    Ok(LessSafeKey::new(key))
}

fn random_bytes<const N: usize>() -> io::Result<[u8; N]> {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; N];
    rng.fill(&mut bytes)
        .map_err(|_| io::Error::other("secure random generation failed"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn vault_round_trips_without_plaintext_file_content() {
        let root = temp_root("vault_round_trips_without_plaintext_file_content");
        let vault_file = root.join("secrets").join("vault.json");
        put_vault_secret(VaultPutOptions {
            vault_file: vault_file.clone(),
            passphrase: "correct horse battery staple".to_string(),
            name: "OPENROUTER_API_KEY".to_string(),
            secret: b"sk-test-secret".to_vec(),
        })
        .unwrap();

        let text = fs::read_to_string(&vault_file).unwrap();
        assert!(!text.contains("sk-test-secret"));
        let secret = get_vault_secret(VaultGetOptions {
            vault_file,
            passphrase: "correct horse battery staple".to_string(),
            name: "OPENROUTER_API_KEY".to_string(),
        })
        .unwrap()
        .unwrap();
        assert_eq!(secret, b"sk-test-secret");

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-vault-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
