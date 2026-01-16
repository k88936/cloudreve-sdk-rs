//! AES-256-CTR encryption support for uploads

use crate::uploader::error::{UploadError, UploadResult};
use aes::Aes256;
use aes::cipher::{KeyIvInit, StreamCipher};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::{Bytes, BytesMut};
use cloudreve_api::models::explorer::EncryptMetadata;
use ctr::Ctr128BE;
use std::io::{Read, Seek, SeekFrom};
use tokio::io::{AsyncRead, AsyncReadExt};

type Aes256Ctr = Ctr128BE<Aes256>;

/// Encryption configuration derived from EncryptMetadata
#[derive(Clone)]
pub struct EncryptionConfig {
    /// AES-256 key (32 bytes)
    key: [u8; 32],
    /// Initial IV/nonce (16 bytes)
    iv: [u8; 16],
}

impl EncryptionConfig {
    /// Create encryption config from Cloudreve's encrypt metadata
    pub fn from_metadata(metadata: &EncryptMetadata) -> UploadResult<Self> {
        let key_bytes = BASE64
            .decode(&metadata.key_plain_text)
            .map_err(|e| UploadError::EncryptionError(format!("Invalid key: {}", e)))?;

        let iv_bytes = BASE64
            .decode(&metadata.iv)
            .map_err(|e| UploadError::EncryptionError(format!("Invalid IV: {}", e)))?;

        if key_bytes.len() != 32 {
            return Err(UploadError::EncryptionError(format!(
                "Invalid key length: expected 32, got {}",
                key_bytes.len()
            )));
        }

        if iv_bytes.len() != 16 {
            return Err(UploadError::EncryptionError(format!(
                "Invalid IV length: expected 16, got {}",
                iv_bytes.len()
            )));
        }

        let mut key = [0u8; 32];
        let mut iv = [0u8; 16];
        key.copy_from_slice(&key_bytes);
        iv.copy_from_slice(&iv_bytes);

        Ok(Self { key, iv })
    }

    /// Create a cipher instance with counter adjusted for the given byte offset
    fn create_cipher_at_offset(&self, byte_offset: u64) -> Aes256Ctr {
        // Calculate block offset and offset within block
        let block_offset = byte_offset / 16;

        // Increment the counter by block_offset blocks
        let mut counter = self.iv;
        Self::increment_counter(&mut counter, block_offset);

        Aes256Ctr::new(&self.key.into(), &counter.into())
    }

    /// Increment a 128-bit counter by the given number of blocks (big-endian)
    fn increment_counter(counter: &mut [u8; 16], blocks: u64) {
        let mut carry = blocks;
        for i in (0..16).rev() {
            if carry == 0 {
                break;
            }
            let sum = counter[i] as u64 + (carry & 0xFF);
            counter[i] = (sum & 0xFF) as u8;
            carry = (carry >> 8) + (sum >> 8);
        }
    }

    /// Encrypt data in place starting at the given byte offset
    pub fn encrypt_at_offset(&self, data: &mut [u8], byte_offset: u64) {
        let mut cipher = self.create_cipher_at_offset(byte_offset);

        // Handle non-block-aligned start
        let offset_in_block = (byte_offset % 16) as usize;
        if offset_in_block != 0 {
            // For non-aligned data, we need to process the partial block
            let first_block_remaining = (16 - offset_in_block).min(data.len());

            // Create a full block with padding, encrypt it, then extract the needed portion
            let mut temp_block = [0u8; 16];
            temp_block[offset_in_block..offset_in_block + first_block_remaining]
                .copy_from_slice(&data[..first_block_remaining]);
            cipher.apply_keystream(&mut temp_block);
            data[..first_block_remaining].copy_from_slice(
                &temp_block[offset_in_block..offset_in_block + first_block_remaining],
            );

            // Process remaining data (already block-aligned now)
            if data.len() > first_block_remaining {
                cipher.apply_keystream(&mut data[first_block_remaining..]);
            }
        } else {
            // Block-aligned, can encrypt directly
            cipher.apply_keystream(data);
        }
    }

    /// Encrypt data and return new encrypted bytes
    pub fn encrypt(&self, data: &[u8], byte_offset: u64) -> Bytes {
        let mut encrypted = BytesMut::from(data);
        self.encrypt_at_offset(&mut encrypted, byte_offset);
        encrypted.freeze()
    }
}

/// Encrypted reader that encrypts data on the fly
pub struct EncryptedReader<R> {
    inner: R,
    config: EncryptionConfig,
    position: u64,
}

impl<R> EncryptedReader<R> {
    /// Create a new encrypted reader
    pub fn new(reader: R, config: EncryptionConfig) -> Self {
        Self {
            inner: reader,
            config,
            position: 0,
        }
    }

    /// Create an encrypted reader starting at a specific offset
    pub fn with_offset(reader: R, config: EncryptionConfig, offset: u64) -> Self {
        Self {
            inner: reader,
            config,
            position: offset,
        }
    }
}

impl<R: Read> Read for EncryptedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;
        if bytes_read > 0 {
            self.config
                .encrypt_at_offset(&mut buf[..bytes_read], self.position);
            self.position += bytes_read as u64;
        }
        Ok(bytes_read)
    }
}

impl<R: Seek> Seek for EncryptedReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = self.inner.seek(pos)?;
        self.position = new_pos;
        Ok(new_pos)
    }
}

/// Read a chunk from an async reader and optionally encrypt it
pub async fn read_and_encrypt_chunk<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut [u8],
    encryption: Option<&EncryptionConfig>,
    byte_offset: u64,
) -> std::io::Result<usize> {
    let bytes_read = reader.read(buffer).await?;

    if bytes_read > 0 {
        if let Some(config) = encryption {
            config.encrypt_at_offset(&mut buffer[..bytes_read], byte_offset);
        }
    }

    Ok(bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_counter() {
        let mut counter = [0u8; 16];
        counter[15] = 0xFF;
        EncryptionConfig::increment_counter(&mut counter, 1);
        assert_eq!(counter[14], 1);
        assert_eq!(counter[15], 0);
    }

    #[test]
    fn test_increment_counter_large() {
        let mut counter = [0u8; 16];
        EncryptionConfig::increment_counter(&mut counter, 256);
        assert_eq!(counter[14], 1);
        assert_eq!(counter[15], 0);
    }
}
