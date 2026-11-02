//! Download the uploaded object back and check its SHA-256.

use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use cloudreve_sdk_api::Client;
use cloudreve_sdk_api::api::ExplorerApi;
use cloudreve_sdk_api::models::explorer::FileURLService;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

/// SHA-256 of an in-memory buffer.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// SHA-256 of a file, read in blocks so a large upload is not held in memory.
#[allow(dead_code)] // only the plain upload reads from a path
pub async fn digest_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize().into())
}

/// The download URL is built from Cloudreve's configured `SiteURL`, which is
/// `http://localhost:5212` in the Podman harness while the published port is
/// random. Rewrite only loopback hosts so the signature (path + query) is kept.
fn retarget(raw: &str, server: &str) -> Result<reqwest::Url> {
    let server = reqwest::Url::parse(server)?;
    let mut url = reqwest::Url::parse(raw).or_else(|_| server.join(raw))?;
    if matches!(url.host_str(), Some("localhost") | Some("127.0.0.1")) {
        url.set_scheme(server.scheme())
            .map_err(|_| anyhow!("invalid server scheme"))?;
        url.set_host(server.host_str())
            .map_err(|_| anyhow!("invalid server host"))?;
        url.set_port(server.port())
            .map_err(|_| anyhow!("invalid server port"))?;
    }
    Ok(url)
}

/// Fetch `uri` back and fail unless the stored bytes hash to `expected`.
pub async fn verify(client: &Client, server: &str, uri: &str, expected: [u8; 32]) -> Result<()> {
    let response = client
        .get_file_url(&FileURLService {
            uris: vec![uri.to_string()],
            download: Some(true),
            redirect: Some(false),
            no_cache: Some(true),
            ..Default::default()
        })
        .await?;
    let raw = response
        .urls
        .first()
        .context("no download URL returned")?
        .url
        .clone();
    let url = retarget(&raw, server)?;

    let response = reqwest::Client::new()
        .get(url.clone())
        .send()
        .await
        .context("download request failed")?;
    if !response.status().is_success() {
        bail!("download failed with {} for {url}", response.status());
    }
    let bytes = response.bytes().await?;

    if digest(&bytes) != expected {
        bail!(
            "downloaded content hash mismatch ({} bytes); the upload stored different data",
            bytes.len()
        );
    }
    Ok(())
}
