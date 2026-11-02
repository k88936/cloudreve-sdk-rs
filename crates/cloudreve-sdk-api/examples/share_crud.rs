//! Live share-link CRUD smoke test. Run through `scripts/e2e_podman.py`.

use anyhow::{anyhow, bail, Result};
use cloudreve_sdk_api::api::{ExplorerApi, ShareApi};
use cloudreve_sdk_api::models::explorer::CreateFileService;
use cloudreve_sdk_api::client::RequestOptions;
use cloudreve_sdk_api::models::share::{ListShareService, ShareCreateService};

#[path = "example_utils/mod.rs"]
mod example_utils;

use example_utils::env::Settings;
use example_utils::login::{anonymous, login};

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    let (client, _) = login(&settings, "cloudreve-sdk-api-example").await?;

    let folder_uri = format!(
        "{}/share-e2e-{}",
        settings.root.trim_end_matches('/'),
        std::process::id()
    );
    let folder = client
        .create_file(&CreateFileService {
            uri: folder_uri.clone(),
            file_type: "folder".into(),
            metadata: None,
            err_on_conflict: Some(true),
        })
        .await?;
    println!("folder: {}", folder.path);

    let mut create = ShareCreateService::new(&folder_uri);
    create.is_private = true;
    create.password = "firstpass".into();
    create.expire = 3600;
    create.share_view = true;
    let url = client.create_share(&create).await?;
    if url.is_empty() {
        bail!("create_share returned an empty URL");
    }
    println!("created share URL: {url}");
    let first_id = share_id_from_url(&url)?;

    let info = client.get_share(&first_id, Some("firstpass"), false, false).await?;
    if !info.password_protected || !info.unlocked {
        bail!("unexpected share info: {info:?}");
    }
    if !info.url.contains(&first_id) {
        bail!("share URL does not contain its id");
    }

    let list = client
        .list_shares(&ListShareService {
            page_size: 100,
            ..Default::default()
        })
        .await?;
    if !list.shares.iter().any(|share| share.id == first_id) {
        bail!("created share is missing from list_shares");
    }
    println!("listed {} shares", list.shares.len());

    let guest = anonymous(&settings);
    let wrong = guest
        .get_share_with_options(
            &first_id,
            Some("wrongpass"),
            false,
            false,
            RequestOptions::new().no_credential(),
        )
        .await?;
    if wrong.unlocked {
        bail!("wrong password unexpectedly unlocked the share");
    }
    let unlocked = guest
        .get_share_with_options(
            &first_id,
            Some("firstpass"),
            false,
            false,
            RequestOptions::new().no_credential(),
        )
        .await?;
    if !unlocked.unlocked {
        bail!("correct password did not unlock the anonymous share");
    }

    let mut update = ShareCreateService::new(&folder_uri);
    update.is_private = true;
    update.password = "secondpass".into();
    update.expire = 7200;
    client.update_share(&first_id, &update).await?;
    let updated = client
        .get_share(&first_id, Some("secondpass"), false, false)
        .await?;
    if !updated.unlocked {
        bail!("updated password did not unlock the share");
    }
    println!("updated share password/expiry");

    let second_url = client.create_share(&ShareCreateService::new(&folder_uri)).await?;
    let second_id = share_id_from_url(&second_url)?;

    client.delete_share(&first_id).await?;
    client.delete_shares(std::slice::from_ref(&second_id)).await?;
    let list = client
        .list_shares(&ListShareService {
            page_size: 100,
            ..Default::default()
        })
        .await?;
    if list
        .shares
        .iter()
        .any(|share| share.id == first_id || share.id == second_id)
    {
        bail!("deleted shares are still present");
    }
    println!("deleted shares; share CRUD passed");
    Ok(())
}

fn share_id_from_url(url: &str) -> Result<String> {
    let path = url.split('?').next().unwrap_or(url).trim_end_matches('/');
    let segments = path.split('/').collect::<Vec<_>>();
    let index = segments
        .iter()
        .position(|segment| *segment == "s")
        .ok_or_else(|| anyhow!("share URL has no /s/ segment"))?;
    segments
        .get(index + 1)
        .filter(|id| !id.is_empty())
        .map(|id| (*id).to_owned())
        .ok_or_else(|| anyhow!("could not extract share id from URL"))
}
