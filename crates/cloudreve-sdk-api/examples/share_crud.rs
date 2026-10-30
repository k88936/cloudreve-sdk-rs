//! Live share-link CRUD smoke test. Run through `scripts/e2e_podman.py`.

use cloudreve_sdk_api::api::{ExplorerApi, ShareApi, UserApi};
use cloudreve_sdk_api::models::explorer::CreateFileService;
use cloudreve_sdk_api::client::RequestOptions;
use cloudreve_sdk_api::models::share::{ListShareService, ShareCreateService};
use cloudreve_sdk_api::{Client, ClientConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("CLOUDREVE_SERVER")?;
    let email = std::env::var("CLOUDREVE_EMAIL")?;
    let password = std::env::var("CLOUDREVE_PASSWORD")?;
    let root = std::env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".into());

    let client = Client::new(ClientConfig::new(&server));
    let login = client.login(&email, &password).await?;
    client.set_tokens_with_expiry(&login.token).await?;

    let folder_uri = format!("{}/share-e2e-{}", root.trim_end_matches('/'), std::process::id());
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
        return Err("create_share returned an empty URL".into());
    }
    println!("created share URL: {url}");
    let first_id = share_id_from_url(&url)?;

    let info = client.get_share(&first_id, Some("firstpass"), false, false).await?;
    if !info.password_protected || !info.unlocked {
        return Err(format!("unexpected share info: {info:?}").into());
    }
    if !info.url.contains(&first_id) {
        return Err("share URL does not contain its id".into());
    }

    let list = client
        .list_shares(&ListShareService {
            page_size: 100,
            ..Default::default()
        })
        .await?;
    if !list.shares.iter().any(|share| share.id == first_id) {
        return Err("created share is missing from list_shares".into());
    }
    println!("listed {} shares", list.shares.len());

    let anonymous = Client::new(ClientConfig::new(&server));
    let wrong = anonymous
        .get_share_with_options(
            &first_id,
            Some("wrongpass"),
            false,
            false,
            RequestOptions::new().no_credential(),
        )
        .await?;
    if wrong.unlocked {
        return Err("wrong password unexpectedly unlocked the share".into());
    }
    let unlocked = anonymous
        .get_share_with_options(
            &first_id,
            Some("firstpass"),
            false,
            false,
            RequestOptions::new().no_credential(),
        )
        .await?;
    if !unlocked.unlocked {
        return Err("correct password did not unlock the anonymous share".into());
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
        return Err("updated password did not unlock the share".into());
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
        return Err("deleted shares are still present".into());
    }
    println!("deleted shares; share CRUD passed");
    Ok(())
}

fn share_id_from_url(url: &str) -> Result<String, Box<dyn Error>> {
    let path = url.split('?').next().unwrap_or(url).trim_end_matches('/');
    let segments = path.split('/').collect::<Vec<_>>();
    let index = segments
        .iter()
        .position(|segment| *segment == "s")
        .ok_or("share URL has no /s/ segment")?;
    segments
        .get(index + 1)
        .filter(|id| !id.is_empty())
        .map(|id| (*id).to_owned())
        .ok_or_else(|| "could not extract share id from URL".into())
}
