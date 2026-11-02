use anyhow::Result;
use cloudreve_sdk_api::api::ExplorerApi;
use cloudreve_sdk_api::models::explorer::ListFileService;

#[path = "example_utils/mod.rs"]
mod example_utils;

use example_utils::env::Settings;
use example_utils::login::login;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    let (client, account) = login(&settings, "cloudreve-sdk-api-example").await?;

    println!("Logged in as {}", account.user.nickname);

    let response = client
        .list_files(&ListFileService {
            uri: settings.root.clone(),
            page: None,
            page_size: Some(50),
            order_by: Some("name".to_string()),
            order_direction: Some("asc".to_string()),
            next_page_token: None,
        })
        .await?;

    for file in response.files {
        println!("{}\t{} bytes\t{}", file.name, file.size, file.path);
    }

    Ok(())
}
