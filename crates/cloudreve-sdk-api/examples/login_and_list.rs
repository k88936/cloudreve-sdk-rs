use cloudreve_sdk_api::api::{ExplorerApi, UserApi};
use cloudreve_sdk_api::models::explorer::ListFileService;
use cloudreve_sdk_api::{Client, ClientConfig};
use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = env::var("CLOUDREVE_SERVER")?;
    let email = env::var("CLOUDREVE_EMAIL")?;
    let password = env::var("CLOUDREVE_PASSWORD")?;
    let remote_uri =
        env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".to_string());

    let client = Client::new(
        ClientConfig::new(server)
            .with_timeout(60)
            .with_user_agent("cloudreve-sdk-api-example"),
    );

    let login = client.login(&email, &password).await?;
    client.set_tokens_with_expiry(&login.token).await?;

    println!("Logged in as {}", login.user.nickname);

    let response = client
        .list_files(&ListFileService {
            uri: remote_uri,
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
