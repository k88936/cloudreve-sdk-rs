//! Clients for the examples.

use anyhow::Result;
use cloudreve_sdk_api::api::UserApi;
use cloudreve_sdk_api::models::user::LoginResponse;
use cloudreve_sdk_api::{Client, ClientConfig};

use super::env::Settings;

/// Log in with the credentials from [`Settings`].
///
/// Returns the client with its tokens installed plus the login response, whose
/// user profile the list example prints.
pub async fn login(settings: &Settings, user_agent: &str) -> Result<(Client, LoginResponse)> {
    let client = Client::new(ClientConfig::new(&settings.server).with_user_agent(user_agent));
    let login = client.login(&settings.email, &settings.password).await?;
    client.set_tokens_with_expiry(&login.token).await?;
    Ok((client, login))
}

/// A second client without credentials, for requests that must stay anonymous.
#[allow(dead_code)] // only the share example needs one
pub fn anonymous(settings: &Settings) -> Client {
    Client::new(ClientConfig::new(&settings.server))
}
