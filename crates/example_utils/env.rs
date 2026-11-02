//! Connection settings read from the environment.

use std::env;

/// `CLOUDREVE_SERVER`, `CLOUDREVE_EMAIL`, `CLOUDREVE_PASSWORD` and an optional
/// `CLOUDREVE_URI` root (default `cloudreve://my/`).
pub struct Settings {
    pub server: String,
    pub email: String,
    pub password: String,
    /// Root URI the examples read and write under.
    #[allow(dead_code)] // the plain upload takes its destination from argv
    pub root: String,
}

impl Settings {
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            server: env::var("CLOUDREVE_SERVER")?,
            email: env::var("CLOUDREVE_EMAIL")?,
            password: env::var("CLOUDREVE_PASSWORD")?,
            root: env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".into()),
        })
    }
}
