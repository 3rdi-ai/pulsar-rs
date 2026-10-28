//! Basic Authenticator

use async_trait::async_trait;

use crate::{authentication::Authentication, error::AuthenticationError};

/// Basic Authentication used for username and password authentication
pub struct BasicAuthentication {
    auth_data: String,
}

/// `auth_data` is `username:password`: it never reaches a formatted value.
impl std::fmt::Debug for BasicAuthentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicAuthentication")
            .field("auth_data", &"<redacted>")
            .finish()
    }
}

impl BasicAuthentication {
    #[must_use]
    pub fn new(username: &str, password: &str) -> Box<Self> {
        Box::new(Self {
            auth_data: format!("{username}:{password}"),
        })
    }
}

#[async_trait]
impl Authentication for BasicAuthentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        String::from("basic")
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        Ok(self.auth_data.clone().into_bytes())
    }
}
