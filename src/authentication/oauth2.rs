//! OAuth2 Authenticator

use std::{
    fmt::{Display, Formatter},
    fs,
    time::Instant,
};

use async_trait::async_trait;
use data_url::DataUrl;
use nom::lib::std::ops::Add;
use oauth2::{
    basic::{BasicClient, BasicErrorResponse, BasicErrorResponseType, BasicTokenResponse},
    reqwest,
    AuthType::RequestBody,
    AuthUrl, ClientId, ClientSecret, HttpClientError, RequestTokenError, Scope, TokenResponse,
    TokenUrl,
};
use openidconnect::{core::CoreProviderMetadata, IssuerUrl};
use serde::Deserialize;
use url::Url;

use crate::{authentication::Authentication, error::AuthenticationError};

/// What a credential reads as in any formatted output.
const REDACTED: &str = "<redacted>";

#[derive(Deserialize, Clone)]
struct OAuth2PrivateParams {
    client_id: String,
    client_secret: String,
    #[allow(dead_code)]
    client_email: Option<String>,
    issuer_url: Option<String>,
}

/// Names the client, never its secret.
impl std::fmt::Debug for OAuth2PrivateParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2PrivateParams")
            .field("client_id", &self.client_id)
            .field("client_secret", &REDACTED)
            .field("client_email", &self.client_email)
            .field("issuer_url", &self.issuer_url)
            .finish()
    }
}

#[derive(Deserialize, Clone)]
pub struct OAuth2Params {
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: Option<String>,
    pub scope: Option<String>,
}

/// The credentials URL shows as its scheme only (a `file:` URL keeps its
/// path): a `data:` URL embeds the client secret.
impl std::fmt::Debug for OAuth2Params {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2Params")
            .field("issuer_url", &self.issuer_url)
            .field("credentials_url", &self.credentials_hint())
            .field("audience", &self.audience)
            .field("scope", &self.scope)
            .finish()
    }
}

impl Display for OAuth2Params {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OAuth2Params({}, {}, {:?}, {:?})",
            self.issuer_url,
            self.credentials_hint(),
            self.audience,
            self.scope
        )
    }
}

pub struct CachedToken {
    token_secret: Vec<u8>,
    expiring_at: Option<Instant>,
    expired_at: Option<Instant>,
}

impl From<BasicTokenResponse> for CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(resp: BasicTokenResponse) -> Self {
        let now = Instant::now();
        CachedToken {
            expiring_at: resp.expires_in().map(|d| now.add(d.mul_f32(0.9))),
            expired_at: resp.expires_in().map(|d| now.add(d)),
            token_secret: resp.access_token().secret().clone().into_bytes(),
        }
    }
}

impl CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expiring(&self) -> bool {
        match &self.expiring_at {
            Some(expiring_at) => Instant::now().ge(expiring_at),
            None => false,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expired(&self) -> bool {
        match &self.expired_at {
            Some(expired_at) => Instant::now().ge(expired_at),
            None => false,
        }
    }
}

pub struct OAuth2Authentication {
    params: OAuth2Params,
    private_params: Option<OAuth2PrivateParams>,
    token_url: Option<TokenUrl>,
    token: Option<CachedToken>,
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn client_credentials(params: OAuth2Params) -> Box<dyn Authentication> {
        Box::new(OAuth2Authentication {
            params,
            private_params: None,
            token_url: None,
            token: None,
        })
    }
}

impl OAuth2Params {
    /// The credentials URL with everything but a `file:` path blanked.
    fn credentials_hint(&self) -> String {
        match Url::parse(self.credentials_url.as_str()) {
            Ok(url) => match url.scheme() {
                "file" => format!("file:{}", url.path()),
                scheme => format!("{scheme}:{REDACTED}"),
            },
            Err(_) => String::from("<invalid credentials_url>"),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn read_private_params(&self) -> Result<OAuth2PrivateParams, Box<dyn std::error::Error>> {
        let credentials_url = Url::parse(self.credentials_url.as_str())?;
        match credentials_url.scheme() {
            "file" => {
                let path = credentials_url.path();
                Ok(serde_json::from_str(fs::read_to_string(path)?.as_str()).map_err(json_error)?)
            }
            "data" => {
                let data_url = match DataUrl::process(self.credentials_url.as_str()) {
                    Ok(data_url) => data_url,
                    Err(err) => {
                        return Err(Box::from(format!("invalid data url: {err:?}")));
                    }
                };
                let body = match data_url.decode_to_vec() {
                    Ok((body, _)) => body,
                    Err(_) => {
                        return Err(Box::from(
                            "invalid data url: malformed base64 body".to_string(),
                        ));
                    }
                };

                Ok(serde_json::from_slice(&body).map_err(json_error)?)
            }
            _ => Err(Box::from(format!(
                "unsupported credentials url scheme [{}]",
                credentials_url.scheme()
            ))),
        }
    }
}

/// A credentials JSON error by kind and position only: serde messages can
/// quote the offending value.
fn json_error(e: serde_json::Error) -> Box<dyn std::error::Error> {
    Box::from(format!(
        "malformed credentials JSON ({:?} error at line {}, column {})",
        e.classify(),
        e.line(),
        e.column()
    ))
}

/// The token endpoint's error type for a [`BasicClient`] over reqwest.
type TokenError = RequestTokenError<HttpClientError<reqwest::Error>, BasicErrorResponse>;

/// A token-endpoint failure by kind only. The server picks the `error` code,
/// `error_description`, body and Content-Type, and reqwest's `Display`
/// carries the token URL (a query can hold a secret): none is echoed.
fn token_error_kind(e: &TokenError) -> String {
    match e {
        RequestTokenError::ServerResponse(r) => {
            format!("server rejected the request: {}", error_code(r.error()))
        }
        RequestTokenError::Request(e) => format!("request failed: {}", transport_kind(e)),
        RequestTokenError::Parse(_, body) => {
            format!("unparseable token response ({} bytes)", body.len())
        }
        RequestTokenError::Other(detail) => other_kind(detail),
    }
}

/// RFC 6749 §5.2 codes by name; an extension code by length only.
fn error_code(code: &BasicErrorResponseType) -> String {
    match code {
        BasicErrorResponseType::InvalidClient => "invalid_client".into(),
        BasicErrorResponseType::InvalidGrant => "invalid_grant".into(),
        BasicErrorResponseType::InvalidRequest => "invalid_request".into(),
        BasicErrorResponseType::InvalidScope => "invalid_scope".into(),
        BasicErrorResponseType::UnauthorizedClient => "unauthorized_client".into(),
        BasicErrorResponseType::UnsupportedGrantType => "unsupported_grant_type".into(),
        BasicErrorResponseType::Extension(s) => format!("extension({} bytes)", s.len()),
    }
}

/// What the oauth2 crate's `Other` detail reports: its fixed messages by name,
/// a Content-Type only when it is a plain media type.
fn other_kind(detail: &str) -> String {
    const CONTENT_TYPE: &str = "unexpected response Content-Type: ";
    if let Some(rest) = detail.strip_prefix(CONTENT_TYPE) {
        let media = rest.split('"').nth(1).unwrap_or_default();
        let plain = !media.is_empty()
            && media.len() <= 64
            && media
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b"/+.-".contains(&b));
        return if plain {
            format!("unexpected response content-type {media}")
        } else {
            format!("unexpected response content-type ({} bytes)", media.len())
        };
    }
    for fixed in [
        "server returned empty error response",
        "server returned empty response body",
    ] {
        if detail == fixed {
            return format!("unexpected token response: {fixed}");
        }
    }
    format!(
        "unexpected token response ({} bytes of detail)",
        detail.len()
    )
}

/// The transport failure's kind, from reqwest's classifiers and the `source()`
/// chain, never its `Display` (which carries the URL).
fn transport_kind(e: &HttpClientError<reqwest::Error>) -> String {
    let e = match e {
        HttpClientError::Reqwest(e) => e,
        HttpClientError::Http(_) => return "http".into(),
        HttpClientError::Io(e) => return format!("io {:?}", e.kind()),
        HttpClientError::Other(detail) => return format!("other ({} bytes)", detail.len()),
        _ => return "unknown".into(),
    };
    if let Some(status) = e.status() {
        return format!("status {}", status.as_u16());
    }
    // the causes' text is only matched here, never printed
    let mut chain = Vec::new();
    let mut io_kind = None;
    let mut source = std::error::Error::source(e.as_ref());
    while let Some(s) = source {
        chain.push(s.to_string().to_ascii_lowercase());
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            io_kind = Some(io.kind());
        }
        source = s.source();
    }
    let base = if e.is_timeout() {
        "timeout"
    } else if chain.iter().any(|s| s.contains("dns error")) {
        "dns"
    } else if chain
        .iter()
        .any(|s| s.contains("tls") || s.contains("certificate"))
    {
        "tls"
    } else if e.is_connect() {
        "connect"
    } else if e.is_redirect() {
        "redirect"
    } else if e.is_body() || e.is_decode() {
        "body"
    } else if e.is_builder() {
        "builder"
    } else {
        "request"
    };
    match io_kind {
        Some(kind) => format!("{base} ({kind:?})"),
        None => base.to_string(),
    }
}

#[async_trait]
impl Authentication for OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        String::from("token")
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        match self.params.read_private_params() {
            Ok(private_params) => self.private_params = Some(private_params),
            Err(e) => {
                return Err(AuthenticationError::Custom(format!(
                    "failed to read OAuth2 credentials from {}: {e}",
                    self.params.credentials_hint()
                )));
            }
        }
        if let Err(e) = self.token_url().await {
            return Err(AuthenticationError::Custom(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            )));
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        if self.private_params.is_none() {
            return Err(AuthenticationError::Custom("not initialized".to_string()));
        }
        let mut need_token = false;
        let mut none_or_expired = true;
        if let Some(token) = self.token.as_ref() {
            none_or_expired = token.is_expired();
            if none_or_expired || token.is_expiring() {
                need_token = true;
            }
        } else {
            need_token = true;
        }
        if need_token {
            match self.fetch_token().await {
                Ok(token) => {
                    self.token = Some(token.into());
                }
                Err(e) => {
                    if none_or_expired {
                        // invalidate the expired token
                        self.token = None;
                        if let Some(auth_err) = e.downcast_ref::<AuthenticationError>() {
                            return Err(auth_err.clone());
                        }
                        return Err(AuthenticationError::Custom(format!(
                            "failed to fetch OAuth2 access token for issuer [{}] (audience={:?}, scope={:?}): {e}",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        )));
                    } else {
                        warn!(
                            "failed to refresh OAuth2 token for issuer [{}] (audience={:?}, scope={:?}): {e}. Using existing non-expired token",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        );
                    }
                }
            }
        }
        Ok(self.token.as_ref().unwrap().token_secret.clone())
    }
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn token_url(&mut self) -> Result<TokenUrl, Box<dyn std::error::Error>> {
        match &self.token_url {
            Some(url) => Ok(url.clone()),
            None => {
                let client = reqwest::Client::new();
                let metadata = CoreProviderMetadata::discover_async(
                    IssuerUrl::from_url(Url::parse(self.params.issuer_url.as_str())?),
                    &client,
                )
                .await?;
                if let Some(token_endpoint) = metadata.token_endpoint() {
                    self.token_url = Some(token_endpoint.clone());
                    Ok(token_endpoint.clone())
                } else {
                    Err(Box::from("token url not exists"))
                }
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn fetch_token(&mut self) -> Result<BasicTokenResponse, Box<dyn std::error::Error>> {
        let token_url = self.token_url().await.map_err(|e| {
            AuthenticationError::Retriable(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            ))
        })?;

        let private_params = self
            .private_params
            .as_ref()
            .expect("oauth2 provider is uninitialized");

        let issuer_url = if let Some(url) = private_params.issuer_url.as_ref() {
            url.as_str()
        } else {
            self.params.issuer_url.as_str()
        };

        let client = BasicClient::new(ClientId::new(private_params.client_id.clone()))
            .set_client_secret(ClientSecret::new(private_params.client_secret.clone()))
            .set_auth_uri(AuthUrl::from_url(Url::parse(issuer_url)?))
            .set_token_uri(token_url)
            .set_auth_type(RequestBody);

        let mut request = client.exchange_client_credentials();

        if let Some(audience) = &self.params.audience {
            request = request.add_extra_param("audience", audience.clone());
        }

        if let Some(scope) = &self.params.scope {
            request = request.add_scope(Scope::new(scope.clone()));
        }

        let client = reqwest::Client::new();
        let token = request.request_async(&client).await.map_err(|e| {
            let kind = token_error_kind(&e);
            let error_message = format!(
                "token endpoint request failed for issuer [{}] (audience={:?}, scope={:?}): {kind}",
                self.params.issuer_url, self.params.audience, self.params.scope
            );
            if let RequestTokenError::ServerResponse(_) = e {
                AuthenticationError::Custom(error_message)
            } else {
                AuthenticationError::Retriable(error_message)
            }
        })?;
        debug!("Got a new oauth2 token for [{}]", self.params);
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use oauth2::TokenUrl;

    use crate::{
        authentication::{
            oauth2::{OAuth2Authentication, OAuth2Params, OAuth2PrivateParams},
            Authentication,
        },
        error::{AuthenticationError, ConnectionError},
    };

    /// A secret no formatted value may ever contain.
    const SENTINEL: &str = "S3CR3T-SENTINEL";

    /// `data:` URL whose base64 JSON embeds [`SENTINEL`] as the client secret,
    /// with the base64 payload returned so a test can assert it is absent too.
    fn sentinel_data_url() -> (String, String) {
        use base64::Engine;
        let json = format!(r#"{{"client_id":"cid","client_secret":"{SENTINEL}"}}"#);
        let b64 = base64::engine::general_purpose::STANDARD.encode(json);
        (format!("data:application/json;base64,{b64}"), b64)
    }

    fn assert_no_secret(rendered: &str, b64: &str) {
        assert!(!rendered.contains(SENTINEL), "secret leaked: {rendered}");
        assert!(!rendered.contains(b64), "base64 secret leaked: {rendered}");
        // a `Vec<u8>` body renders as its byte list under `{:?}`
        let bytes = format!("{:?}", SENTINEL.as_bytes());
        let bytes = bytes.trim_start_matches('[').trim_end_matches(']');
        assert!(!rendered.contains(bytes), "secret bytes leaked: {rendered}");
    }

    #[test]
    fn params_formatting_carries_no_credentials() {
        let (credentials_url, b64) = sentinel_data_url();
        let params = OAuth2Params {
            issuer_url: "https://issuer.example".to_string(),
            credentials_url,
            audience: Some("aud".to_string()),
            scope: None,
        };
        for rendered in [
            format!("{params:?}"),
            format!("{params:#?}"),
            format!("{params}"),
        ] {
            assert_no_secret(&rendered, &b64);
            assert!(rendered.contains("https://issuer.example"), "{rendered}");
        }
    }

    #[test]
    fn private_params_debug_carries_no_secret() {
        let p = OAuth2PrivateParams {
            client_id: "cid".to_string(),
            client_secret: SENTINEL.to_string(),
            client_email: None,
            issuer_url: None,
        };
        for rendered in [format!("{p:?}"), format!("{p:#?}")] {
            assert_no_secret(&rendered, "unused-b64-marker");
            assert!(rendered.contains("cid"));
        }
    }

    #[test]
    fn basic_authentication_debug_carries_no_password() {
        use crate::authentication::basic::BasicAuthentication;
        let auth = BasicAuthentication::new("user", SENTINEL);
        for rendered in [format!("{auth:?}"), format!("{auth:#?}")] {
            assert_no_secret(&rendered, "unused-b64-marker");
        }
    }

    #[test]
    fn bad_credentials_url_errors_carry_no_content() {
        use base64::Engine;
        let urls = [
            format!("data:{SENTINEL}"),
            format!("data:;base64,{SENTINEL}!!!"),
            "data:application/json;base64,".to_string()
                + &base64::engine::general_purpose::STANDARD.encode(format!("not json {SENTINEL}")),
            format!("ftp://{SENTINEL}@host/creds"),
        ];
        for url in urls {
            let params = OAuth2Params {
                issuer_url: "https://issuer.example".to_string(),
                credentials_url: url.clone(),
                audience: None,
                scope: None,
            };
            let result = params.read_private_params();
            let err = result.expect_err("bad credentials url must fail");
            for rendered in [format!("{err}"), format!("{err:?}")] {
                assert_no_secret(&rendered, "unused-b64-marker");
            }
        }
    }

    #[tokio::test]
    async fn initialize_error_carries_no_credentials() {
        let urls = [
            format!("data:{SENTINEL}"),
            format!("data:;base64,{SENTINEL}!!!"),
        ];
        for url in urls {
            let params = OAuth2Params {
                issuer_url: "https://issuer.example".to_string(),
                credentials_url: url.clone(),
                audience: None,
                scope: None,
            };
            let mut auth = OAuth2Authentication::client_credentials(params);
            let err = auth.initialize().await.expect_err("must fail");
            for rendered in [format!("{err}"), format!("{err:?}")] {
                assert_no_secret(&rendered, "unused-b64-marker");
            }
            let conn_err = ConnectionError::from(err.clone());
            for rendered in [format!("{conn_err}"), format!("{conn_err:?}")] {
                assert_no_secret(&rendered, "unused-b64-marker");
            }
        }
    }

    async fn one_shot_http(
        status_line: &'static str,
        content_type: &'static str,
        body: String,
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let sep_pos = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
            let headers = String::from_utf8_lossy(&buf[..sep_pos]);
            let mut content_length = 0usize;
            for line in headers.lines() {
                if let Some(rest) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = rest.trim().parse().unwrap_or(0);
                }
            }
            let body_start = sep_pos + 4;
            while buf.len() - body_start < content_length {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status_line,
                content_type,
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        format!("http://127.0.0.1:{port}")
    }

    fn token_auth(token_url: String) -> OAuth2Authentication {
        let params = OAuth2Params {
            issuer_url: "http://127.0.0.1:1".to_string(),
            credentials_url: "data:,".to_string(),
            audience: None,
            scope: None,
        };
        let private_params = OAuth2PrivateParams {
            client_id: "cid".to_string(),
            client_secret: "other".to_string(),
            client_email: None,
            issuer_url: None,
        };
        OAuth2Authentication {
            params,
            private_params: Some(private_params),
            token_url: Some(TokenUrl::new(token_url).unwrap()),
            token: None,
        }
    }

    async fn assert_token_error(token_url: String, expected_kind: &str) {
        let mut auth = token_auth(token_url);
        let err = auth
            .auth_data()
            .await
            .expect_err("token endpoint failure must surface");
        for rendered in [format!("{err}"), format!("{err:?}")] {
            assert_no_secret(&rendered, "unused-b64-marker");
            assert!(
                rendered.contains(expected_kind),
                "expected kind {expected_kind:?} in: {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn token_endpoint_unparseable_body_names_the_kind() {
        let base = one_shot_http(
            "200 OK",
            "application/json",
            format!(r#"{{"access_token":"{SENTINEL}""#),
        )
        .await;
        assert_token_error(format!("{base}/token"), "unparseable token response").await;
    }

    #[tokio::test]
    async fn token_endpoint_error_description_is_not_echoed() {
        let base = one_shot_http(
            "400 Bad Request",
            "application/json",
            format!(r#"{{"error":"invalid_client","error_description":"{SENTINEL}"}}"#),
        )
        .await;
        assert_token_error(
            format!("{base}/token"),
            "server rejected the request: invalid_client",
        )
        .await;
    }

    #[tokio::test]
    async fn token_endpoint_extension_error_code_is_not_echoed() {
        let base = one_shot_http(
            "400 Bad Request",
            "application/json",
            format!(r#"{{"error":"{SENTINEL}"}}"#),
        )
        .await;
        assert_token_error(
            format!("{base}/token"),
            &format!("extension({} bytes)", SENTINEL.len()),
        )
        .await;
    }

    #[tokio::test]
    async fn token_endpoint_unexpected_content_type_is_not_echoed() {
        let base = one_shot_http(
            "200 OK",
            "text/S3CR3T-SENTINEL",
            r#"{"access_token":"x","token_type":"bearer"}"#.to_string(),
        )
        .await;
        assert_token_error(format!("{base}/token"), "unexpected response content-type").await;
    }

    #[tokio::test]
    async fn token_endpoint_refused_connection_names_the_kind_not_the_url() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        assert_token_error(
            format!("http://127.0.0.1:{port}/token?secret={SENTINEL}"),
            "request failed: connect",
        )
        .await;
    }

    #[tokio::test]
    async fn token_endpoint_parse_failure_does_not_echo_the_url() {
        let base = one_shot_http(
            "200 OK",
            "application/json",
            format!(r#"{{"access_token":"{SENTINEL}""#),
        )
        .await;
        assert_token_error(
            format!("{base}/token?secret={SENTINEL}"),
            "unparseable token response",
        )
        .await;
    }

    #[test]
    fn parse_data_url() {
        let params = OAuth2Params {
            issuer_url: "".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJjbGllbnQtaWQiLCJjbGllbnRfc2VjcmV0IjoiY2xpZW50LXNlY3JldCJ9Cg==".to_string(),
            audience: None,
            scope: None,
        };
        let private_params = params.read_private_params().unwrap();
        assert_eq!(private_params.client_id, "client-id");
        assert_eq!(private_params.client_secret, "client-secret");
        assert_eq!(private_params.client_email, None);
        assert_eq!(private_params.issuer_url, None);
    }

    #[tokio::test]
    async fn auth_data_returns_retriable_error() {
        let params = OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJpZCIsImNsaWVudF9zZWNyZXQiOiJzZWNyZXQifQ==".to_string(),
            audience: None,
            scope: None,
        };

        let private_params = OAuth2PrivateParams {
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            client_email: None,
            issuer_url: None,
        };

        let assert_retriable_error = |err| match err {
            AuthenticationError::Retriable(_) => {
                let err: ConnectionError = err.into();
                assert!(err.establish_retryable());
            }
            other => panic!("expected retriable error, got {other:?}"),
        };

        let mut auth = OAuth2Authentication {
            params: params.clone(),
            private_params: Some(private_params.clone()),
            token_url: None, // token_url is not set to trigger discovery
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());

        let token_url = TokenUrl::new("http://127.0.0.1:1/token".to_string()).unwrap();

        let mut auth = OAuth2Authentication {
            params,
            private_params: Some(private_params),
            token_url: Some(token_url),
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());
    }
}
