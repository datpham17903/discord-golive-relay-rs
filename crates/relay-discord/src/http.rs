//! REST client, port of the request-header construction in `discord/http.py::HTTPClient.request`
//! and `static_login`.
//!
//! Retries port `request()`'s loop: up to 5 attempts, 429 waits `retry_after`, 5xx backs off `1 + 2 * try` s.
//! Not ported (not needed for login): ratelimit buckets, captcha,
//! `X-Discord-Timezone` (needs a local tz lookup), `X-Installation-ID`.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use wreq::header::{AUTHORIZATION, HeaderValue};

use crate::error::{Error, Result};
use crate::props::{HeadersContext, Urls};
use crate::token::Token;

/// The `/users/@me` fields the relay uses.
#[derive(Debug, Clone, Deserialize)]
pub struct User {
    #[serde(deserialize_with = "snowflake")]
    pub id: u64,
    pub username: Option<String>,
}

fn snowflake<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Str(String),
        Num(u64),
    }
    match Raw::deserialize(d)? {
        Raw::Num(n) => Ok(n),
        Raw::Str(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

/// `ContextProperties.from_chat_input()`: base64 of `{"location":"Chat Input"}`.
const CHAT_INPUT_CONTEXT: &str = "eyJsb2NhdGlvbiI6IkNoYXQgSW5wdXQifQ==";

/// `utils._generate_nonce`: a snowflake made from the current time.
fn nonce() -> String {
    const DISCORD_EPOCH_MS: u128 = 1_420_070_400_000;
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    (ms.saturating_sub(DISCORD_EPOCH_MS) << 22).to_string()
}

#[derive(Clone)]
pub struct RestClient {
    http: wreq::Client,
    ctx: Arc<HeadersContext>,
    urls: Urls,
    token: Token,
}

impl fmt::Debug for RestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestClient")
            .field("urls", &self.urls)
            .field("ctx", &self.ctx)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl RestClient {
    /// Fetches the header context (info API, else scrape, else constants) and builds the
    /// browser-emulating client. A plain client does the bootstrap requests, like the Python
    /// library's `aiohttp` session.
    pub async fn new(urls: Urls, token: Token) -> Result<Self> {
        let bootstrap = wreq::Client::builder().build()?;
        let ctx = HeadersContext::fetch_default(&bootstrap, &urls).await;
        Self::with_context(urls, token, Arc::new(ctx))
    }

    pub fn with_context(urls: Urls, token: Token, ctx: Arc<HeadersContext>) -> Result<Self> {
        let http = wreq::Client::builder().emulation(ctx.emulation()).build()?;
        Ok(Self {
            http,
            ctx,
            urls,
            token,
        })
    }

    /// The emulating client, shared with the gateway so both present the same TLS/HTTP fingerprint.
    pub fn http_client(&self) -> &wreq::Client {
        &self.http
    }

    pub fn headers_context(&self) -> &Arc<HeadersContext> {
        &self.ctx
    }

    pub fn urls(&self) -> &Urls {
        &self.urls
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    /// `static_login`: `GET /users/@me?with_analytics_token=true`; 401 is `LoginFailed`.
    pub async fn login(&self) -> Result<User> {
        let resp = self
            .send_retrying(|| {
                self.request(wreq::Method::GET, "/users/@me?with_analytics_token=true")
            })
            .await?;
        let status = resp.status();
        if status.as_u16() == 401 {
            return Err(Error::LoginFailed);
        }
        if !status.is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let message = body
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_owned();
            return Err(Error::Status {
                status: status.as_u16(),
                message,
            });
        }
        resp.json()
            .await
            .map_err(|e| Error::InvalidData(format!("users/@me: {e}")))
    }

    /// Replies to `message_id` in `channel_id` without pinging anyone: no author mention and no parsed
    /// mentions in `text` (`PartialMessage.reply(mention_author=False, allowed_mentions=none())`).
    pub async fn reply(
        &self,
        guild_id: Option<u64>,
        channel_id: u64,
        message_id: u64,
        text: &str,
    ) -> Result<()> {
        let mut reference = serde_json::json!({
            "message_id": message_id.to_string(),
            "channel_id": channel_id.to_string(),
            "fail_if_not_exists": true,
        });
        if let Some(guild_id) = guild_id {
            reference["guild_id"] = guild_id.to_string().into();
        }
        let body = serde_json::json!({
            "tts": false,
            "content": text,
            "nonce": nonce(),
            "message_reference": reference,
            "allowed_mentions": {"parse": [], "replied_user": false},
        });
        let resp = self
            .send_retrying(|| {
                Ok(self
                    .request(
                        wreq::Method::POST,
                        &format!("/channels/{channel_id}/messages"),
                    )?
                    .header("X-Context-Properties", CHAT_INPUT_CONTEXT)
                    .json(&body))
            })
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        Err(Error::Status {
            status: status.as_u16(),
            message: body
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_owned(),
        })
    }

    /// Sends the request built by `build`, retrying like the Python library: a 429 sleeps `retry_after`
    /// (JSON body, else `Retry-After`), 500/502/503/504 sleep `1 + 2 * attempt` s; at most 5 attempts, after
    /// which the last response is returned for the caller to report.
    async fn send_retrying(
        &self,
        build: impl Fn() -> Result<wreq::RequestBuilder>,
    ) -> Result<wreq::Response> {
        const ATTEMPTS: u32 = 5;
        let mut attempt = 0;
        loop {
            let resp = build()?.send().await?;
            let status = resp.status().as_u16();
            attempt += 1;
            if attempt >= ATTEMPTS || !matches!(status, 429 | 500 | 502 | 503 | 504) {
                return Ok(resp);
            }
            let delay = if status == 429 {
                let header = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<f64>().ok());
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                body.get("retry_after")
                    .and_then(serde_json::Value::as_f64)
                    .or(header)
                    .filter(|s| *s > 0.0)
                    .and_then(|s| Duration::try_from_secs_f64(s).ok())
                    .unwrap_or(Duration::from_secs(1))
            } else {
                Duration::from_secs(1 + 2 * u64::from(attempt - 1))
            };
            tokio::time::sleep(delay).await;
        }
    }

    fn request(&self, method: wreq::Method, path: &str) -> Result<wreq::RequestBuilder> {
        let url = format!("{}{path}", self.urls.api_base);
        let mut auth = HeaderValue::from_str(self.token.expose())
            .map_err(|_| Error::InvalidData("token is not a valid header value".into()))?;
        auth.set_sensitive(true);

        let mut req = self.http.request(method, url);
        for (k, v) in self.ctx.client_hints() {
            req = req.header(k, v);
        }
        Ok(req
            .header("Accept", "*/*")
            .header("Accept-Language", "en-US,en;q=0.9")
            .header("Origin", &self.urls.origin)
            .header("Referer", format!("{}/channels/@me", self.urls.origin))
            .header("Sec-Fetch-Dest", "empty")
            .header("Sec-Fetch-Mode", "cors")
            .header("Sec-Fetch-Site", "same-origin")
            .header("User-Agent", self.ctx.user_agent())
            .header("X-Discord-Locale", "en-US")
            .header("X-Super-Properties", &self.ctx.encoded_super_properties)
            .header(AUTHORIZATION, auth))
    }
}
