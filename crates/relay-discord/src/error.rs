pub type Result<T, E = Error> = std::result::Result<T, E>;

/// None of these variants ever carries the token: wreq errors describe the URL and transport only.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 401 on `/users/@me` (`LoginFailure` in discord.py-self).
    #[error("improper token has been passed")]
    LoginFailed,
    #[error("HTTP {status}: {message}")]
    Status { status: u16, message: String },
    #[error("http: {0}")]
    Http(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
    /// The gateway was closed with a code that must not be retried (`ConnectionClosed`).
    #[error("gateway closed with {code} (reason: {reason:?})")]
    GatewayClosed { code: i32, reason: String },
    /// The gateway task has ended; nothing more can be sent.
    #[error("gateway is shut down")]
    GatewayShutDown,
}

impl From<wreq::Error> for Error {
    fn from(e: wreq::Error) -> Self {
        Error::Http(e.to_string())
    }
}
