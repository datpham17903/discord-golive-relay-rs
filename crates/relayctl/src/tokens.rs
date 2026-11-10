//! Token loading, port of `cli.py::load_token` / `_read_token`. The token never comes from argv or YAML and never
//! appears in an error message.

use std::path::Path;

pub const REDACTED: &str = "***";

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error(
        "no credential directory set for {0}: set RELAY_TOKEN_DIR or use systemd LoadCredential"
    )]
    NoDirectory(String),
    #[error("cannot read credential {credential}: {source}")]
    Unreadable {
        credential: String,
        source: std::io::Error,
    },
    #[error("credential {0} is empty")]
    Empty(String),
}

/// Token from `$RELAY_TOKEN_DIR/<credential>` (development) or `$CREDENTIALS_DIRECTORY/<credential>`
/// (systemd), first non-empty variable wins.
pub fn load_token(credential: &str) -> Result<String, TokenError> {
    load_token_with(credential, |var| std::env::var(var).ok())
}

/// [`load_token`] with the environment supplied by the caller.
pub fn load_token_with(
    credential: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<String, TokenError> {
    for var in ["RELAY_TOKEN_DIR", "CREDENTIALS_DIRECTORY"] {
        let Some(directory) = env(var).filter(|d| !d.is_empty()) else {
            continue;
        };
        let text =
            std::fs::read_to_string(Path::new(&directory).join(credential)).map_err(|source| {
                TokenError::Unreadable {
                    credential: credential.to_owned(),
                    source,
                }
            })?;
        let token = text.trim();
        if token.is_empty() {
            return Err(TokenError::Empty(credential.to_owned()));
        }
        return Ok(token.to_owned());
    }
    Err(TokenError::NoDirectory(credential.to_owned()))
}

/// Ingest token (`_read_token`): `RELAY_TOKEN_FILE` keeps working, else [`load_token_with`].
/// `None` when it cannot be read or is empty.
pub fn read_token_with(credential: &str, env: impl Fn(&str) -> Option<String>) -> Option<String> {
    match env("RELAY_TOKEN_FILE").filter(|p| !p.is_empty()) {
        Some(path) => std::fs::read_to_string(path)
            .ok()
            .map(|t| t.trim().to_owned())
            .filter(|t| !t.is_empty()),
        None => load_token_with(credential, env).ok(),
    }
}

/// `text` with every occurrence of `secret` replaced by `***`.
pub fn redact(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_owned()
    } else {
        text.replace(secret, REDACTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_env(dir: &Path) -> impl Fn(&str) -> Option<String> + use<> {
        let dir = dir.to_string_lossy().into_owned();
        move |var| (var == "RELAY_TOKEN_DIR").then(|| dir.clone())
    }

    #[test]
    fn reads_and_trims_the_credential_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("relay-token"), "  abc.def  \r\n").unwrap();
        assert_eq!(
            load_token_with("relay-token", dir_env(tmp.path())).unwrap(),
            "abc.def"
        );
    }

    #[test]
    fn relay_token_dir_wins_over_credentials_directory() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        std::fs::write(a.path().join("t"), "from-a").unwrap();
        std::fs::write(b.path().join("t"), "from-b").unwrap();
        let (da, db) = (
            a.path().to_string_lossy().into_owned(),
            b.path().to_string_lossy().into_owned(),
        );
        let env = |var: &str| match var {
            "RELAY_TOKEN_DIR" => Some(da.clone()),
            "CREDENTIALS_DIRECTORY" => Some(db.clone()),
            _ => None,
        };
        assert_eq!(load_token_with("t", env).unwrap(), "from-a");
    }

    #[test]
    fn falls_back_to_credentials_directory() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("t"), "sysd").unwrap();
        let d = tmp.path().to_string_lossy().into_owned();
        let env = |var: &str| match var {
            "RELAY_TOKEN_DIR" => Some(String::new()),
            "CREDENTIALS_DIRECTORY" => Some(d.clone()),
            _ => None,
        };
        assert_eq!(load_token_with("t", env).unwrap(), "sysd");
    }

    #[test]
    fn no_directory_is_an_error() {
        let err = load_token_with("relay-token", |_| None).unwrap_err();
        assert!(matches!(err, TokenError::NoDirectory(_)));
        assert!(err.to_string().contains("RELAY_TOKEN_DIR"));
    }

    #[test]
    fn empty_and_missing_files_are_errors() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("blank"), " \n").unwrap();
        assert!(matches!(
            load_token_with("blank", dir_env(tmp.path())),
            Err(TokenError::Empty(_))
        ));
        assert!(matches!(
            load_token_with("absent", dir_env(tmp.path())),
            Err(TokenError::Unreadable { .. })
        ));
    }

    #[test]
    fn redact_replaces_every_occurrence() {
        assert_eq!(
            redact("login abc.def failed (abc.def)", "abc.def"),
            "login *** failed (***)"
        );
        assert_eq!(redact("nothing here", ""), "nothing here");
    }
}
