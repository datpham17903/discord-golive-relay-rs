//! Local control socket: a Unix socket, one JSON request line in, one JSON response line out
//! (port of `control/server.py`). On Windows the same protocol runs over a named pipe whose name is
//! derived from the socket path ([`pipe_name`]).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// `handler(request) -> response`; runs on the connection's task, so it must not block for long.
pub type Handler = Arc<dyn Fn(&Value) -> Value + Send + Sync>;

#[cfg_attr(not(any(unix, windows)), allow(dead_code))]
pub struct ControlServer {
    path: PathBuf,
    handler: Handler,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// The Windows named pipe standing for the control socket at `path`: a hash of its absolute path
/// (lowercased: Windows paths ignore case), so services with different state directories never share
/// a pipe, and `relayctl --state-dir` reaches the service whose `storage.state_directory` it names.
pub fn pipe_name(path: &Path) -> String {
    let path = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let key = path.to_string_lossy().to_lowercase();
    // FNV-1a 64: stable across builds and platforms, unlike `DefaultHasher`
    let hash = key.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!(r"\\.\pipe\discord-relay-{hash:016x}")
}

impl ControlServer {
    /// `None` where neither Unix sockets nor named pipes exist.
    pub fn create(path: impl Into<PathBuf>, handler: Handler) -> Option<Self> {
        cfg!(any(unix, windows)).then(|| Self {
            path: path.into(),
            handler,
            task: None,
        })
    }

    /// Replaces a stale socket file, listens, and restricts the file to owner and group.
    #[cfg(unix)]
    pub async fn start(&mut self) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = tokio::net::UnixListener::bind(&self.path)?;
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o660))?;
        let handler = Arc::clone(&self.handler);
        self.task = Some(tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        // SO_PEERCRED; never the client's own claim.
                        let peer = stream.peer_cred().ok().map(|c| u64::from(c.uid()));
                        let handler = Arc::clone(&handler);
                        tokio::spawn(async move {
                            serve(stream, peer, &handler, MAX_REQUEST_BYTES).await;
                        });
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }));
        Ok(())
    }

    /// Creates the first instance of [`pipe_name`] and serves it, one instance per client.
    ///
    /// The pipe keeps its default DACL: only this user, SYSTEM and administrators may write to it
    /// (everyone else may at most open it for reading, which cannot send a request); remote clients
    /// are rejected. There is no peer uid, so requests carry `_peer_uid: null`.
    #[cfg(windows)]
    pub async fn start(&mut self) -> io::Result<()> {
        use tokio::net::windows::named_pipe::ServerOptions;

        let name = pipe_name(&self.path);
        // Fails rather than add instances to a pipe someone else created under this name.
        let mut listening = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&name)?;
        let handler = Arc::clone(&self.handler);
        self.task = Some(tokio::spawn(async move {
            loop {
                let connected = listening.connect().await;
                // The next instance listens before this one is served; meanwhile clients retry.
                let next = loop {
                    match ServerOptions::new().create(&name) {
                        Ok(next) => break next,
                        Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                };
                let stream = std::mem::replace(&mut listening, next);
                if connected.is_ok() {
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        serve(stream, None, &handler, MAX_REQUEST_BYTES).await;
                    });
                }
            }
        }));
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    pub async fn start(&mut self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "control socket is not available on this platform",
        ))
    }

    /// Stops listening and removes the socket file (Windows: the pipe goes once its instances close).
    pub fn close(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The uid this process runs as, read off the owner of a file it creates (`geteuid` needs `unsafe`).
#[cfg(unix)]
pub fn own_uid() -> Option<u64> {
    use std::os::unix::fs::MetadataExt;

    tempfile::tempfile()
        .and_then(|f| f.metadata())
        .ok()
        .map(|m| u64::from(m.uid()))
}

#[cfg(not(unix))]
pub fn own_uid() -> Option<u64> {
    None
}

/// One connection: read a request line (5 s, at most `max_request` bytes), answer with one line.
pub async fn serve<S>(stream: S, peer_uid: Option<u64>, handler: &Handler, max_request: usize)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (rd, mut wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd).take(max_request as u64 + 1);
    let mut line = Vec::new();
    match tokio::time::timeout(READ_TIMEOUT, rd.read_until(b'\n', &mut line)).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    let resp = match respond(&line, peer_uid, handler, max_request) {
        Ok(v) => v,
        Err(e) => json!({"ok": false, "error": e}),
    };
    let mut out = resp.to_string();
    out.push('\n');
    let _ = wr.write_all(out.as_bytes()).await;
    let _ = wr.shutdown().await;
}

fn respond(
    line: &[u8],
    peer_uid: Option<u64>,
    handler: &Handler,
    max_request: usize,
) -> Result<Value, String> {
    if line.len() > max_request {
        return Err("ValueError: request too large".into());
    }
    let mut req: Value =
        serde_json::from_slice(line).map_err(|e| format!("JSONDecodeError: {e}"))?;
    let Some(obj) = req.as_object_mut() else {
        return Err("ValueError: request must be an object".into());
    };
    obj.insert("_peer_uid".into(), json!(peer_uid));
    Ok(handler(&req))
}

#[cfg(test)]
mod tests {
    use tokio::io::duplex;

    use super::*;

    fn echo() -> Handler {
        Arc::new(|req| json!({"ok": true, "echo": req["cmd"], "peer": req["_peer_uid"]}))
    }

    async fn exchange(request: &[u8], limit: usize, peer: Option<u64>) -> Value {
        let (mut client, server) = duplex(4096);
        let handler = echo();
        let task = tokio::spawn(async move { serve(server, peer, &handler, limit).await });
        client.write_all(request).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        task.await.unwrap();
        assert!(buf.ends_with(b"\n"));
        serde_json::from_slice(&buf).unwrap()
    }

    #[tokio::test]
    async fn oversized_request_line_gets_json_error_and_closes() {
        let mut req = vec![b'x'; 500];
        req.push(b'\n');
        let resp = exchange(&req, 64, None).await;
        assert_eq!(resp["ok"], false);
    }

    #[tokio::test]
    async fn answers_with_the_peer_uid_never_a_client_supplied_one() {
        let resp = exchange(
            b"{\"cmd\": \"status\", \"_peer_uid\": 0}\n",
            1024,
            Some(1000),
        )
        .await;
        assert_eq!(resp, json!({"ok": true, "echo": "status", "peer": 1000}));
    }

    #[tokio::test]
    async fn rejects_garbage_and_non_objects() {
        for bad in [&b"not json\n"[..], b"[1]\n", b"\n"] {
            let resp = exchange(bad, 1024, None).await;
            assert_eq!(resp["ok"], false, "{bad:?}");
        }
    }

    #[test]
    fn pipe_name_follows_the_absolute_path() {
        let rel = Path::new("state").join("control.sock");
        let abs = std::env::current_dir().unwrap().join(&rel);
        assert_eq!(pipe_name(&rel), pipe_name(&abs));
        assert_ne!(
            pipe_name(&rel),
            pipe_name(&Path::new("other").join("control.sock"))
        );
        let name = pipe_name(&rel);
        let hash = name.strip_prefix(r"\\.\pipe\discord-relay-").unwrap();
        assert!(hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[cfg(windows)]
    #[test]
    fn pipe_name_ignores_case_on_windows() {
        assert_eq!(
            pipe_name(Path::new(r"C:\Relay\State\control.sock")),
            pipe_name(Path::new(r"c:\relay\state\control.sock"))
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn named_pipe_roundtrip_one_instance_per_client() {
        use tokio::net::windows::named_pipe::ClientOptions;

        const ERROR_FILE_NOT_FOUND: i32 = 2;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        let name = pipe_name(&sock);
        let mut srv = ControlServer::create(&sock, echo()).unwrap();
        srv.start().await.unwrap();
        for _ in 0..3 {
            let mut c = ClientOptions::new().open(&name).unwrap();
            c.write_all(b"{\"cmd\": \"status\", \"_peer_uid\": 0}\n")
                .await
                .unwrap();
            let mut line = String::new();
            BufReader::new(c).read_line(&mut line).await.unwrap();
            let resp: Value = serde_json::from_str(&line).unwrap();
            // no peer uid on Windows, and never the client's own claim
            assert_eq!(resp, json!({"ok": true, "echo": "status", "peer": null}));
        }
        // a second server for the same state directory is refused
        let mut twin = ControlServer::create(&sock, echo()).unwrap();
        assert!(twin.start().await.is_err());
        srv.close();
        let gone = async {
            while ClientOptions::new()
                .open(&name)
                .err()
                .and_then(|e| e.raw_os_error())
                != Some(ERROR_FILE_NOT_FOUND)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), gone)
            .await
            .expect("pipe removed after close");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_socket_roundtrip_with_mode_660() {
        use std::os::unix::fs::PermissionsExt;
        use tokio::net::UnixStream;

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let mut srv = ControlServer::create(&sock, echo()).unwrap();
        srv.start().await.unwrap();
        assert_eq!(
            std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
            0o660
        );
        let mut c = UnixStream::connect(&sock).await.unwrap();
        c.write_all(b"{\"cmd\": \"status\"}\n").await.unwrap();
        let mut buf = Vec::new();
        c.read_to_end(&mut buf).await.unwrap();
        let resp: Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(resp["echo"], "status");
        assert_eq!(Some(resp["peer"].as_u64().unwrap()), own_uid());
        srv.close();
        assert!(!sock.exists());
    }
}
