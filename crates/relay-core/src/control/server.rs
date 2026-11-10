//! Local control socket: a Unix socket, one JSON request line in, one JSON response line out
//! (port of `control/server.py`). Not available on Windows, where [`ControlServer::create`] is `None`.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// `handler(request) -> response`; runs on the connection's task, so it must not block for long.
pub type Handler = Arc<dyn Fn(&Value) -> Value + Send + Sync>;

#[cfg_attr(not(unix), allow(dead_code))]
pub struct ControlServer {
    path: PathBuf,
    handler: Handler,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ControlServer {
    /// `None` where Unix sockets are not used (Windows).
    pub fn create(path: impl Into<PathBuf>, handler: Handler) -> Option<Self> {
        cfg!(unix).then(|| Self {
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

    #[cfg(not(unix))]
    pub async fn start(&mut self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "control socket is not available on Windows",
        ))
    }

    /// Stops listening and removes the socket file.
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

    #[cfg(windows)]
    #[test]
    fn unavailable_on_windows() {
        assert!(ControlServer::create("c.sock", echo()).is_none());
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
