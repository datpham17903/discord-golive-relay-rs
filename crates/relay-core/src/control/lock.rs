//! Per-account instance lock (spec §15), port of `golive_relay/control/lock.py`.
//!
//! Python exposes these as context managers; here the guard is the value, and dropping it releases
//! the lock. The lock file is never deleted, as in Python.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs4::{FileExt, TryLockError};

/// Another process already holds the lock for this relay account.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct InstanceLocked(pub String);

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error(transparent)]
    Locked(#[from] InstanceLocked),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Takes a non-blocking exclusive lock on `f`; `Ok(false)` if held elsewhere.
pub fn try_lock(f: &File) -> io::Result<bool> {
    match FileExt::try_lock(f) {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

pub fn unlock(f: &File) -> io::Result<()> {
    FileExt::unlock(f)
}

/// Waits for an exclusive lock on `f`.
pub fn lock_blocking(f: &File) -> io::Result<()> {
    FileExt::lock(f)
}

/// `RELAY_LOCK_DIR`, else `/run/lock/discord-relay` on POSIX when it is writable, else the temp dir.
pub fn default_lock_dir() -> PathBuf {
    if let Some(env) = std::env::var_os("RELAY_LOCK_DIR")
        && !env.is_empty()
    {
        return PathBuf::from(env);
    }
    #[cfg(unix)]
    {
        let run_lock = Path::new("/run/lock/discord-relay");
        // A real create-and-delete probe: permission bits alone do not tell whether we may write.
        if std::fs::create_dir_all(run_lock).is_ok() && tempfile::tempfile_in(run_lock).is_ok() {
            return run_lock.to_path_buf();
        }
    }
    std::env::temp_dir()
}

/// One service instance per relay account, held until dropped or process death.
#[derive(Debug)]
pub struct InstanceLock {
    file: Option<File>,
}

impl InstanceLock {
    pub fn acquire(relay_user_id: u64, lock_dir: Option<&Path>) -> Result<Self, LockError> {
        let dir = lock_dir.map_or_else(default_lock_dir, Path::to_path_buf);
        let path = dir.join(format!("discord-relay-{relay_user_id}.lock"));
        std::fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        if !try_lock(&file)? {
            return Err(InstanceLocked(format!(
                "relay account instance already running ({})",
                path.display()
            ))
            .into());
        }
        Ok(Self { file: Some(file) })
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            let _ = unlock(&f);
        }
    }
}

/// One `InstanceLock` per account, acquired all or none.
#[derive(Debug)]
pub struct InstanceLocks {
    held: Vec<InstanceLock>,
}

impl InstanceLocks {
    /// Duplicate ids are locked once; the first failure releases everything taken so far.
    pub fn acquire(
        user_ids: impl IntoIterator<Item = u64>,
        lock_dir: Option<&Path>,
    ) -> Result<Self, LockError> {
        let mut locks = Self { held: Vec::new() };
        let mut seen = std::collections::HashSet::new();
        for uid in user_ids {
            if seen.insert(uid) {
                locks.held.push(InstanceLock::acquire(uid, lock_dir)?);
            }
        }
        Ok(locks)
    }
}

impl Drop for InstanceLocks {
    fn drop(&mut self) {
        while self.held.pop().is_some() {}
    }
}
