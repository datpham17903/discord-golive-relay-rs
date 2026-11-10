//! `test_instance_lock_env_dir`, in its own test binary because it mutates the process environment.

use relay_core::control::lock::{InstanceLock, LockError};

#[test]
fn instance_lock_env_dir() {
    let tmp = tempfile::tempdir().unwrap();
    // SAFETY: this binary holds a single test, so no other thread reads the environment.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RELAY_LOCK_DIR", tmp.path().join("locks"));
    }
    let _held = InstanceLock::acquire(7, None).unwrap();
    assert!(tmp.path().join("locks").is_dir());
    assert!(matches!(
        InstanceLock::acquire(7, None),
        Err(LockError::Locked(_))
    ));
}
