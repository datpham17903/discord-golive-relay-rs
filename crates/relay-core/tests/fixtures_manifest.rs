//! Smoke test: the shared T4 codec fixtures (kept once, under `relay-media`) are reachable from
//! relay-core and the manifest parses. Worker/publisher tests load samples from here.

use std::path::PathBuf;

use serde_json::Value;

#[test]
fn test_shared_fixtures_reachable() {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../relay-media/tests/fixtures/codecs");
    let text = std::fs::read_to_string(dir.join("manifest.json")).expect("manifest.json");
    let v: Value = serde_json::from_str(&text).expect("manifest parses");
    let files = v["files"].as_array().expect("files array");
    assert!(!files.is_empty());
    for e in files {
        let name = e["file"].as_str().unwrap();
        assert!(dir.join(name).is_file(), "{name} missing");
    }
}
