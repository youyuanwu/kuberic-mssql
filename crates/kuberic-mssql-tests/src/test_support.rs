//! Runtime Cargo paths also work after nextest remaps an archived build.

use std::path::{Path, PathBuf};

pub mod phase_timing;

pub fn manifest_dir() -> PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

pub fn workspace_root() -> PathBuf {
    manifest_dir()
        .parent()
        .and_then(Path::parent)
        .expect("test crate belongs to the two-level crates workspace")
        .to_path_buf()
}

pub fn companion_binary(name: &str, cargo_fallback: Option<&str>) -> PathBuf {
    std::env::var_os(format!("NEXTEST_BIN_EXE_{}", name.replace('-', "_")))
        .or_else(|| std::env::var_os(format!("CARGO_BIN_EXE_{name}")))
        .map(PathBuf::from)
        .or_else(|| cargo_fallback.map(PathBuf::from))
        .unwrap_or_else(|| panic!("{name} binary path is unavailable"))
}
