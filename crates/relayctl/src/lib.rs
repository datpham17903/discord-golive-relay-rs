//! Library side of `relayctl`: the commands (S3), the service wiring (S2) and token loading, shared
//! by the binary and the integration tests.

pub mod calibrate;
pub mod cli;
pub mod service;
pub mod tokens;
