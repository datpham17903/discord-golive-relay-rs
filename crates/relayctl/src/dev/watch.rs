//! `relayctl dev watch`: the receive half of `scripts/m0/session.py` (+ the watch half of the g1/g2a
//! probes) on the Rust stack, to verify gate V-DAVE against a live account.
//!
//! One account logs in, joins the allowed voice channel (self-muted, like `session.py`), waits for the
//! target's Go Live in that channel, watches it and reports what arrives. JSON log lines go to stderr
//! (through `JsonLogger`, token redacted), one summary object goes to stdout. It only receives: no
//! stream is ever created or published. The session mechanics live in [`super::session`].

use std::io::Write;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use relay_core::config::load_config;
use serde_json::json;

use super::WatchArgs;
use super::session::{Probe, watch_loop};
use super::stats::{Progress, WatchAccount, exit_code, summary_json};
use crate::tokens::load_token;

/// How long to wait for the target to go live (the watch duration only starts after it).
const STREAM_WAIT: Duration = Duration::from_secs(300);

/// The summary on stdout (one line), then the exit code.
fn finish(probe: &Probe) -> u8 {
    let account = probe.account.lock();
    let seen = probe.observed.lock();
    let summary = summary_json(
        &probe.progress.lock(),
        &account,
        seen.dave_protocol_version,
        seen.transport_failures + seen.dave_failures,
    );
    println!("{}", probe.redact_secrets(&summary.to_string()));
    exit_code(&account)
}

/// Prints a summary for a run that failed before the probe existed.
fn finish_early(errors: Vec<String>) -> u8 {
    let progress = Progress {
        errors,
        ..Progress::default()
    };
    println!("{}", summary_json(&progress, &WatchAccount::new(), None, 0));
    1
}

pub async fn run(args: WatchArgs) -> u8 {
    let config = match load_config(&args.config) {
        Ok(config) => config,
        Err(e) => return finish_early(e.errors),
    };
    let token = match load_token(&args.credential) {
        Ok(token) => token,
        Err(e) => return finish_early(vec![e.to_string()]),
    };
    let writer: Arc<StdMutex<dyn Write + Send>> = Arc::new(StdMutex::new(std::io::stderr()));
    let probe = Arc::new(Probe::new(
        writer,
        None,
        token,
        &[],
        config.discord.guild_id,
        config.discord.target_user_id,
        config.discord.allowed_voice_channel_id,
    ));

    let result = tokio::select! {
        result = flow(&probe, args.seconds) => result,
        _ = tokio::signal::ctrl_c() => {
            probe.log("stop_requested", json!({"reason": "sigint"}));
            Ok(())
        }
    };
    if let Err(message) = result {
        probe.error(message);
    }
    probe.teardown().await;
    finish(&probe)
}

async fn flow(probe: &Arc<Probe>, seconds: u64) -> Result<(), String> {
    let mut conn = probe.connect().await?;
    let stream = probe.watch_target(&conn, STREAM_WAIT, None).await?;
    watch_loop(
        probe,
        &conn.voice,
        &stream,
        &mut conn.voice_events,
        Some(seconds),
    )
    .await;
    Ok(())
}
