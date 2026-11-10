//! Hidden developer subcommands: live probes used to verify the gates before the service exists.

mod forward;
mod relay;
mod session;
mod stats;
mod watch;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand};

#[derive(Subcommand)]
pub enum DevCommand {
    /// Join the allowed voice channel, watch the target's Go Live and report what arrives (gate
    /// V-DAVE). Receive only: never creates or publishes a stream.
    Watch(WatchArgs),
    /// Relay the target's Go Live as the relay account's own, and watch it with a second account
    /// (gate V-RELAY). Publishes a stream: run it only against a live test setup.
    Relay(RelayArgs),
}

#[derive(Args, Debug)]
pub struct WatchArgs {
    /// Relay YAML config (`discord.guild_id`, `target_user_id`, `allowed_voice_channel_id`).
    #[arg(long)]
    pub config: PathBuf,
    /// Credential name under `RELAY_TOKEN_DIR` / `CREDENTIALS_DIRECTORY`.
    #[arg(long, default_value = "relay-token")]
    pub credential: String,
    /// How long to watch once connected.
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..))]
    pub seconds: u64,
}

#[derive(Args, Debug)]
pub struct RelayArgs {
    /// Relay YAML config (`discord.*`, `quality`, `runtime`).
    #[arg(long)]
    pub config: PathBuf,
    /// Credential of the relay account (watches the target, publishes the relayed stream).
    #[arg(long, default_value = "relay-token")]
    pub credential: String,
    /// Credential of the viewer account (watches the relay's stream).
    #[arg(long, default_value = "relay-token-2")]
    pub viewer_credential: String,
    /// How long the viewer watches once connected.
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..))]
    pub seconds: u64,
}

pub fn run(command: DevCommand) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match command {
        DevCommand::Watch(args) => ExitCode::from(runtime.block_on(watch::run(args))),
        DevCommand::Relay(args) => ExitCode::from(runtime.block_on(relay::run(args))),
    }
}
