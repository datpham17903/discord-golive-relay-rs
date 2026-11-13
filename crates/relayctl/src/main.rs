mod dev;
use relayctl::{calibrate, cli, dashboard, tokens};

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "relayctl", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(flatten)]
    Relay(cli::Command),
    /// Derive `source_dead_timeout_seconds` from watch logs.
    #[command(name = "calibrate-tdead", hide = true)]
    CalibrateTdead(calibrate::CalibrateArgs),
    /// Public read-only status page served from status.json and the run log.
    Dashboard(dashboard::DashboardArgs),
    /// Developer tools (live probes); not part of the supported interface.
    #[command(subcommand, hide = true)]
    Dev(dev::DevCommand),
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Relay(command) => {
            ExitCode::from(u8::try_from(cli::run_process(command)).unwrap_or(1))
        }
        Command::CalibrateTdead(args) => ExitCode::from(
            u8::try_from(calibrate::run(
                &args,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            ))
            .unwrap_or(1),
        ),
        Command::Dashboard(args) => {
            ExitCode::from(u8::try_from(dashboard::run(&args)).unwrap_or(1))
        }
        Command::Dev(command) => dev::run(command),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::CommandFactory;

    use super::*;
    use crate::dev::DevCommand;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("relayctl").chain(args.iter().copied()))
    }

    #[test]
    fn dev_watch_defaults() {
        let cli = parse(&["dev", "watch", "--config", "relay.yaml"]).unwrap();
        let Command::Dev(DevCommand::Watch(args)) = cli.command else {
            panic!("not dev watch");
        };
        assert_eq!(args.config, PathBuf::from("relay.yaml"));
        assert_eq!(args.credential, "relay-token");
        assert_eq!(args.seconds, 120);
    }

    #[test]
    fn dev_watch_overrides() {
        let cli = parse(&[
            "dev",
            "watch",
            "--config",
            "c.yaml",
            "--credential",
            "relay-token-2",
            "--seconds",
            "30",
        ])
        .unwrap();
        let Command::Dev(DevCommand::Watch(args)) = cli.command else {
            panic!("not dev watch");
        };
        assert_eq!(args.credential, "relay-token-2");
        assert_eq!(args.seconds, 30);
    }

    #[test]
    fn dev_watch_requires_config() {
        assert!(parse(&["dev", "watch"]).is_err());
    }

    #[test]
    fn dev_watch_rejects_bad_seconds() {
        for bad in ["0", "-1", "abc"] {
            assert!(
                parse(&["dev", "watch", "--config", "c.yaml", "--seconds", bad]).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn token_is_not_an_argument() {
        assert!(parse(&["dev", "watch", "--config", "c.yaml", "--token", "x"]).is_err());
    }

    #[test]
    fn dev_relay_defaults() {
        let cli = parse(&["dev", "relay", "--config", "relay.yaml"]).unwrap();
        let Command::Dev(DevCommand::Relay(args)) = cli.command else {
            panic!("not dev relay");
        };
        assert_eq!(args.config, PathBuf::from("relay.yaml"));
        assert_eq!(args.credential, "relay-token");
        assert_eq!(args.viewer_credential, "relay-token-2");
        assert_eq!(args.seconds, 120);
    }

    #[test]
    fn dev_relay_overrides() {
        let cli = parse(&[
            "dev",
            "relay",
            "--config",
            "c.yaml",
            "--credential",
            "a",
            "--viewer-credential",
            "b",
            "--seconds",
            "45",
        ])
        .unwrap();
        let Command::Dev(DevCommand::Relay(args)) = cli.command else {
            panic!("not dev relay");
        };
        assert_eq!(args.credential, "a");
        assert_eq!(args.viewer_credential, "b");
        assert_eq!(args.seconds, 45);
    }

    #[test]
    fn dev_relay_requires_config_and_takes_no_token_argument() {
        assert!(parse(&["dev", "relay"]).is_err());
        for flag in ["--token", "--viewer-token"] {
            assert!(parse(&["dev", "relay", "--config", "c.yaml", flag, "x"]).is_err());
        }
        for bad in ["0", "-1", "abc"] {
            assert!(parse(&["dev", "relay", "--config", "c.yaml", "--seconds", bad]).is_err());
        }
    }

    #[test]
    fn dev_is_hidden_from_help() {
        let help = Cli::command().render_help().to_string();
        assert!(!help.contains("dev"), "{help}");
    }
}
