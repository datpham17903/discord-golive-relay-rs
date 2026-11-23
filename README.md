# discord-golive-relay (Rust)

Relay one consenting person's Discord Go Live to a bigger audience: one account watches the stream once, and one or more accounts publish it again as their own Go Live.

```
target's Go Live
      |
      v
ingest account                 watches the target (takes 1 of its 50 viewer slots)
      |
      +--> ingest's own Go Live        up to 50 viewers
      +--> relay accounts' Go Lives    main channel, started as viewers grow
      +--> mirror accounts' Go Lives   other voice channels
```

`relayctl` is a single binary, the Rust port of the Python relay: running it needs no Python, ffmpeg or Rust toolchain. It relays while the target shares in the configured voice channel and stops when they stop, leave or move.

**Passthrough only.** Video and audio are forwarded unchanged. The `transcode` modes are not supported, and `codecs.video_encoder`, `codecs.hw_decode` and the `*_bitrate_kbps` keys are ignored. A source larger than `quality.max_*`, or audio that is not one 20 ms Opus frame per packet, is not relayed: there is no downscale or re-encode fallback (the transcode notes in `config.example.yaml` do not apply).

## Warnings

- Automating a normal Discord account (a self-bot) breaks Discord's terms, and accounts can be terminated. Every extra account adds risk. Use dedicated accounts, never your main one.
- Relay only with the clear consent of the person being relayed, for this destination and audience. Write the agreement down and reference it in `authorization.agreement_ref`; the field does not replace real consent.
- Never share or commit a token. Tokens live only in token files (`secrets/` is gitignored). If one leaks, log that account out or change its password, then take a new token.
- Never use the target's own password or token.

## Install

Download your platform's archive from GitHub Releases and check it with the `.sha256` file next to it (`sha256sum -c`, or `shasum -a 256 -c` on macOS):

- `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc`, `aarch64-apple-darwin`
- experimental, missing from a release when their build fails: `aarch64-unknown-linux-gnu`, `x86_64-unknown-linux-musl` (static)

Each archive holds `relayctl`, `config.example.yaml`, `manifests/`, this README, `LICENSE` and `NOTICE`. The relay needs outbound TCP and UDP to Discord. It is used live on Windows and on Linux x86_64 (the musl build); the macOS and Linux arm64 builds have not been run live.

To build from source you need the toolchain pinned in `rust-toolchain.toml` (installed by `rustup`), `cmake` and `libclang`. On Windows, BoringSSL (built by `wreq`) also needs NASM on `PATH` and `LIBCLANG_PATH` set to the LLVM `bin` folder.

```bash
cargo build --release -p relayctl      # target/release/relayctl
```

## Setup

### IDs and tokens

Turn on Developer Mode (User Settings > Advanced), then right-click the server, the target and their voice channel and copy each ID: these are `guild_id`, `target_user_id` and `allowed_voice_channel_id`. Account user IDs are optional; they are read from the token files at startup.

For each relay account (one dedicated account per token): log in to Discord in a browser, open the developer tools (F12) > Network, click any request to `discord.com/api` and copy its `authorization` header. Do not press Discord's "Log out" afterwards: that invalidates the token. Save each token as a one-line file named after its credential: `secrets/relay-token` for the ingest account, `secrets/relay-token-2` and so on for the others (`chmod 600 secrets/*` on Linux). Tokens are never read from YAML or the command line.

### Configuration

```bash
cp config.example.yaml config.yaml
relayctl validate --config config.yaml
```

`config.example.yaml` lists every key with its default. Set at least the three IDs, `authorization.agreement_ref` and, on Windows, `storage.state_directory` (for example `state`). `validate` works offline and lists every problem at once: unknown keys are errors, IDs are quoted strings, only `true` and `false` are booleans (YAML 1.2), `max_width` and `max_height` are even, and `runtime.recording_enabled` and `logging.include_media` stay `false`.

More accounts go in `discord.relay_accounts` (extra Go Lives in the main channel) and `discord.mirror_accounts` (Go Lives in other voice channels, optionally pinned with `channel_id`), each as `{credential: <token file name>}`. Credential names match `[A-Za-z0-9_.-]+` and are unique, an account is in one list only, and there are at most 9 of them. Output accounts do not watch the target, so they take no slot on its stream, and each has its own FATAL latch and quarantine.

- **Autoscale** (`runtime.relay_autoscale: true`): relay accounts publish only when the main channel needs them. The target's stream holds 49 viewers besides the ingest and each Go Live holds 50, so with the ingest publishing, 99 people need no relay, 100 need one and 150 need two (the target and the relay's own accounts do not count). A relay stops after `runtime.relay_idle_stop_seconds` without viewers; a failing one is replaced by the next free account.
- **Codec** (`codecs.output_codec: adaptive`): all outputs forward the target's video in one common codec, the best of AV1 > H265 > H264 > VP8 > VP9 that the target sends and every viewer decodes. A viewer who decodes less lowers it until they leave. `H264` is the fixed H.264 mode.

### Calibrate T_dead

The service refuses to run while `liveness.source_dead_timeout_seconds` is `null`. Measure it once: let the target share a static screen for at least 10 minutes while the receive-only probe records it, then calibrate from the log:

```sh
relayctl dev watch --config config.yaml --seconds 660 2> tdead.jsonl
relayctl calibrate-tdead tdead.jsonl --write --config config.yaml   # without --write it only prints
```

The value is `max(3.0, round_up(2 x max_gap + 1.0, 0.5))` over the log's `stats` lines (Python relay logs work too). This `--write` is the only time the relay edits the config.

## Running

```bash
export RELAY_TOKEN_DIR=secrets        # PowerShell: $env:RELAY_TOKEN_DIR = "secrets"
relayctl doctor --config config.yaml
relayctl run    --config config.yaml
```

A fresh install starts disabled: run `relayctl enable --state-dir DIR` once from another shell, where `DIR` is `storage.state_directory`. From then on the service relays whenever the target shares in the allowed channel. It stops when they stop, leave or move, on `relayctl disable`, or on SIGINT/SIGTERM. Runtime state (enabled flag, FATAL latches, quarantine, routing) lives in `state.json` in the state directory.

| Command | What it does |
| --- | --- |
| `validate --config FILE` | Checks the config offline, then prints one `not operable: ...` line per reason `run` would refuse. Exit 1 if invalid |
| `doctor --config FILE` | Checks the config and the state directory (creating it). Never publishes. Exit 1 on a failed check |
| `run --config FILE [--metrics-listen IP:PORT]` | Runs the service. Exit 78 means a config or credential problem, which systemd does not restart |
| `status`, `enable`, `disable` | Shows the state, or sets the persistent enabled flag (`disable` also stops the session) |
| `reset-fatal [--account USER_ID]` | Clears the FATAL latch of the ingest, or of one output account, after you fixed the cause |
| `dashboard` | Public read-only status page built from `status.json` and the log of `run` (`relayctl dashboard --help`) |

`status`, `enable`, `disable` and `reset-fatal` take the same `--state-dir` (default `/var/lib/discord-relay`). They ask the running service, or use the files in the state directory when none answers. On Windows the control channel is a named pipe derived from that directory's absolute path; only the service's user, SYSTEM and administrators can reach it, and each of them counts as an operator.

**Monitoring.** `--metrics-listen 127.0.0.1:9108` (or `RELAY_METRICS_LISTEN`) serves `GET /healthz` (`200` while `status.json` is less than 15 s old, else `503`) and `GET /metrics` in Prometheus format (`relay_up`, `relay_state{state}`, `relay_viewers`, per-account `relay_account_*`, and every counter and gauge of `status.json`). It is off by default and has no authentication, so bind it to localhost or a private interface. No token or channel ID is exposed.

### Chat commands

With `control.chat_commands: true` (the default) the ingest account answers commands typed in the guild:

| Command | What it does |
| --- | --- |
| `!relay` | Lists the commands; public ones are marked |
| `!relay status` | State, target, channels, every relay and mirror account, cooldowns |
| `!relay on` / `off` | Same as `relayctl enable` / `disable` |
| `!relay target <user_id>` | Relays another person |
| `!relay channel <channel_id>` | Moves the main voice channel |
| `!relay mirror [channel_id [account_id]]` | Publishes in another voice channel; without an ID, the one you sit in |
| `!relay unmirror [channel_id \| all]` | Removes the mirror of your channel, of that channel, or all of them (only their creator or an operator) |
| `!relay prejoin on\|off`, `autoscale on\|off` | Turns pre-join or relay autoscale on or off |
| `!relay op list`, `op add <user_id>`, `op remove <user_id>` | Lists operators (operators only); adding and removing is for owners only |

Anyone may use the commands in `control.public_commands` (default `status`, `mirror`, `unmirror`); the others need an operator: an owner (`control.operator_user_ids`, or a role in `operator_role_ids`) or someone an owner added in chat. When `control.command_channel_id` is set, commands are accepted only there. Commands without permission, from elsewhere, or from the relay accounts are ignored without a reply. IDs are plain digits (mentions are refused), replies never ping, and message text is never logged. Target, channel and mirror changes are stored in `state.json` and survive restarts; an invalid stored routing stops the service at start.

## Deploying on Linux with systemd

Download is the target's bitrate (about 1 to 8 Mbps) and upload is that bitrate per publishing account. Passthrough needs little CPU, so a small VPS is enough. Do not run the relay on the target's streaming PC, which shares its uplink.

As root, from the unpacked Linux archive and `deploy/systemd/discord-relay.service` of this repository:

```bash
useradd --system --home /opt/discord-relay --shell /usr/sbin/nologin discord-relay
install -d /opt/discord-relay
install -m 0755 relayctl /opt/discord-relay/relayctl
install -d -m 0750 -o root -g discord-relay /etc/discord-relay
install -m 0640 -g discord-relay config.yaml /etc/discord-relay/config.yaml
install -m 0600 secrets/relay-token   /etc/discord-relay/token
install -m 0600 secrets/relay-token-2 /etc/discord-relay/token-2
```

Keep `storage.state_directory: /var/lib/discord-relay` (the unit's `StateDirectory=`). The unit loads the ingest token; add a `LoadCredential=` line for each output account, with its credential name on the left:

```
LoadCredential=relay-token:/etc/discord-relay/token
LoadCredential=relay-token-2:/etc/discord-relay/token-2
```

```bash
install -m 0644 discord-relay.service /etc/systemd/system/
systemctl daemon-reload
sudo -u discord-relay /opt/discord-relay/relayctl doctor --config /etc/discord-relay/config.yaml
systemctl enable --now discord-relay
sudo -u discord-relay /opt/discord-relay/relayctl enable --state-dir /var/lib/discord-relay
journalctl -u discord-relay -f
```

The unit runs as `discord-relay`, restarts on failure except after exit 78, and reads `/etc/discord-relay` read-only. It has not been run live yet. Use each account in one place only: no second instance and no Discord client on it at the same time.

## Troubleshooting

- **Discord error 2002 / 2003 (bandwidth):** passthrough cannot lower the target's bitrate. Run the relay away from the target's PC, publish from fewer accounts, or ask the target to lower their stream bitrate.
- **Stutter:** the upload needed is the target's bitrate times the publishing accounts. Stalls that hit every output at the same moment come from the source arriving late (the target's upload), not from the relay. `logging.network_stats: true` adds packet loss, NACK and RTX counters to `status`.
- **`no credential: ...`:** set `RELAY_TOKEN_DIR` (or `LoadCredential=`); the file name must equal the credential name.

| Latch | Meaning | What to do |
| --- | --- | --- |
| FATAL `AUTH_REJECTED` | Discord rejected the token | Put in a new token, then `reset-fatal` |
| FATAL `ACCOUNT_OWNERSHIP_UNCERTAIN` | The account's voice session changed (probably used elsewhere) | Stop the other use, then `reset-fatal` |
| FATAL `RELAY_REJECTED_BY_SERVER` | 3 sessions in a row were kicked or moved | Check channel permissions and moderators, then `reset-fatal` |
| QUARANTINE | The account was moved or disconnected | Wait for the target's next share; `disable` / `enable` do not clear it |

`reset-fatal` (add `--account USER_ID` for an output account) neither enables the relay nor clears a quarantine.

## Development

```
crates/relay-core/      core types and logic
crates/relay-discord/   Discord gateway adapter
crates/relay-voice/     voice connection
crates/relay-media/     passthrough media pipeline
crates/relayctl/        the relayctl binary (service, CLI, dashboard)
deploy/                 systemd units: the relay, the status dashboard, an auto-update timer
manifests/              live results (recorded by hand) and the build manifest
tools/                  fixture generators
```

`cargo test --workspace` needs no Discord. CI runs fmt, clippy and the tests on Linux, Windows and macOS. Pushing a `v*` tag builds the archives listed under Install and attaches them to a GitHub Release.

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
