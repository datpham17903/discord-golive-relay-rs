# discord-golive-relay (Rust)

Relay one consenting person's Discord Go Live to one or more relay accounts: watch once, publish many times.

This is the Rust port of the Python relay. It is a single `relayctl` binary: no Python, no ffmpeg and no Rust toolchain are needed to run it.

**This build is passthrough only.** The target's video and audio are forwarded unchanged. `codecs.video_mode: transcode` and `codecs.audio_mode: transcode` are not supported. `codecs.video_encoder`, `codecs.hw_decode` and `quality.audio_bitrate_kbps` are accepted for config compatibility but ignored. A source larger than `quality.max_*`, or audio that is not one 20 ms Opus frame per packet, is not relayed (there is no downscale or re-encode fallback).

## 1. What it is

One consenting person (the target) shares their screen with Discord Go Live in one voice channel. A relay account joins the same channel, watches that stream, and publishes it again as its own Go Live. Extra publish-only accounts publish the same stream, so more people can watch (up to 50 viewers per Go Live).

```
target Go Live
      |
      v
ingest relay account (watches the target, uses 1 viewer slot)
      |
      +--> relay Go Live (ingest account)   up to 50 viewers
      +--> relay Go Live (output account 1) up to 50 viewers
      +--> ... up to 9 extra output accounts
```

The relay stops by itself when the target stops sharing, leaves the voice channel or moves to another channel.

## 2. Important warnings

- Automating a normal Discord account (a "self-bot") violates Discord's terms. Accounts can be terminated. Every extra account adds risk. Use dedicated accounts, never your main one.
- You need the clear consent of the person being relayed, for this destination and this audience. Write down the agreement and put a reference in `authorization.agreement_ref`. The config field does not replace real consent.
- Never share a token and never commit one. Tokens live only in token files (`secrets/` is gitignored). If a token was pasted in a chat or log, log out of that account or change its password to invalidate it, then get a new token.
- Never use the target's own password or token.

## 3. Install

Download the archive for your platform from the GitHub Releases page and unpack it:

| Platform | Archive |
| --- | --- |
| Linux x86_64 | `relayctl-<tag>-x86_64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `relayctl-<tag>-x86_64-pc-windows-msvc.zip` |
| macOS Apple Silicon | `relayctl-<tag>-aarch64-apple-darwin.tar.gz` |

Each archive holds `relayctl` (`relayctl.exe` on Windows), `config.example.yaml`, `manifests/`, `README.md`, `LICENSE` and `NOTICE`. You need outbound UDP and TCP to Discord (voice uses UDP).

Check the binary and the install (see section 7 for `doctor`):

```bash
./relayctl --version
./relayctl doctor --config config.example.yaml
```

With the example config, `doctor` also reports capability failures. That is expected until `liveness.source_dead_timeout_seconds` is set (section 6). `doctor` has no encoder or decoder probes in this build: it checks the config, the state directory and the capability manifest only. On Windows, set `storage.state_directory` first (section 5), because `doctor` creates that directory.

### Build from source

Prerequisites: the Rust toolchain from `rust-toolchain.toml` (1.98.0, installed automatically by `rustup`), `cmake` and `libclang` (for bindgen). `wreq` builds BoringSSL, which on Windows also needs NASM on `PATH` and `LIBCLANG_PATH` pointing at the LLVM `bin` folder.

```bash
cargo build --release -p relayctl      # binary: target/release/relayctl
cargo test --workspace
```

The live gates were run on Windows. Linux and macOS builds are unverified until CI has run on them.

## 4. Getting IDs and tokens

IDs (all are long numbers, always written as quoted strings):

1. Discord: User Settings, Advanced, turn on Developer Mode.
2. Right-click a user, channel or server and choose Copy User ID / Copy Channel ID / Copy Server ID.

| You need | Where |
| --- | --- |
| `guild_id` | the server |
| `target_user_id` | the person being relayed |
| `allowed_voice_channel_id` | the voice channel they share in |
| `expected_relay_user_id` (optional) | the ingest relay account |
| each output account `user_id` (optional) | each extra relay account |

Account user IDs are optional: `relayctl run` and `relayctl doctor` read them from the token files at startup and log `account <credential> -> <id>`. Set one to pin the account; it is checked at login.

Tokens: use a dedicated relay account per token. Log in to Discord in a browser, open developer tools (F12), Network tab, click any request to `discord.com/api`, and copy the `authorization` request header value. Do not log out with the Discord "Log out" button afterwards, because that invalidates the token.

Store each token as a one-line text file named after the credential:

| File | Used by |
| --- | --- |
| `secrets/relay-token` | ingest account (`discord.ingest_credential`) |
| `secrets/relay-token-2`, ... | relay and mirror accounts (`credential` in `relay_accounts` / `mirror_accounts`) |

On Linux restrict them: `chmod 600 secrets/*`. The relay never reads tokens from YAML or the command line.

## 5. Configuration

Copy the example and edit it. `config.yaml` is gitignored.

```bash
cp config.example.yaml config.yaml          # Windows: Copy-Item config.example.yaml config.yaml
relayctl validate --config config.yaml
```

`validate` prints every problem at once. Unknown keys are errors. All IDs must be quoted strings. Booleans follow YAML 1.2: only `true` and `false` are booleans; `yes`, `no`, `on` and `off` are plain strings and are rejected where a boolean is expected. `validate` works offline. The relay never writes the YAML file except for T_dead calibration with `--write` (section 6).

### Keys

| Key | Meaning | Default in example | Change when |
| --- | --- | --- | --- |
| `schema_version` | Config format | `1` | never |
| `discord.expected_relay_user_id` | ID of the ingest account; optional, read from the token at startup when absent | absent | you want to pin the ingest account |
| `discord.guild_id` | Server ID | dummy | always |
| `discord.target_user_id` | Person to relay (must differ from the relay) | dummy | always |
| `discord.allowed_voice_channel_id` | The only voice channel used | dummy | always |
| `discord.ingest_credential` | Token file name of the ingest account | `relay-token` | you use another file name |
| `discord.ingest_publishes` | Ingest account also publishes its own Go Live | `true` | set `false` to publish only from output accounts |
| `discord.relay_accounts` | Extra publish-only accounts for the main channel (list of `{credential}`, optional `user_id`); started as viewers grow. Relay and mirror accounts together: max 9, never shared | `[]` | the main channel outgrows one Go Live |
| `discord.mirror_accounts` | Publish-only accounts for other voice channels (`{credential}` plus optional `user_id` and `channel_id`); one account per channel, one channel per account | `[]` | you mirror the stream into other channels |
| `authorization.agreement_ref` | Note pointing to the consent record | placeholder | always |
| `authorization.excluded_viewer_ids` | Viewer IDs the target asked to exclude | `[]` | the target asks |
| `codecs.output_codec` | `adaptive` forwards the target's video unchanged in one common codec (see "Codec"); `H264` is the fixed H.264 mode | `adaptive` | rarely |
| `codecs.parent_codecs`, `watch_codecs` | Ignored when `output_codec` is `adaptive` | `h264_only` | rarely |
| `codecs.video_mode`, `codecs.audio_mode` | Must be `passthrough`; `transcode` is not supported in this build | `passthrough` | never |
| `codecs.video_encoder`, `codecs.hw_decode`, `quality.audio_bitrate_kbps`, `quality.video_bitrate_kbps` | Ignored (passthrough keeps the target's own bitrate and codec) | as in example | never |
| `quality.mode` | `auto` (a source up to `max_*` is passed through unchanged) or `fixed` | `auto` | rarely |
| `quality.max_width/height/fps` | Output cap in `auto` mode (width and height must be even). A larger source is not relayed | 2560 x 1440 @ 60 | lower it to refuse heavy sources |
| `quality.fixed_width/height/fps` | Expected size in `fixed` mode | 1920 x 1080 @ 60 | only with `fixed` |
| `quality.approved_profile_manifest` | Approved profiles file | `manifests/verified-profiles.json` | different location |
| `audio.enabled` | Relay the stream audio | `true` | set `false` for video only |
| `runtime.start_debounce_ms` | Wait before reacting to a start | `100` | `0`-`100` when viewers race for the 50 slots of the target's Go Live |
| `runtime.self_deaf_ingest`, `self_deaf_outputs` | Accounts join voice self-muted and self-deafened | `true` | only if the stream audio goes missing |
| `runtime.idle_voice_grace_seconds` | Grace while the target is idle in voice | `30` | rarely |
| `runtime.hold_recovery_timeout_seconds` | How long a HOLD (slate) may last | `15` | rarely |
| `runtime.connect_timeout_seconds` | Voice connect timeout | `15` | slow networks |
| `runtime.first_media_timeout_seconds` | Wait for first media from the target | `10` | rarely |
| `runtime.retry_delays_seconds`, `retry_cooldown_seconds` | Backoff list, then a cooldown | `[1, 2, 4, 8, 15]`, `60` | rarely |
| `runtime.keyframe_interval_seconds` | Keyframe interval (> 0 and <= 2) | `2` | rarely |
| `runtime.feedback_idr_min_interval_ms` | Min gap between keyframes forced by viewer feedback | `500` | rarely |
| `runtime.repeated_rejection_limit` | Consecutive rejected sessions before FATAL | `3` | rarely |
| `runtime.recording_enabled` | Recording | `false` | never (must be `false`) |
| `runtime.prejoin_voice` | The ingest joins the main channel while the target is there, before it streams. `!relay prejoin on\|off` overrides it at runtime | `true` | set `false` to stay out of voice until the target streams |
| `runtime.relay_autoscale` | Start relay accounts only as needed (see "Relay autoscale"); `false` = every relay account always publishes | `true` | you want all relays live at once |
| `runtime.relay_idle_stop_seconds` | How long an extra relay's Go Live must have no viewer before it stops | `30` | rarely |
| `runtime.stream_viewer_cap` | Viewers one Go Live can hold | `50` | only for tests |
| `liveness.source_dead_timeout_seconds` | Seconds without source packets before the source is treated as dead (`T_dead`) | `null` | set by section 6 (the service refuses to run while `null`) |
| `liveness.verified_capability_manifest` | Capability file with the measured value | `manifests/verified-capabilities.json` | different location |
| `storage.state_directory` | Where `state.json`, `status.json` and the control socket live | `/var/lib/discord-relay` | always on Windows (for example `state`) |
| `logging.level` | `DEBUG` `INFO` `WARNING` `ERROR` `CRITICAL` | `INFO` | debugging |
| `logging.include_media` | Log media content | `false` | never (must be `false`) |
| `logging.network_stats` | Expose packet loss / NACK / RTX counters in `status` | `false` | diagnosing stutter |
| `control.chat_commands` | Accept `!relay ...` commands from guild chat; when `false` no message is read | `true` | you do not want chat commands |
| `control.command_prefix` | First word of every command, no whitespace | `!relay` | it clashes with another bot |
| `control.operator_user_ids`, `operator_role_ids` | Owners (quoted IDs) allowed to use every command; with none set only the public commands work | `[]` | someone must use the non-public commands |
| `control.command_channel_id` | When set, commands are accepted only in this text channel | `null` | you want one command channel |
| `control.public_commands` | Commands anyone in the guild may use: `status`, `mirror`, `unmirror`, `on`, `off`, `target`, `channel`, `prejoin`, `autoscale` | `["status", "mirror", "unmirror"]` | you want to open or close a command |
| `control.command_cooldown_seconds` | Per-author cooldown for non-operators; `0` turns it off | `5` | rarely |
| `control.change_cooldown_seconds` | Global cooldown after an applied change; `0` turns it off | `3` | rarely |

Relative manifest paths are looked up next to the config file. `config.example.yaml` is the reference for every key.

### Multiple accounts

Each extra account needs its own token file. The credential name must match `[A-Za-z0-9_.-]+` and be unique (also different from `ingest_credential`). Relay accounts publish in the main channel; mirror accounts publish in other channels. An account is in one list only.

```yaml
discord:
  ingest_credential: relay-token
  relay_accounts:
    - {credential: relay-token-2}
  mirror_accounts:
    - {credential: relay-token-3, channel_id: "333333333333333333"}
```

Each account has its own quarantine and FATAL latch; one failing output does not stop the others. Output accounts do not watch the target, so they use no extra viewer slot on the target's stream. A relay account is never used as a mirror and a mirror account is never used as a relay. A mirror with `channel_id` is used only for that channel; mirrors without one are free for `!relay mirror`.

### Relay autoscale

With `runtime.relay_autoscale: true` only as many relay accounts publish as the main channel needs. N = members of the main channel, not counting the target, the ingest account and every relay or mirror account in the config. One Go Live holds `cap = runtime.stream_viewer_cap` viewers (50). The target's stream holds `cap - 1` (the ingest uses one slot) and the ingest's own Go Live holds `cap` more when `ingest_publishes` is true. Relays needed = ceil(max(0, N - capacity) / cap): 99 people need 0 relays, 100 need 1, 150 need 2, 200 need 3.

More people start relays at once. An extra relay keeps publishing while anyone watches it and stops (ends its Go Live and leaves voice) once it has had no viewer for `runtime.relay_idle_stop_seconds`. A FATAL, quarantined or failing relay is replaced by the next free relay account; when there are not enough usable relay accounts `!relay status` shows a shortage warning.

### Codec

With `codecs.output_codec: adaptive` (the default) every output forwards the target's video unchanged in ONE common codec: the highest of AV1 > H265 > H264 > VP8 > VP9 that the target produces and that every viewer of every output decodes. When a weaker viewer joins, the whole chain drops to a codec that viewer decodes and rises again after that viewer leaves. `!relay status` shows the current codec. Set `output_codec: H264` for the fixed H.264 mode.

### Approved profile manifest

The service only runs a quality profile that was verified live on that exact relay account and server. Copy `manifests/verified-profiles.example.json` to `manifests/verified-profiles.json` (gitignored; it holds your IDs) and edit it:

```json
{
  "schema_version": 1,
  "profiles": [
    {
      "relay_user_id": "YOUR_INGEST_ACCOUNT_ID",
      "guild_id": "YOUR_GUILD_ID",
      "width": 1920,
      "height": 1080,
      "fps": 60,
      "verified_on": "2026-11-01",
      "evidence": "viewer saw 1080P 60FPS and smooth video"
    }
  ]
}
```

`width`, `height` and `fps` must equal `max_*` (auto) or `fixed_*` (fixed). Add the entry after the profile played well in a live run.

## 6. First-time validation

The relay refuses to run while `liveness.source_dead_timeout_seconds` is `null`. Measure it once: let the target share a static screen for at least 10 minutes while the receive-only probe watches it, then calibrate `T_dead` from the probe's JSON log:

```sh
relayctl dev watch --config config.yaml --seconds 660 2> tdead.jsonl
relayctl calibrate-tdead tdead.jsonl                                  # print only
relayctl calibrate-tdead tdead.jsonl --write --config config.yaml     # update manifest + config
```

The formula is `max(3.0, round_up(2 x observed_max_gap + 1.0, 0.5))`. The calibration reads the `event: "stats"` log lines (`streams["<ssrc>/<kind>"].max_gap_s`), including logs written by the Python relay. `--write` updates `manifests/verified-capabilities.json` and your config; without it the value is only printed. Keep the same value in `config.yaml`.

Record the results of live runs by hand in `manifests/verified-capabilities.json` under `live_results`.

## 7. Running the service

Tell the service where the tokens are:

| Where | How |
| --- | --- |
| Development | `RELAY_TOKEN_DIR=secrets` (files named by credential) |
| systemd | `LoadCredential=` (section 8) sets `CREDENTIALS_DIRECTORY` |

```bash
export RELAY_TOKEN_DIR=secrets              # Windows PowerShell: $env:RELAY_TOKEN_DIR = "secrets"
relayctl validate --config config.yaml
relayctl doctor   --config config.yaml
relayctl run      --config config.yaml
```

| Command | What it does |
| --- | --- |
| `relayctl validate --config FILE` | Checks the config and approved profile. No Discord connection. Prints `config valid`, then one `not operable: ...` line per reason the service would refuse to run (null `T_dead`, profile not approved). Exit 1 on an invalid config. |
| `relayctl doctor --config FILE` | Checks the config, state directory and capability manifest. Never publishes. No encoder or decoder probes. Exit 1 if any check fails. |
| `relayctl run --config FILE` | Runs the service. Exit 78 means a config or credential problem (no automatic restart under systemd). Starts disabled on first run. |
| `relayctl status [--state-dir DIR]` | Shows state, enabled flag, FATAL, quarantine, profile. Asks the running service, else reads `status.json` or `state.json`. |
| `relayctl enable [--state-dir DIR]` | Sets `desired_enabled=true`. Does not clear FATAL or quarantine. |
| `relayctl disable [--state-dir DIR]` | Sets `desired_enabled=false` and stops the session. Survives restarts. |
| `relayctl reset-fatal [--state-dir DIR] [--account USER_ID]` | Clears a FATAL latch after you fixed the cause. Without `--account` it clears the ingest account; with it, that output account's latch. Does not enable and does not clear quarantine. |

`--state-dir` must equal `storage.state_directory` (default `/var/lib/discord-relay`).

A fresh install is disabled. Run `relayctl enable` once. The service then relays automatically whenever the target shares in the allowed voice channel. It stops when the target stops sharing, leaves or moves, or when you run `relayctl disable` (or SIGINT/SIGTERM). Runtime state (enabled flag, FATAL latch, quarantine, rejection counter) is in `state.json` inside the state directory; the config is never modified.

Windows note: the control socket is a Unix socket and does not exist on Windows. There, `enable`, `disable` and `reset-fatal` change `state.json` directly and `status` reads the files, so run them while the service is stopped or after it exited.

### Chat commands

With `control.chat_commands: true` (the default) the ingest account reads messages in the configured guild and answers commands typed there:

| Command | What it does |
| --- | --- |
| `!relay` | Shows the command list; public commands are marked |
| `!relay status` | Session state and uptime; target; main channel and member count; relays needed/running/available; each relay and mirror account; pre-join, debounce and output size; cooldowns; last watch time |
| `!relay on` / `!relay off` | Same as `relayctl enable` / `disable` |
| `!relay target <user_id>` | Relays another person. A relay account is refused |
| `!relay channel <channel_id>` | Moves the main voice channel (must be a voice channel of the guild and not a mirror) |
| `!relay mirror <channel_id> [account_id]` | Publishes the same stream in another voice channel with a mirror account |
| `!relay mirror` | Same, for the voice channel you are sitting in (refused in the main channel) |
| `!relay unmirror` / `unmirror <channel_id>` / `unmirror all` | Removes the mirror of your channel, of one channel, or all of them |
| `!relay prejoin on` / `off` | Turns pre-join on or off; stored in `state.json` |
| `!relay autoscale on` / `off` | Turns relay autoscale on or off at once; stored in `state.json` |
| `!relay op list` | Lists the operators. Operators only |
| `!relay op add <user_id>` / `op remove <user_id>` | Adds or removes a chat-added operator. Owners only |

Anyone in the guild may use the commands in `control.public_commands`; every other command needs an operator. Owners are the users in `control.operator_user_ids` and the holders of a role in `control.operator_role_ids`. Commands are accepted only in `control.command_channel_id` when it is set. Non-public commands from non-operators, other guilds or channels, and messages from the relay accounts themselves are ignored without a reply. Give every ID as plain decimal digits; mentions such as `<@123>` are refused. Replies never ping anyone. Message text is never logged.

Changes are written to `routing` in `state.json` and survive restarts; while no routing is stored the `discord.target_user_id` and `discord.allowed_voice_channel_id` from config are used. A stored routing that is invalid (target is a relay account, mirror on an unknown or wrong account, mirror on the main channel) stops the service at start with `state.json routing is invalid`.

## 8. Deploying on a Linux VPS with systemd

Bandwidth: download is the target's stream (about 1 to 8 Mbps); upload is about the target's bitrate per publishing account. Passthrough uses almost no CPU, so a small VPS is enough. Do not run it on the target's streaming PC: its uplink is shared.

Steps (as root), from the unpacked Linux release archive (copy `discord-relay.service` from `deploy/systemd/` of the source repository next to it):

```bash
useradd --system --home /opt/discord-relay --shell /usr/sbin/nologin discord-relay
install -d /opt/discord-relay
install -m 0755 relayctl /opt/discord-relay/relayctl

install -d -m 0750 -o root -g discord-relay /etc/discord-relay /etc/discord-relay/manifests
install -m 0640 -g discord-relay config.yaml /etc/discord-relay/config.yaml
install -m 0640 -g discord-relay manifests/verified-capabilities.json manifests/verified-profiles.json /etc/discord-relay/manifests/
install -m 0600 secrets/relay-token   /etc/discord-relay/token
install -m 0600 secrets/relay-token-2 /etc/discord-relay/token-2
```

The manifests go in `/etc/discord-relay/manifests/` because relative paths in the config are resolved next to the config file. The config should keep `storage.state_directory: /var/lib/discord-relay` (the unit's `StateDirectory=discord-relay`).

The unit has one `LoadCredential` line for the ingest token. Add one line per output account, with the credential name from `relay_accounts` or `mirror_accounts` on the left:

```
LoadCredential=relay-token:/etc/discord-relay/token
LoadCredential=relay-token-2:/etc/discord-relay/token-2
```

The unit sets: user `discord-relay`, `Restart=on-failure`, `RestartPreventExitStatus=78`, `TimeoutStopSec=20`, `LimitCORE=0`, `ReadOnlyPaths=/etc/discord-relay`, `NoNewPrivileges=yes`, and `ExecStart=/opt/discord-relay/relayctl run --config /etc/discord-relay/config.yaml`.

```bash
install -m 0644 discord-relay.service /etc/systemd/system/
systemctl daemon-reload
sudo -u discord-relay /opt/discord-relay/relayctl doctor --config /etc/discord-relay/config.yaml
systemctl enable --now discord-relay
sudo -u discord-relay /opt/discord-relay/relayctl enable --state-dir /var/lib/discord-relay    # first run is disabled
journalctl -u discord-relay -f
```

Run one instance per account. Do not use the same account in another Discord client at the same time.

## 9. Troubleshooting

### Discord errors 2002 / 2003 (reduced quality / bandwidth)

Passthrough sends the target's bitrate unchanged and this build cannot re-encode it lower. Run the relay on a machine that is not the target's streaming PC (separate uplink), publish from fewer accounts on one connection, or ask the target to lower their stream bitrate.

### Stutter checklist

1. Check upload: total = target bitrate x number of publishing accounts, plus the target's own stream if on the same line.
2. Stalls hitting every account at the same moment come from the source arriving late (target's upload), not from the relay.

### FATAL latches and quarantine

| Item | Meaning | What to do |
| --- | --- | --- |
| FATAL `AUTH_REJECTED` | Discord rejected the token | Get a new token, replace the file, `reset-fatal` |
| FATAL `ACCOUNT_OWNERSHIP_UNCERTAIN` | The account's voice session changed (probably used elsewhere) | Make sure nothing else uses the account, `reset-fatal` |
| FATAL `RELAY_REJECTED_BY_SERVER` | 3 relay sessions in a row were kicked or moved | Check channel permissions and moderators, `reset-fatal` |
| QUARANTINE | The account was moved or disconnected. It will not rejoin until the target's current share ends and a new one starts. | Wait for the next share. `disable`/`enable` do not clear it. |

After fixing the cause: `relayctl reset-fatal --state-dir DIR` (add `--account ID` for an output account), then `relayctl enable` if needed.

### Common config errors

| Message | Fix |
| --- | --- |
| `x.y: missing` / `unknown key` | Compare with `config.example.yaml`; names are exact |
| `must be a string of decimal digits` | Quote the ID: `"123..."` |
| `must be even` | `max_width` and `max_height` must be even numbers |
| `video_mode` / `audio_mode` set to `transcode` | Not supported in this build; use `passthrough` |
| `recording_enabled: must be false (not implemented)` | Leave it `false` (same for `include_media`) |
| `not operable: liveness.source_dead_timeout_seconds ...` | Calibrate `T_dead` (section 6) |
| `not operable: profile ... not approved` | Add the profile to `verified-profiles.json` (section 5) |
| `no credential: ...` | Set `RELAY_TOKEN_DIR` (or systemd `LoadCredential`); file name must equal the credential name |

## 10. What is verified live

From `live_results` in `manifests/verified-capabilities.json`: the login (`V-LOGIN`), DAVE (`V-DAVE`) and relay (`V-RELAY`) gates passed live on Windows. Linux and macOS builds are unverified until CI has run on them, and `relayctl run` as a systemd service on a Linux VPS has not been exercised live.

## 11. Project layout and development

```
config.example.yaml       example config (copy to config.yaml)
crates/relay-core/        shared core types and logic
crates/relay-discord/     Discord gateway adapter
crates/relay-voice/       voice connection
crates/relay-media/       passthrough media pipeline
crates/relayctl/          the relayctl binary (service wiring, CLI)
manifests/                verified-capabilities.json (live results, T_dead), build-manifest.json,
                          verified-profiles.example.json
deploy/systemd/           discord-relay.service
tools/                    fixture generators
.github/workflows/        CI (fmt, clippy, test on three OS) and tagged releases
```

Run the tests (no Discord needed):

```bash
cargo test --workspace
```

Pushing a tag `v*` builds `relayctl` for `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc` and `aarch64-apple-darwin` and attaches the archives to a GitHub Release.

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
