#!/bin/sh
# Installs the latest stable relayctl release for the relay and the dashboard.
# The dashboard restarts at once; the relay gets SIGUSR2 and restarts itself only once idle
# (target not live), so an update never costs the ingest its watch slot.
set -eu

REPO=datpham17903/discord-golive-relay-rs
TARGET=x86_64-unknown-linux-musl
RELAY_BIN=${RELAY_BIN:-/opt/discord-relay-rs/relayctl}
DASH_BIN=${DASH_BIN:-/opt/relay-dashboard/relayctl}
DASH_UNIT=relay-dashboard

version_of() { "$1" --version 2>/dev/null | awk '$1 == "relayctl" {print $2; exit}'; }
# true when $1 is a strictly newer version than $2 (an empty $2 counts as older)
newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n 1)" = "$1" ]; }

tag=$(curl -fsSL --max-time 20 "https://api.github.com/repos/$REPO/releases/latest" |
    sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)
[ -n "$tag" ] || { echo "autoupdate: no release tag found" >&2; exit 1; }
want=${tag#v}

need=""
for bin in "$RELAY_BIN" "$DASH_BIN"; do
    [ -e "$bin" ] || continue
    if newer "$want" "$(version_of "$bin" || true)"; then need="$need $bin"; fi
done
[ -n "$need" ] || exit 0

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
pkg=relayctl-$tag-$TARGET
cd "$tmp"
curl -fsSLO --max-time 300 "https://github.com/$REPO/releases/download/$tag/$pkg.tar.gz"
curl -fsSLO --max-time 60 "https://github.com/$REPO/releases/download/$tag/$pkg.tar.gz.sha256"
sha256sum -c "$pkg.tar.gz.sha256"
tar xzf "$pkg.tar.gz"
[ "$(version_of "$tmp/$pkg/relayctl")" = "$want" ] || { echo "autoupdate: version mismatch" >&2; exit 1; }

for bin in $need; do
    install -m 0755 "$pkg/relayctl" "$bin"
    echo "autoupdate: installed $tag to $bin"
done

case " $need " in *" $DASH_BIN "*)
    systemctl try-restart "$DASH_UNIT" && echo "autoupdate: restarted $DASH_UNIT" ;;
esac

case " $need " in *" $RELAY_BIN "*)
    for pid in $(pgrep -f 'relayctl run' || true); do
        # a shell whose command line merely mentions "relayctl run" also matches
        case $(readlink "/proc/$pid/exe" 2>/dev/null) in
            */relayctl | */relayctl\ \(deleted\)) ;;
            *) continue ;;
        esac
        # the binary the process runs (its file may have been replaced): only a relay that
        # handles SIGUSR2 (>= 0.1.6) may get it; for older ones SIGUSR2 is fatal
        running=$(version_of "/proc/$pid/exe" || true)
        if [ -n "$running" ] && ! newer 0.1.6 "$running"; then
            kill -USR2 "$pid" && echo "autoupdate: relay $pid ($running) will restart into $tag when idle"
        else
            echo "autoupdate: relay $pid runs ${running:-unknown}; restart it by hand to use $tag"
        fi
    done ;;
esac
