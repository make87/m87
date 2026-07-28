#!/bin/sh
# m87 manual update — safe on small/slow field devices (Pi Zero 2 W on LTE).
#
# Why this exists — the old script could permanently brick a device:
#   /tmp is tmpfs (RAM) and a DIFFERENT filesystem from ~/.local/bin, so
#   `mv /tmp/m87 ~/.local/bin/m87` was not a rename but a 9.3 MB copy onto the
#   live binary. On a slow SD card that leaves the binary partially written for
#   seconds. The script also runs inside the m87-runtime.service cgroup
#   (KillMode=control-group), so any restart SIGTERMs it mid-copy -> truncated
#   binary -> crash loop -> systemd start-limit -> service dead for good. With
#   no containers running there is no watchdog left to reboot the device.
#
# What this script does differently:
#   1. PERSISTENT, version-keyed staging dir on the SAME filesystem as the
#      target binary. Same fs => the install is an atomic rename(): the binary
#      is either fully old or fully new, never partial — even if killed at the
#      worst possible instant. Persistent => a kill/reboot resumes the download
#      instead of re-pulling megabytes over a 10 KB/s link.
#   2. Verifies the archive AND the staged binary before touching the live one.
#   3. Keeps a backup and rolls back automatically if the new binary is bad.
#   4. Restarts via a transient systemd scope, so restarting the runtime cannot
#      kill this script (or a half-finished write).
#   5. nice/ionice so the update cannot starve the runtime on a 4-core Pi.
#
# Usage:
#   sh m87-update.sh [VERSION]         # default: latest known-good below
#   SKIP_RESTART=1 sh m87-update.sh    # install only, don't restart the runtime
#   RATE_LIMIT=20k sh m87-update.sh    # cap download speed (curl --limit-rate)
set -u

VERSION="${1:-0.8.6}"
SKIP_RESTART="${SKIP_RESTART:-0}"
# Optional download rate cap. Useful to keep an update from saturating a shared
# LTE link (and to reproduce slow-link behaviour when testing on fast networks).
RATE_LIMIT="${RATE_LIMIT:-}"
LIMIT_ARG=""
[ -n "$RATE_LIMIT" ] && LIMIT_ARG="--limit-rate $RATE_LIMIT"
# Optional: pin the download to one interface/source IP (e.g. IFACE=ppp0) so an
# update provably uses the metered LTE link and cannot silently fail over to a
# secondary path. Also makes LTE-only testing honest on multi-homed devices.
IFACE="${IFACE:-}"
[ -n "$IFACE" ] && LIMIT_ARG="$LIMIT_ARG --interface $IFACE"
BASE="https://github.com/make87/m87/releases/download/v$VERSION"

case "$(uname -m)" in
  aarch64|arm64) T="aarch64-unknown-linux-musl" ;;
  x86_64|amd64)  T="x86_64-unknown-linux-musl" ;;
  *) echo "unsupported arch: $(uname -m)"; exit 1 ;;
esac
NAME="m87-$T"

BIN="$(command -v m87 2>/dev/null || echo "$HOME/.local/bin/m87")"
BINDIR="$(dirname "$BIN")"
[ -d "$BINDIR" ] || { echo "ERROR: $BINDIR does not exist"; exit 1; }

# Staging dir MUST sit on the same filesystem as $BIN for the atomic rename.
WORK="$BINDIR/.m87-update"
mkdir -p "$WORK" || { echo "ERROR: cannot create $WORK"; exit 1; }

GZ="$WORK/$NAME-$VERSION.gz"   # resumable partial download
STAGE="$WORK/m87-$VERSION"     # decompressed + verified, ready to install
BAK="$WORK/m87-previous"       # rollback copy

echo "m87 update -> v$VERSION"
echo "  target:  $BIN"
echo "  staging: $WORK  (persistent, resumable, same filesystem)"

# Sanity: same filesystem? If not, the install would silently degrade to a
# non-atomic copy — the exact failure mode this script exists to prevent.
if [ "$(stat -c %d "$WORK")" != "$(stat -c %d "$BINDIR")" ]; then
  echo "ERROR: $WORK and $BINDIR are on different filesystems (install would not be atomic)"
  exit 1
fi

# Drop partials for OTHER versions: never splice two releases together, and
# never let the staging dir grow on a small SD card.
for f in "$WORK"/m87-* "$WORK"/"$NAME"-*.gz; do
  [ -e "$f" ] || continue
  case "$f" in
    "$GZ"|"$STAGE"|"$BAK") : ;;
    *) echo "  cleaning stale $(basename "$f")"; rm -f "$f" ;;
  esac
done

# Run the heavy work at low priority so it cannot starve the runtime/network.
NICE=""
command -v nice   >/dev/null 2>&1 && NICE="nice -n 10"
command -v ionice >/dev/null 2>&1 && NICE="ionice -c3 $NICE"

# Resumable download. -C - continues from the bytes already on disk, so a kill,
# reboot or LTE drop costs nothing. --speed-limit/--speed-time abort a STALLED
# transfer (so the loop reconnects) without imposing an overall timeout: at
# ~10 KB/s a 5 MB file legitimately takes ~9 minutes.
dl() { # $1=url $2=out
  i=0
  while [ "$i" -lt 100 ]; do
    if $NICE curl -4 --http1.1 -L -C - -o "$2" -# $LIMIT_ARG \
         --retry 20 --retry-delay 5 --retry-connrefused \
         --speed-limit 300 --speed-time 60 "$1"; then
      return 0
    fi
    i=$((i + 1))
    echo "  ...dropped, resuming in 5s (attempt $i)"
    sleep 5
  done
  echo "ERROR: download did not complete"
  return 1
}

# Reuse an already-staged, already-verified binary (e.g. a previous run was
# killed right before the install step).
# Probe for the .gz asset, distinguishing "asset genuinely absent" (HTTP 404 ->
# fall back to the raw binary) from "network/DNS is down" (curl transport error
# -> retry, then fail loudly). A bare `curl -f` conflates the two, so a flaky
# link silently tricks the script into pulling the ~3x larger raw asset — the
# worst possible outcome on a 10 KB/s connection.
probe_gz() {
  i=0
  while [ "$i" -lt 10 ]; do
    code="$($NICE curl -4 -sL -o /dev/null -w '%{http_code}' -r 0-0 $LIMIT_ARG \
            --max-time 30 "$BASE/$NAME.gz" 2>/dev/null)"
    rc=$?
    if [ "$rc" -eq 0 ]; then
      case "$code" in
        200|206) return 0 ;;  # compressed asset exists
        404)     return 1 ;;  # genuinely absent -> use raw
      esac
    fi
    i=$((i + 1))
    echo "  ...cannot reach release server (curl rc=$rc http=${code:-none}), retry $i/10 in 5s"
    sleep 5
  done
  echo "ERROR: release server unreachable — check DNS/connectivity (nothing was changed)"
  exit 1
}

if [ -x "$STAGE" ] && "$STAGE" --version >/dev/null 2>&1; then
  echo "reusing verified staged binary from a previous run"
else
  if probe_gz; then
    echo "downloading $NAME.gz (compressed)"
    dl "$BASE/$NAME.gz" "$GZ" || exit 1
    # gzip CRC catches a truncated/corrupt partial before we trust it.
    if ! gunzip -t "$GZ" 2>/dev/null; then
      echo "ERROR: archive is corrupt/incomplete — discarding so the next run refetches"
      rm -f "$GZ"
      exit 1
    fi
    $NICE gunzip -c "$GZ" > "$STAGE" || { echo "ERROR: gunzip failed"; rm -f "$STAGE"; exit 1; }
  else
    echo "no .gz asset — downloading raw $NAME"
    dl "$BASE/$NAME" "$STAGE" || exit 1
  fi

  chmod +x "$STAGE"
  # Verify the staged binary RUNS before it is allowed near the live one.
  if ! "$STAGE" --version >/dev/null 2>&1; then
    echo "ERROR: staged binary does not execute — discarding"
    rm -f "$STAGE"
    exit 1
  fi
fi

echo "staged: $("$STAGE" --version 2>&1 | tr '\n' ' ')"

# Backup for rollback, then install atomically.
cp -p "$BIN" "$BAK" 2>/dev/null || echo "  (warning: could not back up current binary)"
mv -f "$STAGE" "$BIN" || { echo "ERROR: install failed"; exit 1; }

# Verify the LIVE binary; roll back if it is somehow bad.
if ! "$BIN" --version >/dev/null 2>&1; then
  echo "ERROR: installed binary does not run — rolling back"
  [ -f "$BAK" ] && mv -f "$BAK" "$BIN" && echo "  rolled back to previous binary"
  exit 1
fi
echo "installed: $("$BIN" --version 2>&1 | tr '\n' ' ')"
rm -f "$GZ"

if [ "$SKIP_RESTART" = "1" ]; then
  echo "SKIP_RESTART=1 -> not restarting. Apply later with: sudo systemctl restart m87-runtime"
  exit 0
fi

# Restart OUTSIDE this script's cgroup. Running `systemctl restart` directly
# would SIGTERM this very script (it lives in m87-runtime.service's cgroup),
# so hand the restart to a transient systemd unit and exit cleanly.
echo "restarting runtime (connection will drop briefly)"
if command -v systemd-run >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  sudo -n systemd-run --collect --unit=m87-post-update-restart --on-active=2 \
       systemctl restart m87-runtime >/dev/null 2>&1 \
    && { echo "restart scheduled in its own scope; device returns in a few seconds"; exit 0; }
fi

echo "  (systemd-run unavailable — falling back; this script may be killed by the restart,"
echo "   which is now harmless: the binary is already installed atomically)"
"$BIN" runtime restart
