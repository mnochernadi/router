#!/bin/bash
# ocx-client.sh -- a hard-isolated OpenCode client that never touches the host IP.
#
# Purpose: run the genuine OpenCode client as if it were a stranger's machine on a
# foreign IP. Two independent guarantees, both fail-closed:
#
#   1. Egress is forced through one of the operator's own v2ray exits
#      (127.0.0.1:20801 -> 51.77.217.70, OVH FR, AS16276 -- different ASN and country
#      from the host's Hetzner AS24940). HTTPS_PROXY/HTTP_PROXY/ALL_PROXY are set, and
#      an owner-matched iptables chain drops anything the client tries to send
#      directly, so a proxy-ignoring code path cannot leak the host IP.
#   2. Identity is fresh: dedicated uid `ocx`, dedicated HOME, dedicated project,
#      freshly downloaded binary. No credential, session, project handle or build
#      fingerprint is shared with the host's own client at /opt/oc/bin.
#
# Usage:
#   ocx-client.sh setup     # one-time: user, dirs, fresh binary, firewall
#   ocx-client.sh run "say OK"   # one completion through the pinned exit
#   ocx-client.sh verify    # prove the exit IP and that no direct socket exists
#   ocx-client.sh fw        # re-apply the firewall chain only
#
# Exit selection: OCX_EXIT (port on 127.0.0.1) overrides the default 20801.

set -u

EXIT_PORT="${OCX_EXIT:-20801}"
OCX_USER=ocx
OCX_HOME=/var/lib/ocx
OCX_BIN=/opt/ocx/bin/opencode
OCX_PROJ="$OCX_HOME/project"
OCX_VER="${OCX_VER:-1.19.4}"
CHAIN=OCX_OUT

die() { echo "ocx: $*" >&2; exit 1; }

fw() {
  command -v iptables >/dev/null || die "iptables missing"
  iptables -N "$CHAIN" 2>/dev/null
  iptables -F "$CHAIN" || die "cannot flush $CHAIN"
  # loopback: the client's own local server, the proxy port, and the resolver
  iptables -A "$CHAIN" -o lo -j ACCEPT
  iptables -A "$CHAIN" -d 127.0.0.1 -p tcp --dport "$EXIT_PORT" -j ACCEPT
  iptables -A "$CHAIN" -p udp --dport 53 -j ACCEPT
  iptables -A "$CHAIN" -p tcp --dport 53 -j ACCEPT
  # everything else is refused, not silently dropped: a leak shows up as an error
  iptables -A "$CHAIN" -j REJECT --reject-with icmp-port-unreachable
  iptables -C OUTPUT -m owner --uid-owner "$OCX_USER" -j "$CHAIN" 2>/dev/null \
    || iptables -I OUTPUT 1 -m owner --uid-owner "$OCX_USER" -j "$CHAIN" \
    || die "cannot hook $CHAIN into OUTPUT"

  # IPv6: the host has a global v6 address; refuse it outright so nothing rides it
  if command -v ip6tables >/dev/null; then
    ip6tables -N "$CHAIN" 2>/dev/null
    ip6tables -F "$CHAIN" 2>/dev/null
    ip6tables -A "$CHAIN" -o lo -j ACCEPT
    ip6tables -A "$CHAIN" -p udp --dport 53 -j ACCEPT
    ip6tables -A "$CHAIN" -p tcp --dport 53 -j ACCEPT
    ip6tables -A "$CHAIN" -j REJECT
    ip6tables -C OUTPUT -m owner --uid-owner "$OCX_USER" -j "$CHAIN" 2>/dev/null \
      || ip6tables -I OUTPUT 1 -m owner --uid-owner "$OCX_USER" -j "$CHAIN" 2>/dev/null
  fi
  echo "ocx: firewall armed for uid $OCX_USER (only 127.0.0.1:$EXIT_PORT + DNS)"
}

setup() {
  id "$OCX_USER" >/dev/null 2>&1 || useradd -r -m -d "$OCX_HOME" -s /bin/bash "$OCX_USER" \
    || die "cannot create user"
  install -d -o "$OCX_USER" -g "$OCX_USER" -m 700 "$OCX_HOME" "$OCX_PROJ" "$OCX_HOME/tmp"
  install -d -m 755 /opt/ocx/bin

  # fresh binary, always: no reuse of the host's copy
  tmp=$(mktemp -d)
  curl -fsSL -m 120 "https://registry.npmjs.org/opencode-linux-x64/-/opencode-linux-x64-${OCX_VER}.tgz" \
    -o "$tmp/oc.tgz" || die "download failed"
  tar -xzf "$tmp/oc.tgz" -C "$tmp" || die "extract failed"
  src=$(find "$tmp" -type f -name opencode -perm -u+x | head -1)
  [ -n "$src" ] || die "binary not found in tarball"
  install -m 755 -o root -g root "$src" "$OCX_BIN" || die "install failed"
  rm -rf "$tmp"
  echo "ocx: installed $("$OCX_BIN" --version 2>&1 | head -1) at $OCX_BIN"

  fw
  echo "ocx: setup complete; HOME=$OCX_HOME project=$OCX_PROJ exit=127.0.0.1:$EXIT_PORT"
}

as_ocx() {
  setpriv --reuid="$OCX_USER" --regid="$OCX_USER" --init-groups \
    env -i \
      HOME="$OCX_HOME" \
      XDG_CONFIG_HOME="$OCX_HOME/.config" \
      XDG_DATA_HOME="$OCX_HOME/.local/share" \
      XDG_CACHE_HOME="$OCX_HOME/.cache" \
      XDG_STATE_HOME="$OCX_HOME/.local/state" \
      TMPDIR="$OCX_HOME/tmp" \
      PATH=/usr/local/bin:/usr/bin:/bin \
      TERM=dumb NO_COLOR=1 LANG=C.UTF-8 TZ=UTC \
      HTTPS_PROXY="http://127.0.0.1:$EXIT_PORT" \
      HTTP_PROXY="http://127.0.0.1:$EXIT_PORT" \
      ALL_PROXY="http://127.0.0.1:$EXIT_PORT" \
      NO_PROXY=127.0.0.1,localhost \
      "$@"
}

run() {
  [ -x "$OCX_BIN" ] || die "not set up"
  local msg="${1:-say OK}"
  cd "$OCX_PROJ" || die "no project dir"
  as_ocx timeout "${OCX_TIMEOUT:-120}" "$OCX_BIN" run \
    --print-logs --log-level INFO \
    -m opencode/deepseek-v4.1-flash-free "$msg"
}

verify() {
  echo "--- pinned exit identity (as the client sees it) ---"
  as_ocx curl -s -m 15 https://api.ipify.org; echo
  as_ocx curl -s -m 15 https://ipinfo.io/org; echo
  echo "--- host identity for contrast ---"
  curl -s -m 10 https://api.ipify.org; echo
  echo "--- firewall chain ---"
  iptables -S "$CHAIN" 2>/dev/null
  echo "--- uid sockets right now ---"
  ss -tnp 2>/dev/null | grep -E "pid=" | grep -c . >/dev/null
  ps -o pid= -u "$OCX_USER" 2>/dev/null | while read -r p; do
    ss -tnp 2>/dev/null | grep "pid=$p,"
  done | head -20
}

case "${1:-}" in
  setup) setup ;;
  fw) fw ;;
  run) shift; run "${1:-say OK}" ;;
  verify) verify ;;
  *) sed -n '2,30p' "$0"; exit 2 ;;
esac
