#!/usr/bin/env bash
#
# LLM API Router — one-command installer for Linux servers.
#
#   curl -fsSL https://raw.githubusercontent.com/mnochernadi/router/main/install.sh \
#     | sudo bash -s -- --domain router.example.com --email you@example.com
#
#   sudo ./install.sh                 # local checkout, bare metal, port 8787
#   sudo ./install.sh --docker        # force the Docker layout
#   sudo ./install.sh --dry-run       # print every action, change nothing
#   sudo ./install.sh --uninstall     # remove service + unit (keeps your keys)
#
# Idempotent: re-running updates the code and restarts, never rotates your key
# and never overwrites config.json.
#
set -euo pipefail

# ---------------------------------------------------------------------------
# defaults (override with flags or env)
# ---------------------------------------------------------------------------
REPO_URL="${ROUTER_REPO_URL:-https://github.com/mnochernadi/router.git}"
BRANCH="${ROUTER_BRANCH:-main}"
INSTALL_DIR="${ROUTER_INSTALL_DIR:-/opt/llm-router}"
STATE_DIR="${ROUTER_STATE_DIR:-/var/lib/llm-router}"
ENV_FILE="${ROUTER_ENV_FILE:-/etc/llm-router.env}"
SERVICE_NAME="${ROUTER_SERVICE_NAME:-llm-router}"
SERVICE_USER="${ROUTER_SERVICE_USER:-llmrouter}"
PORT="${ROUTER_PORT:-8787}"
DOMAIN=""
EMAIL=""
ADMIN_KEY=""
MODE="auto"          # auto | docker | systemd
WITH_NGINX="auto"    # auto | yes | no
WITH_FIREWALL=1
WITH_XRAY=0
# Xray 25.9.10 added VLESS Encryption and now refuses a plain (no-TLS) VLESS
# outbound whose server is a bare public IP, which is exactly what most public
# V2Ray subscriptions hand out. One rejected outbound aborts the whole config,
# so the pool is unusable on 25.9.10+ until those exits are filtered. Pin the
# last release that accepts them; override with --xray-version or
# ROUTER_XRAY_VERSION=latest.
XRAY_VERSION="${ROUTER_XRAY_VERSION:-v25.9.5}"
DRY_RUN=0
ASSUME_YES=0
UNINSTALL=0
PURGE=0

# ---------------------------------------------------------------------------
# output helpers
# ---------------------------------------------------------------------------
if [ -t 1 ] && [ "${NO_COLOR:-}" = "" ]; then
  C_RESET=$'\033[0m'; C_DIM=$'\033[2m'; C_OK=$'\033[32m'; C_WARN=$'\033[33m'; C_ERR=$'\033[31m'; C_INFO=$'\033[36m'
else
  C_RESET=""; C_DIM=""; C_OK=""; C_WARN=""; C_ERR=""; C_INFO=""
fi

say()  { printf '%s==>%s %s\n' "$C_INFO" "$C_RESET" "$*"; }
ok()   { printf '%s  ok%s %s\n' "$C_OK" "$C_RESET" "$*"; }
warn() { printf '%swarn%s %s\n' "$C_WARN" "$C_RESET" "$*" >&2; }
die()  { printf '%serr %s %s\n' "$C_ERR" "$C_RESET" "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

run() {
  if [ "$DRY_RUN" = 1 ]; then
    printf '%s  [dry-run]%s %s\n' "$C_DIM" "$C_RESET" "$*"
    return 0
  fi
  "$@"
}

usage() {
  cat <<'USAGE'
LLM API Router installer

Usage: sudo ./install.sh [options]

Options:
  --domain <host>      Public hostname. Enables nginx + Let's Encrypt and binds
                       the router to 127.0.0.1 behind the proxy.
  --email <address>    Let's Encrypt contact address (with --domain).
  --port <port>        Router port (default 8787).
  --admin-key <key>    Use this dashboard key instead of generating one.
  --docker             Install with Docker Compose (falls back to systemd if
                       Docker is unavailable).
  --systemd            Install as a systemd service (default when Docker is absent).
  --dir <path>         Install directory (default /opt/llm-router).
  --state-dir <path>   Config/state directory (default /var/lib/llm-router).
  --no-nginx           Never touch nginx/certbot.
  --no-firewall        Never touch ufw/firewalld.
  --with-xray          Download Xray-core into the install dir (enables the V2Ray exit pool).
  --xray-version <tag> Xray release tag (default v25.9.5; "latest" follows upstream).
  --uninstall          Stop and remove the service and unit files.
  --purge              With --uninstall: also delete install and state dirs.
  --dry-run            Print every action without executing it.
  -y, --yes            Non-interactive (no prompts; implies defaults).
  -h, --help           This help.

Examples:
  sudo ./install.sh --domain router.example.com --email me@example.com
  sudo ./install.sh --docker --domain router.example.com
  sudo ./install.sh --port 9000 --no-nginx
USAGE
}

# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------
ORIG_ARGS=("$@")   # kept for the sudo re-exec: parsing below consumes "$@"
while [ $# -gt 0 ]; do
  case "$1" in
    --domain)      DOMAIN="${2:-}"; shift 2 ;;
    --domain=*)    DOMAIN="${1#*=}"; shift ;;
    --email)       EMAIL="${2:-}"; shift 2 ;;
    --email=*)     EMAIL="${1#*=}"; shift ;;
    --port)        PORT="${2:-}"; shift 2 ;;
    --port=*)      PORT="${1#*=}"; shift ;;
    --admin-key)   ADMIN_KEY="${2:-}"; shift 2 ;;
    --admin-key=*) ADMIN_KEY="${1#*=}"; shift ;;
    --docker)      MODE="docker"; shift ;;
    --systemd)     MODE="systemd"; shift ;;
    --dir)         INSTALL_DIR="${2:-}"; shift 2 ;;
    --dir=*)       INSTALL_DIR="${1#*=}"; shift ;;
    --state-dir)   STATE_DIR="${2:-}"; shift 2 ;;
    --state-dir=*) STATE_DIR="${1#*=}"; shift ;;
    --no-nginx)    WITH_NGINX="no"; shift ;;
    --no-firewall) WITH_FIREWALL=0; shift ;;
    --with-xray)   WITH_XRAY=1; shift ;;
    --xray-version) XRAY_VERSION="${2:-}"; shift 2 ;;
    --xray-version=*) XRAY_VERSION="${1#*=}"; shift ;;
    --uninstall)   UNINSTALL=1; shift ;;
    --purge)       PURGE=1; shift ;;
    --dry-run)     DRY_RUN=1; shift ;;
    -y|--yes)      ASSUME_YES=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             printf 'unknown option: %s\n\n' "$1" >&2; usage; exit 2 ;;
  esac
done

[ -n "$INSTALL_DIR" ] || die "--dir cannot be empty"
case "$PORT" in (*[!0-9]*|"") die "--port must be a number" ;; esac
[ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || die "--port out of range"
[ "$WITH_NGINX" = "no" ] || [ -n "$DOMAIN" ] || WITH_NGINX="no"

# ---------------------------------------------------------------------------
# root + OS
# ---------------------------------------------------------------------------
if [ "$DRY_RUN" = 0 ] && [ "$(id -u)" != "0" ]; then
  if have sudo; then
    say "re-running with sudo"
    exec sudo -E bash "$0" "${ORIG_ARGS[@]}"
  fi
  die "run as root (or install sudo)"
fi

OS_ID="unknown"; OS_LIKE=""; PKG="unknown"
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  OS_ID="${ID:-unknown}"; OS_LIKE="${ID_LIKE:-}"
fi
case " $OS_ID $OS_LIKE " in
  *debian*|*ubuntu*)   PKG="apt" ;;
  *fedora*|*rhel*|*centos*|*rocky*|*almalinux*|*amzn*) PKG="dnf" ;;
  *alpine*)            PKG="apk" ;;
  *arch*|*manjaro*)    PKG="pacman" ;;
  *suse*|*opensuse*)   PKG="zypper" ;;
  *) PKG="unknown" ;;
esac
say "install directory : $INSTALL_DIR"
say "state directory   : $STATE_DIR"
say "detected distro   : $OS_ID (package manager: $PKG), dry-run: $DRY_RUN"

pkg_install() {
  [ "$#" -gt 0 ] || return 0
  case "$PKG" in
    apt)    run env DEBIAN_FRONTEND=noninteractive apt-get update -qq; run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@" ;;
    dnf)    run dnf install -y -q "$@" ;;
    apk)    run apk add --no-cache "$@" ;;
    pacman) run pacman -Sy --noconfirm --needed "$@" ;;
    zypper) run zypper --non-interactive install "$@" ;;
    *)      warn "cannot install $* automatically (unknown package manager) — install them by hand" ;;
  esac
}

node_major() { node -v 2>/dev/null | sed 's/^v//' | cut -d. -f1; }

install_node() {
  local m=0
  have node && m="$(node_major 2>/dev/null || echo 0)"
  case "$m" in (*[!0-9]*|"") m=0 ;; esac
  if [ "$m" -ge 20 ]; then
    ok "node $(node -v) present"
    return 0
  fi
  say "installing Node.js 20 LTS"
  case "$PKG" in
    apt)
      run env DEBIAN_FRONTEND=noninteractive apt-get update -qq
      run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ca-certificates curl gnupg
      if [ "$DRY_RUN" = 1 ]; then
        run sh -c 'curl -fsSL https://deb.nodesource.com/setup_20.x | bash -'
      else
        curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null
      fi
      run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nodejs
      ;;
    dnf)    run sh -c 'curl -fsSL https://rpm.nodesource.com/setup_20.x | bash -'; run dnf install -y -q nodejs ;;
    apk)    run apk add --no-cache nodejs npm ;;
    pacman) run pacman -Sy --noconfirm --needed nodejs npm ;;
    zypper) run zypper --non-interactive install nodejs20 ;;
    *)      die "install Node.js >= 20 manually, then re-run" ;;
  esac
}

# ---------------------------------------------------------------------------
# uninstall
# ---------------------------------------------------------------------------
if [ "$UNINSTALL" = 1 ]; then
  say "uninstalling $SERVICE_NAME"
  if have systemctl; then
    run systemctl disable --now "$SERVICE_NAME" || true
    run rm -f "/etc/systemd/system/$SERVICE_NAME.service"
    run systemctl daemon-reload || true
  fi
  if have docker && [ -f "$INSTALL_DIR/docker-compose.yml" ]; then
    run docker compose -f "$INSTALL_DIR/docker-compose.yml" down || true
  fi
  if [ "$PURGE" = 1 ]; then
    warn "purging $INSTALL_DIR and $STATE_DIR (including API keys)"
    run rm -rf "$INSTALL_DIR" "$STATE_DIR" "$ENV_FILE"
  else
    ok "kept $STATE_DIR/config.json and $ENV_FILE — delete them yourself with --purge"
  fi
  ok "uninstalled"
  exit 0
fi

# ---------------------------------------------------------------------------
# preflight: base tools
# ---------------------------------------------------------------------------
need_tools=()
have git  || need_tools+=(git)
have curl || need_tools+=(curl)
if [ "${#need_tools[@]}" -gt 0 ]; then
  say "installing: ${need_tools[*]}"
  pkg_install "${need_tools[@]}"
fi
have git || [ "$DRY_RUN" = 1 ] || die "git is required"

# ---------------------------------------------------------------------------
# pick mode
# ---------------------------------------------------------------------------
if [ "$MODE" = "auto" ]; then
  if have docker && docker compose version >/dev/null 2>&1; then MODE="docker"; else MODE="systemd"; fi
fi
say "install mode      : $MODE"

# ---------------------------------------------------------------------------
# fetch / update the code
# ---------------------------------------------------------------------------
fetch_code() {
  if [ -d "$INSTALL_DIR/.git" ] && [ "$DRY_RUN" = 0 ]; then
    say "updating existing checkout in $INSTALL_DIR"
    if [ -n "$(git -C "$INSTALL_DIR" status --porcelain)" ]; then
      warn "local edits in $INSTALL_DIR are being replaced by origin/$BRANCH (mixed checkouts are what caused the last bad build)"
    fi
    # Hard reset, not pull: a hand-edited file must never leave the box in a
    # state that matches no commit, which is undebuggable.
    run git -C "$INSTALL_DIR" fetch --quiet --prune origin "$BRANCH"
    run git -C "$INSTALL_DIR" checkout --quiet --force "$BRANCH"
    run git -C "$INSTALL_DIR" reset --quiet --hard "origin/$BRANCH"
    say "now at $(git -C "$INSTALL_DIR" rev-parse --short HEAD)"
  elif [ -d "$INSTALL_DIR/.git" ]; then
    run git -C "$INSTALL_DIR" fetch --quiet origin "$BRANCH"
    run git -C "$INSTALL_DIR" reset --quiet --hard "origin/$BRANCH"
  else
    say "cloning $REPO_URL into $INSTALL_DIR"
    run mkdir -p "$(dirname "$INSTALL_DIR")"
    run git clone --quiet --branch "$BRANCH" "$REPO_URL" "$INSTALL_DIR"
  fi
}

# ---------------------------------------------------------------------------
# admin key + env file
# ---------------------------------------------------------------------------
gen_key() {
  if have openssl; then openssl rand -hex 24
  else head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; fi
}

resolve_admin_key() {
  if [ -n "$ADMIN_KEY" ]; then return 0; fi
  if [ -f "$ENV_FILE" ]; then
    ADMIN_KEY="$(sed -n 's/^ADMIN_KEY=//p' "$ENV_FILE" | head -n1)"
    [ -n "$ADMIN_KEY" ] && { ok "reusing ADMIN_KEY from $ENV_FILE"; return 0; }
  fi
  ADMIN_KEY="$(gen_key)"
}

write_env_file() {
  write_file "$ENV_FILE" 600 <<EOF
# LLM API Router environment — created by install.sh
ADMIN_KEY=$ADMIN_KEY
PORT=$PORT
HOST=$BIND_HOST
ROUTER_CONFIG=$STATE_DIR/config.json
ROUTER_STATE=$STATE_DIR/state.json
# Host port for the docker-compose mapping (ignored by the systemd unit).
ROUTER_PORT=$PORT
ROUTER_XRAY=$XRAY_BIN
EOF
}

# ---------------------------------------------------------------------------
# file writer (honours --dry-run)
# ---------------------------------------------------------------------------
write_file() {
  local path="$1" mode="${2:-644}"
  if [ "$DRY_RUN" = 1 ]; then
    say "would write $path (mode $mode):"
    sed 's/^/    | /'
  else
    mkdir -p "$(dirname "$path")"
    cat > "$path"
    chmod "$mode" "$path"
    ok "wrote $path"
  fi
}

# ---------------------------------------------------------------------------
# systemd layout
# ---------------------------------------------------------------------------
install_systemd() {
  BIND_HOST="127.0.0.1"
  [ "$WITH_NGINX" = "no" ] && BIND_HOST="0.0.0.0"

  if ! have systemctl; then
    warn "systemd is not available on this host"
    if [ "$DRY_RUN" = 0 ]; then die "use --docker on a system without systemd"; fi
  fi

  if [ "$DRY_RUN" = 0 ]; then
    id -u "$SERVICE_USER" >/dev/null 2>&1 || run useradd --system --home "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
  else
    run useradd --system --home "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
  fi

  install_node
  local node_bin; node_bin="$(command -v node || echo /usr/bin/node)"

  fetch_code

  # Key first: config.json, the env file and the final summary all use it.
  resolve_admin_key
  if [ "$WITH_XRAY" = 1 ]; then install_xray; fi
  write_env_file

  run mkdir -p "$STATE_DIR"
  if [ "$DRY_RUN" = 0 ]; then
    if [ ! -f "$STATE_DIR/config.json" ]; then
      run cp "$INSTALL_DIR/config.example.json" "$STATE_DIR/config.json"
      run sed -i "s/\"adminKey\": \"changeme\"/\"adminKey\": \"$ADMIN_KEY\"/" "$STATE_DIR/config.json"
      ok "created $STATE_DIR/config.json"
    else
      ok "keeping existing $STATE_DIR/config.json"
    fi
    # It holds the admin key and every provider key: 0600 like the other secrets.
    run chmod 600 "$STATE_DIR/config.json"
    run chown -R "$SERVICE_USER:$SERVICE_USER" "$STATE_DIR" "$INSTALL_DIR"
  else
    run cp "$INSTALL_DIR/config.example.json" "$STATE_DIR/config.json"
    run sed -i "s/\"adminKey\": \"changeme\"/\"adminKey\": \"$ADMIN_KEY\"/" "$STATE_DIR/config.json"
    run chown -R "$SERVICE_USER:$SERVICE_USER" "$STATE_DIR" "$INSTALL_DIR"
  fi

  say "writing systemd unit /etc/systemd/system/$SERVICE_NAME.service"
  write_file "/etc/systemd/system/$SERVICE_NAME.service" 644 <<EOF
[Unit]
Description=LLM API Router (multi-account LLM proxy + dashboard)
Documentation=https://github.com/mnochernadi/router
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
Environment=NODE_ENV=production
EnvironmentFile=-$ENV_FILE
ExecStart=$node_bin --max-old-space-size=96 $INSTALL_DIR/server.js
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$STATE_DIR
MemoryMax=128M
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

  run systemctl daemon-reload
  run systemctl enable --now "$SERVICE_NAME"
  if [ "$DRY_RUN" = 0 ]; then run systemctl restart "$SERVICE_NAME"; fi
}

# ---------------------------------------------------------------------------
# docker layout
# ---------------------------------------------------------------------------
install_docker() {
  BIND_HOST="0.0.0.0"
  [ "$STATE_DIR" = "/var/lib/llm-router" ] || warn "--state-dir is ignored in docker mode: config lives in the router-data volume"
  if ! have docker; then
    if [ "$ASSUME_YES" = 1 ] || [ "$DRY_RUN" = 1 ]; then
      warn "docker is not installed — installing it from get.docker.com"
      run sh -c 'curl -fsSL https://get.docker.com | sh'
      run systemctl enable --now docker
    else
      die "docker is not installed; install it, or run without --docker"
    fi
  fi
  [ "$DRY_RUN" = 0 ] && { docker compose version >/dev/null 2>&1 || die "the docker compose plugin is required"; }

  # No Node.js needed on the host: the image ships its own runtime.
  fetch_code
  resolve_admin_key
  write_env_file

  say "starting containers"
  run docker compose -f "$INSTALL_DIR/docker-compose.yml" --env-file "$ENV_FILE" up -d --build
}

# ---------------------------------------------------------------------------
# health wait
# ---------------------------------------------------------------------------
wait_healthy() {
  if [ "$DRY_RUN" = 1 ]; then
    run curl -fsS "http://127.0.0.1:$PORT/health"
    return 0
  fi
  local i
  for i in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
      ok "router is answering on 127.0.0.1:$PORT"
      return 0
    fi
    sleep 2
  done
  warn "no answer from 127.0.0.1:$PORT yet — check: journalctl -u $SERVICE_NAME -n 50 --no-pager"
  return 1
}

# ---------------------------------------------------------------------------
# nginx + TLS
# ---------------------------------------------------------------------------
configure_nginx() {
  [ "$WITH_NGINX" = "no" ] && return 0
  [ -n "$DOMAIN" ] || return 0

  say "configuring nginx for $DOMAIN"
  have nginx || pkg_install nginx

  # Debian/Ubuntu use sites-available/sites-enabled; RHEL family uses conf.d.
  local nginx_conf nginx_link=""
  if [ -d /etc/nginx/sites-available ]; then
    nginx_conf="/etc/nginx/sites-available/$SERVICE_NAME"
    nginx_link="/etc/nginx/sites-enabled/$SERVICE_NAME"
  else
    nginx_conf="/etc/nginx/conf.d/$SERVICE_NAME.conf"
  fi

  write_file "$nginx_conf" 644 <<EOF
upstream llm_router {
    server 127.0.0.1:$PORT;
    keepalive 16;
}

server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;

    client_max_body_size 32m;

    location / {
        proxy_pass http://llm_router;
        proxy_http_version 1.1;
        proxy_set_header Host              \$host;
        proxy_set_header X-Real-IP         \$remote_addr;
        proxy_set_header X-Forwarded-For   \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Connection        "";

        # SSE must not be buffered
        proxy_buffering off;
        proxy_cache off;
        proxy_request_buffering off;
        chunked_transfer_encoding on;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
EOF

  if [ -n "$nginx_link" ]; then
    run ln -sf "$nginx_conf" "$nginx_link"
    [ "$DRY_RUN" = 1 ] || rm -f /etc/nginx/sites-enabled/default 2>/dev/null || true
  fi
  if [ "$DRY_RUN" = 0 ]; then
    if nginx -t >/dev/null 2>&1; then ok "nginx config valid"; else warn "nginx -t failed — inspect $nginx_conf"; fi
    systemctl reload nginx 2>/dev/null || systemctl restart nginx 2>/dev/null || true
  else
    run nginx -t
    run systemctl reload nginx
  fi

  say "requesting a Let's Encrypt certificate for $DOMAIN"
  have certbot || pkg_install certbot python3-certbot-nginx
  if [ "$DRY_RUN" = 1 ]; then
    run certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --redirect
    return 0
  fi
  if have certbot; then
    local cert_args=(--nginx -d "$DOMAIN" --non-interactive --agree-tos --redirect)
    if [ -n "$EMAIL" ]; then cert_args+=(-m "$EMAIL"); else cert_args+=(--register-unsafely-without-email); fi
    if certbot "${cert_args[@]}"; then ok "TLS enabled"; else warn "certbot failed (DNS pointing elsewhere?) — DNS must resolve to this server before retrying"; fi
  else
    warn "certbot unavailable — install it and run: certbot --nginx -d $DOMAIN"
  fi
}

# ---------------------------------------------------------------------------
# Xray-core (optional): powers the V2Ray exit pool used for IP rotation
# ---------------------------------------------------------------------------
XRAY_BIN="${ROUTER_XRAY:-xray}"
install_xray() {
  local xa
  case "$(uname -m)" in
    x86_64|amd64)  xa=64 ;;
    aarch64|arm64) xa=arm64-v8a ;;
    *)             xa=64 ;;
  esac
  if have xray; then XRAY_BIN="$(command -v xray)"; ok "xray already installed at $XRAY_BIN"; return 0; fi
  local xray_url
  if [ "$XRAY_VERSION" = "latest" ]; then
    xray_url="https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-${xa}.zip"
  else
    xray_url="https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/Xray-linux-${xa}.zip"
  fi
  say "downloading Xray-core $XRAY_VERSION ($xa)"
  run mkdir -p "$INSTALL_DIR/bin"
  if [ "$DRY_RUN" = 1 ]; then
    run sh -c "curl -fsSL -o /tmp/xray.zip $xray_url"
    run sh -c "unzip -o -q /tmp/xray.zip -d $INSTALL_DIR/bin"
    XRAY_BIN="$INSTALL_DIR/bin/xray"
    return 0
  fi
  have unzip || pkg_install unzip
  curl -fsSL -o /tmp/xray.zip "$xray_url" || die "could not download Xray-core $XRAY_VERSION"
  mkdir -p "$INSTALL_DIR/bin"
  unzip -o -q /tmp/xray.zip -d "$INSTALL_DIR/bin"
  chmod +x "$INSTALL_DIR/bin/xray"
  rm -f /tmp/xray.zip
  XRAY_BIN="$INSTALL_DIR/bin/xray"
  ok "xray installed: $XRAY_BIN"
}

# ---------------------------------------------------------------------------
# firewall
# ---------------------------------------------------------------------------
configure_firewall() {
  [ "$WITH_FIREWALL" = 1 ] || return 0
  if have ufw && ufw status 2>/dev/null | grep -q "Status: active"; then
    say "opening ufw ports"
    if [ -n "$DOMAIN" ]; then run ufw allow 80/tcp; run ufw allow 443/tcp
    else run ufw allow "$PORT/tcp"; fi
  elif have firewall-cmd && firewall-cmd --state >/dev/null 2>&1; then
    say "opening firewalld ports"
    if [ -n "$DOMAIN" ]; then
      run firewall-cmd --permanent --add-service=http
      run firewall-cmd --permanent --add-service=https
    else
      run firewall-cmd --permanent --add-port="$PORT/tcp"
    fi
    run firewall-cmd --reload
  else
    ok "no active ufw/firewalld — nothing to open"
  fi
}

# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
summary() {
  local ip url
  # Every detection below is best-effort: `hostname -I` is Linux-only and must
  # never abort the install (set -e + pipefail would otherwise kill it here).
  ip=""
  if have hostname; then ip="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"; fi
  if [ -z "$ip" ] && have ip; then ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}' || true)"; fi
  [ -n "$ip" ] || ip="<server-ip>"
  if [ -n "$DOMAIN" ]; then url="https://$DOMAIN"; else url="http://$ip:$PORT"; fi

  printf '\n%s────────────────────────────────────────────────────────────%s\n' "$C_DIM" "$C_RESET"
  printf ' %sLLM API Router installed%s\n' "$C_OK" "$C_RESET"
  printf '%s────────────────────────────────────────────────────────────%s\n' "$C_DIM" "$C_RESET"
  printf '  dashboard   : %s/admin\n' "$url"
  printf '  API base    : %s/v1\n' "$url"
  printf '  ADMIN_KEY   : %s\n' "$ADMIN_KEY"
  printf '  install dir : %s\n' "$INSTALL_DIR"
  printf '  config/state: %s\n' "$STATE_DIR"
  printf '  env file    : %s (mode 600)\n' "$ENV_FILE"
  if [ "$MODE" = "systemd" ]; then
    printf '  service     : systemctl status %s · journalctl -u %s -f\n' "$SERVICE_NAME" "$SERVICE_NAME"
    printf '  update      : sudo %s/install.sh   (or: git -C %s pull && sudo systemctl restart %s)\n' "$INSTALL_DIR" "$INSTALL_DIR" "$SERVICE_NAME"
  else
    printf '  service     : docker compose -f %s/docker-compose.yml ps\n' "$INSTALL_DIR"
    printf '  logs        : docker compose -f %s/docker-compose.yml logs -f\n' "$INSTALL_DIR"
    printf '  update      : sudo %s/install.sh --docker\n' "$INSTALL_DIR"
  fi
  printf '%s────────────────────────────────────────────────────────────%s\n' "$C_DIM" "$C_RESET"
  printf '\nNext:\n  1. open %s/admin and unlock with the ADMIN_KEY above\n  2. add a provider, then an account (single key or bulk paste)\n  3. point clients at %s/v1\n\n' "$url" "$url"
  if [ -z "$DOMAIN" ]; then
    printf '%sSECURITY:%s no --domain was given, so the router listens on 0.0.0.0:%s over plain HTTP.\n' "$C_WARN" "$C_RESET" "$PORT"
    printf '      The admin key and router keys travel in cleartext. Re-run with --domain <host> --email <you@example.com>\n'
    printf '      to bind loopback and put nginx + Let%ss Encrypt in front.\n\n' "'"
  fi
}

# ---------------------------------------------------------------------------
# go
# ---------------------------------------------------------------------------
install_node   # systemd mode needs the runtime on the host; docker mode does not
if [ "$MODE" = "docker" ]; then install_docker; else install_systemd; fi
wait_healthy || true
configure_nginx
configure_firewall
summary
