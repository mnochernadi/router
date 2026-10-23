# syntax=docker/dockerfile:1
# Ultra-lightweight LLM API Router — single stage, zero dependencies.
FROM node:20-alpine

ENV NODE_ENV=production \
    PORT=8787 \
    HOST=0.0.0.0 \
    ROUTER_CONFIG=/data/config.json \
    ROUTER_STATE=/data/state.json \
    NODE_OPTIONS="--max-old-space-size=96"

WORKDIR /app

# No package.json install step: the router has zero runtime dependencies, but
# server.js does require these three modules, so they belong in the image.
# Shipping only server.js made every container die with MODULE_NOT_FOUND.
COPY package.json server.js proxy-manager.js proxy-client.js freebuff-driver.js ./
COPY public ./public
COPY scripts ./scripts
COPY config.example.json ./

RUN mkdir -p /data && chown -R node:node /data /app
USER node

# Xray-core inside the image so the exit pool works out of the box. The Go
# binary is static, so the musl base is fine.
USER root
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64)         xa=64 ;; \
      aarch64|arm64)  xa=arm64-v8a ;; \
      *)              xa=64 ;; \
    esac; \
    apk add --no-cache --virtual .xray-dl curl unzip >/dev/null; \
    curl -fsSL -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-${xa}.zip"; \
    mkdir -p /tmp/xray && unzip -o -q /tmp/xray.zip -d /tmp/xray; \
    install -m 0755 /tmp/xray/xray /usr/local/bin/xray; \
    install -m 0644 /tmp/xray/geoip.dat /usr/local/share/xray-geoip.dat 2>/dev/null || true; \
    install -m 0644 /tmp/xray/geosite.dat /usr/local/share/xray-geosite.dat 2>/dev/null || true; \
    rm -rf /tmp/xray /tmp/xray.zip; \
    apk del .xray-dl >/dev/null || true; \
    /usr/local/bin/xray -version
USER node

EXPOSE 8787
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8787)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "server.js"]
