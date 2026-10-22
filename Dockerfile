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

# No package.json install step: the router has zero runtime dependencies.
COPY package.json server.js ./
COPY public ./public
COPY config.example.json ./

RUN mkdir -p /data && chown -R node:node /data /app
USER node

EXPOSE 8787
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8787)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "server.js"]
