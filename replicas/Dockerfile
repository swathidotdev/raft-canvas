# Multi-stage build so the runtime image doesn't carry python/g++.
# Stage 1: build native deps (better-sqlite3 needs to compile against Node headers).
FROM node:20-alpine AS builder
WORKDIR /app
RUN apk add --no-cache python3 make g++
COPY package*.json ./
RUN npm install --omit=dev

# Stage 2: slim runtime.
FROM node:20-alpine
WORKDIR /app
RUN apk add --no-cache tini
COPY --from=builder /app/node_modules ./node_modules
COPY . .
# Data dir mounted as a volume by docker-compose.
RUN mkdir -p /app/data
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "replicaServer.js"]
