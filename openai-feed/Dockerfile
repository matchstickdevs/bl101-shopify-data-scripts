# Cloud Run Job image for the daily Node.js scripts.
# ENTRYPOINT is `node`, so the Job selects which script to run via --args,
# e.g. `--args openai-feed.js`. Runs to completion and exits non-zero on failure.
FROM node:22-slim

WORKDIR /app

# Install deps first for layer caching. Copy lockfile + manifest, then npm ci
# (reproducible install from package-lock.json; omit dev deps).
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# App source.
COPY . .

ENTRYPOINT ["node"]
CMD ["openai-feed.js"]
