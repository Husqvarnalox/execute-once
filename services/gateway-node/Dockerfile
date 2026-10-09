FROM node:25-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM node:25-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY --from=deps --chown=10001:10001 /app/node_modules ./node_modules
COPY --chown=10001:10001 package*.json ./
COPY --chown=10001:10001 src ./src
USER 10001:10001
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD ["node", "-e", "fetch('http://127.0.0.1:3000/health').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"]
CMD ["node", "src/server.js"]
