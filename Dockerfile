# Dockerfile — jobs service (cron only, no HTTP server)
# Place this file in the ROOT of the jobs repo.

FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --omit=dev

COPY . .

# No EXPOSE — this service has no HTTP port
CMD ["npm", "start"]
