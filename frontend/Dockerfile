# syntax=docker/dockerfile:1

FROM node:22-alpine AS deps
WORKDIR /app

RUN npm install -g npm@11.8.0

COPY package.json package-lock.json ./
RUN npm ci

FROM deps AS dev
ARG VITE_API_BASE
ENV VITE_API_BASE=${VITE_API_BASE}

COPY . .

EXPOSE 5175
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5175", "--strictPort"]

FROM deps AS build
ARG VITE_API_BASE
ENV VITE_API_BASE=${VITE_API_BASE}

COPY . .
RUN npm run build

FROM caddy:2-alpine AS runner

COPY --from=build /app/dist /srv
COPY Caddyfile /etc/caddy/Caddyfile

EXPOSE 5175
