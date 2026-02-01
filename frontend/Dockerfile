# syntax=docker/dockerfile:1

FROM node:22.12.0-alpine AS build
WORKDIR /app

RUN npm install -g npm@11.8.0

ARG VITE_API_BASE
ENV VITE_API_BASE=${VITE_API_BASE}

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM node:22.12.0-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production

RUN npm install -g npm@11.8.0 serve

COPY --from=build /app/dist ./dist

EXPOSE 5175
CMD ["serve", "-s", "dist", "-l", "5175"]
