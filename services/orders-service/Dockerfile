ARG NODE_IMAGE=node:22-alpine

FROM ${NODE_IMAGE} AS dependencies

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci


FROM dependencies AS build

COPY tsconfig.json tsconfig.build.json ./
COPY src ./src

RUN npm run build


FROM ${NODE_IMAGE} AS production-dependencies

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci --omit=dev \
    && npm cache clean --force


FROM ${NODE_IMAGE} AS runtime

ARG VCS_REF=unknown
ARG VERSION=development
ARG BUILD_SOURCE=local

ENV NODE_ENV=production

LABEL org.opencontainers.image.title="OrderFlow Orders Service" \
      org.opencontainers.image.description="OrderFlow API and worker runtime" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="${BUILD_SOURCE}"

WORKDIR /app

COPY --from=production-dependencies \
    --chown=node:node \
    /app/node_modules \
    ./node_modules

COPY --from=build \
    --chown=node:node \
    /app/dist \
    ./dist

COPY --chown=node:node package.json ./

USER node

EXPOSE 3000

CMD ["node", "dist/server.js"]
