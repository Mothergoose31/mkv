FROM elixir:1.18-otp-26-slim AS builder

ENV MIX_ENV=prod

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    ca-certificates \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mix local.hex --force && \
    mix local.rebar --force

COPY mix.exs mix.lock ./

RUN mix deps.get --only $MIX_ENV
RUN mix deps.compile

COPY lib lib

RUN mix compile
RUN mix release mkv

FROM debian:bullseye-slim AS app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl1.1 \
    libsnappy1v5 \
    zlib1g \
    libbz2-1.0 \
    liblz4-1 \
    libzstd1 \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/_build/prod/rel/mkv ./

ENV MKV_PORT=3000
ENV MKV_DB_PATH=/data/indexdb
ENV MKV_VOLUMES="host.docker.internal:3001,host.docker.internal:3002,host.docker.internal:3003"
ENV MKV_REPLICAS=3
ENV MKV_FALLBACK=""
ENV MKV_PROTECT="false"
ENV MKV_SUBVOLUMES=10
ENV MKV_MODE="server"

EXPOSE ${MKV_PORT}

RUN groupadd --system --gid 1001 appgroup && \
    useradd --system --uid 1001 --gid appgroup appuser

RUN mkdir -p /data/indexdb && chown -R appuser:appgroup /data
USER appuser:appgroup

CMD ["/app/bin/mkv", "start"]