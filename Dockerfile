FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum* ./
COPY third_party/go-libp2p-pubsub ./third_party/go-libp2p-pubsub
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kpl ./cmd/kpl

FROM builder AS test
RUN apk add --no-cache bash make coreutils
RUN CGO_ENABLED=0 make test-swarm test-go

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates tzdata docker-cli iproute2 iproute2-tc \
    && addgroup -S kpl \
    && adduser -S -G kpl kpl
COPY --from=builder /out/kpl /usr/local/bin/kpl
COPY scripts/swarm-agent.sh /usr/local/bin/kpl-swarm-agent.sh
WORKDIR /var/lib/kpl
RUN mkdir -p /var/lib/kpl/data /var/lib/kpl/agent \
    && chown -R kpl:kpl /var/lib/kpl
USER kpl
ENTRYPOINT ["kpl"]
