FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum* ./
COPY third_party/go-libp2p-pubsub ./third_party/go-libp2p-pubsub
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kpl ./cmd/kpl-master

FROM builder AS test
RUN apk add --no-cache bash coreutils
RUN CGO_ENABLED=0 go test -buildvcs=false ./... -timeout 120s \
    && CGO_ENABLED=0 go test -buildvcs=false github.com/libp2p/go-libp2p-pubsub -run '^Test(KPL|Score)' -timeout 120s

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates tzdata docker-cli iproute2 iproute2-tc \
    && addgroup -S -g 101 kpl \
    && adduser -S -u 100 -G kpl kpl
COPY --from=builder /out/kpl /usr/local/bin/kpl
WORKDIR /var/lib/kpl
RUN mkdir -p /var/lib/kpl/data/current-run /var/lib/kpl/data/runs /var/lib/kpl/agent \
    && chown -R 100:101 /var/lib/kpl
USER 100:101
ENTRYPOINT ["kpl"]
