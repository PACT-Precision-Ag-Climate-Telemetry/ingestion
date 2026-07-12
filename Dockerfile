FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

LABEL org.opencontainers.image.source https://github.com/PACT-Precision-Ag-Climate-Telemetry/ingestion

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY main.go ./

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/ingest .

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/ingest /usr/local/bin/ingest

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/ingest"]