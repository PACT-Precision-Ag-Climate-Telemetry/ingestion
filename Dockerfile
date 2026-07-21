FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

WORKDIR /src

# Install CA certificates and create a non-root user
RUN apk add --no-cache ca-certificates && \
    adduser -D -g '' appuser

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/ingest .

# Use scratch (an entirely empty image) for the runtime
FROM scratch

WORKDIR /app

# Copy certificates and user data from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd

# Copy the static binary
COPY --from=builder /out/ingest /usr/local/bin/ingest

# Switch to the non-root user
USER appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/ingest"]