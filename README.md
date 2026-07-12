# Ingest Service

TCP ingestion service for AgriCarbon telemetry. It accepts a 54-byte binary payload over TCP, parses the sensor fields, prints the parsed data, publishes the parsed JSON to RabbitMQ exchange `pact.telemetry.json`, and forwards failures to `error_handler_queue`.

## What it does

- Listens on TCP port `8080`
- Parses the fixed telemetry payload format used by the device
- Publishes parsed telemetry as JSON to `pact.telemetry.json`
- Uses RabbitMQ publisher confirms for the JSON publish path
- Publishes parse and transport errors to `error_handler_queue`

## Requirements

- Go 1.22+
- RabbitMQ, if you want the publish path enabled
- Python 3.14+ only if you want to use `test.py` to send sample payloads

## Configuration

The service reads these values from `.env` or the process environment:

- `magic_byte_1` - first payload magic byte, for example `0x1A`
- `magic_byte_2` - second payload magic byte, for example `0x2B`
- `RABBITMQ_URL` - RabbitMQ connection URL
- `RABBITMQ_QUEUE` - queue consumed by the RabbitMQ consumer
- `TCP_HOST` - optional host for the local sender script
- `TCP_PORT` - optional port for the local sender script

Example `.env`:

```env
magic_byte_1=0x1A
magic_byte_2=0x2B
RABBITMQ_URL=amqp://username:password@ip:port/
RABBITMQ_QUEUE=pact_telemetry
```

## Run

Start the service locally:

```bash
go run main.go
```

Build the binary:

```bash
go build -o ingest .
```

Run with Docker:

```bash
docker build -t ingest:latest .
docker run --rm -p 8080:8080 --env-file .env ingest:latest
```

## Test Sender

Use `test.py` to build and send a sample TCP payload:

```bash
python test.py
```

You can override the target and payload fields:

```bash
python test.py --host 127.0.0.1 --port 8080 --device-id ABCDEF1234 --timestamp 1710000000
```

The script prints the payload hex and the server response.

## Payload Layout

The TCP payload is 54 bytes total:

- 2 bytes magic
- 10 bytes device ID
- 3 bytes version
- 8 bytes timestamp
- 4 bytes latitude
- 4 bytes longitude
- 2 bytes carbon dioxide
- 2 bytes methane raw
- 2 bytes methane
- 2 bytes level
- 2 bytes distance
- 2 bytes moisture raw
- 2 bytes moisture
- 2 bytes mobile country code
- 2 bytes mobile network code
- 4 bytes uptime
- 1 byte error code

## Repository Files

- `main.go` - TCP server and RabbitMQ integration
- `test.py` - sample TCP payload sender
- `Dockerfile` - container build
- `.dockerignore` - excludes local-only files from the Docker build context
