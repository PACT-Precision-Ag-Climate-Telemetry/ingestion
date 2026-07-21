# Ingest Service

TCP ingestion service for AgriCarbon telemetry. It accepts a variable-length binary payload over TCP, parses sensor readings, publishes parsed JSON to RabbitMQ, and forwards failures to `error_handler_queue`.

## What it does

- Listens on TCP port `8080`
- Parses the current variable-length telemetry payload format used by the device
- Publishes parsed telemetry as JSON to exchange `pact.telemetry` (type `topic`)
- Uses routing key pattern `pact.telemetry.<version-major>.<device-id>`
- Uses RabbitMQ publisher confirms for the JSON publish path
- Publishes parse and transport errors to `error_handler_queue`
- Rejects unsupported major versions (`version_major != 1`) and logs them through the error path

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
- `RABBITMQ_TELEMETRY_EXCHANGE` - optional telemetry JSON exchange name (default: `pact.telemetry`)
- `TCP_HOST` - optional host for the local sender script
- `TCP_PORT` - optional port for the local sender script

RabbitMQ queues created/used by the service:

- `RABBITMQ_QUEUE` - ingest queue consumed by this service
- `failed_messages_queue` - dead-letter target for rejected messages (when DLX args are present)
- `error_handler_queue` - stores error JSON emitted by the service

Note on existing queues: if `RABBITMQ_QUEUE` already exists, the service keeps existing queue arguments as-is and does not force dead-letter arguments.

Example `.env`:

```env
magic_byte_1=0x1A
magic_byte_2=0x2B
RABBITMQ_URL=amqp://username:password@ip:port/
RABBITMQ_QUEUE=pact_telemetry
RABBITMQ_TELEMETRY_EXCHANGE=pact.telemetry
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

To test version validation (server accepts only major version 1):

```bash
python test.py --version-major 2
```

## Payload Layout

The TCP payload is variable length and has this structure:

1. Header (17 bytes)
2. `count` records (oldest to newest)
3. Optional GPS block (8 bytes when flag bit 0 is set)
4. Optional Cell block (4 bytes when flag bit 1 is set)

### Header (17 bytes)

- Byte 0-1: magic bytes
- Byte 2: version major
- Byte 3: version minor
- Byte 4: version patch
- Byte 5-14: device ID (10 bytes)
- Byte 15: flags
- Byte 16: record count

### Records

- Older records (`count-1` entries): 22 bytes each
  - timedif (2 bytes) + fixed sensor/body fields (20 bytes)
- Newest record (last entry): 28 bytes
  - absolute timestamp (8 bytes) + fixed sensor/body fields (20 bytes)

### Size formula

`total = 17 + (22 * (count - 1)) + 28 + gps + cell`

Where:

- `gps = 8` if GPS flag is set, else `0`
- `cell = 4` if Cell flag is set, else `0`

For a deeper byte-by-byte guide, see `telemetry.md`.

## RabbitMQ Publish

- JSON exchange: `RABBITMQ_TELEMETRY_EXCHANGE` (default `pact.telemetry`, type `topic`, durable)
- Routing key: `pact.telemetry.<version-major>.<device-id>`
- Content type: `application/json`
- Delivery mode: persistent
- Publish confirmation: required (publisher confirm)

See `rabbitmq.md` for full queue topology, dead-letter behavior, and message schemas.

## Repository Files

- `main.go` - TCP server and RabbitMQ integration
- `test.py` - sample TCP payload sender
- `telemetry.md` - byte-level payload construction guide
- `rabbitmq.md` - RabbitMQ topology, routing, and error/dead-letter details
- `Dockerfile` - container build
- `.dockerignore` - excludes local-only files from the Docker build context
