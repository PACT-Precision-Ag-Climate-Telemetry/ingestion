# RabbitMQ Publish Structure

This document describes what this service publishes to RabbitMQ, including routing topology and JSON message schemas.

It also documents the consume-side rejection/dead-letter behavior used for malformed payloads.

## Overview

The service publishes two kinds of messages:

1. Telemetry JSON payloads (parsed sensor data)
2. Error JSON payloads (ingest/parse/publish failures)

## Telemetry Publish

### Routing and exchange details

- Channel: `rabbitJSONCh`
- Exchange type: `topic`
- Exchange durability: `durable=true`
- Exchange name: `pact.telemetry`
- Routing key pattern: `pact.telemetry.{version-major}.{device-id}`
- Content type: `application/json`
- Delivery mode: persistent
- Publisher confirm: enabled (`Confirm(false)` + `NotifyPublish` ack check)

Telemetry exchange is declared lazily on first publish.

### Telemetry routing key naming

Routing keys are generated from parsed telemetry ID and payload version:

- Base format: `pact.telemetry.{version-major}.{device-id}`
- Allowed characters preserved in `{device-id}`: `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`
- Any other character is replaced with `-`
- Empty/blank device id becomes `unknown`
- `{version-major}` is derived from `version` (`major.minor.patch`) by taking the first segment before `.`
- Allowed characters preserved in `{version-major}`: `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`
- Any other character in `{version-major}` is replaced with `-`
- Empty/blank version major becomes `0`

Example:

- Device ID: `ABC-DEF-1234`
- Version: `2.7.1`
- Routing key: `pact.telemetry.2.ABC-DEF-1234`

### Telemetry JSON Schema (Draft 2020-12)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pact.local/schemas/telemetry-data.json",
  "title": "TelemetryData",
  "type": "object",
  "additionalProperties": false,
  "required": ["id", "version", "flags", "readings", "has_gps", "has_cell"],
  "properties": {
    "id": {
      "type": "string",
      "description": "Device ID formatted by parser as XXX-XXX-XXXX"
    },
    "version": {
      "type": "string",
      "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$",
      "description": "major.minor.patch from payload header"
    },
    "flags": {
      "type": "integer",
      "minimum": 0,
      "maximum": 255
    },
    "readings": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "timestamp",
          "carbon_dioxide",
          "methane_raw",
          "methane",
          "level",
          "distance",
          "moisture_raw",
          "moisture",
          "battery_voltage",
          "battery_percentage",
          "status",
          "error_code"
        ],
        "properties": {
          "timestamp": { "type": "integer", "minimum": 0 },
          "carbon_dioxide": {
            "type": "integer",
            "minimum": 0,
            "maximum": 65535
          },
          "methane_raw": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "methane": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "level": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "distance": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "moisture_raw": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "moisture": { "type": "integer", "minimum": 0, "maximum": 65535 },
          "battery_voltage": {
            "type": "integer",
            "minimum": 0,
            "maximum": 65535
          },
          "battery_percentage": {
            "type": "integer",
            "minimum": 0,
            "maximum": 65535
          },
          "status": { "type": "integer", "minimum": 0, "maximum": 255 },
          "error_code": { "type": "integer", "minimum": 0, "maximum": 255 }
        }
      }
    },
    "has_gps": { "type": "boolean" },
    "latitude": { "type": "number" },
    "longitude": { "type": "number" },
    "has_cell": { "type": "boolean" },
    "mobile_country_code": {
      "type": "integer",
      "minimum": 0,
      "maximum": 65535
    },
    "mobile_network_code": { "type": "integer", "minimum": 0, "maximum": 65535 }
  },
  "allOf": [
    {
      "if": {
        "properties": { "has_gps": { "const": true } },
        "required": ["has_gps"]
      },
      "then": { "required": ["latitude", "longitude"] }
    },
    {
      "if": {
        "properties": { "has_cell": { "const": true } },
        "required": ["has_cell"]
      },
      "then": { "required": ["mobile_country_code", "mobile_network_code"] }
    }
  ]
}
```

## Error Publish

### Routing details

- Channel: `rabbitErrorCh`
- Exchange: default exchange `""`
- Routing key: `error_handler_queue`
- Queue: `error_handler_queue` (declared durable)
- Content type: `application/json`
- Delivery mode: persistent

Note: error messages are published directly to a queue via the default exchange, not to a custom topic/fanout exchange.

## Consume and Dead-Letter Flow

### Main ingest queue

The configured ingest queue (`RABBITMQ_QUEUE`) is declared with dead-letter settings:

- `x-dead-letter-exchange`: `""` (default exchange)
- `x-dead-letter-routing-key`: `failed_messages_queue`

This means rejected messages are dead-lettered to `failed_messages_queue`.

### Failed queue

- Queue: `failed_messages_queue`
- Durable: `true`
- Purpose: capture rejected messages for alerting/inspection

### Reject behavior

When the consumer detects malformed JSON payloads (for messages with content type `application/json`) or cannot parse the payload, it:

1. Publishes an error message to `error_handler_queue`.
2. Calls `basic.reject(requeue=false)`.

With the queue DLX settings above, RabbitMQ routes those rejected messages to `failed_messages_queue` automatically. This avoids infinite retries while preserving failed messages for debugging.

### Error JSON payload shape

The service builds the JSON body using string formatting:

```json
{ "source": "...", "error": "...", "payload": "..." }
```

- `source`: logical source of failure (for example `tcp_ingest`, `json_publish`, `rabbitmq_consumer`, `rabbitmq_ack`)
- `error`: error text
- `payload`: raw payload converted to string and JSON-escaped

### Error JSON Schema (Draft 2020-12)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pact.local/schemas/error-message.json",
  "title": "IngestErrorMessage",
  "type": "object",
  "additionalProperties": false,
  "required": ["source", "error", "payload"],
  "properties": {
    "source": { "type": "string", "minLength": 1 },
    "error": { "type": "string", "minLength": 1 },
    "payload": {
      "type": "string",
      "description": "Original binary payload coerced to string and JSON-escaped"
    }
  }
}
```

## Consumer Notes

- Telemetry consumers should bind queues to exchange `pact.telemetry` using topic binding keys (for example `pact.telemetry.*.*` or `pact.telemetry.2.*`).
- Error consumer should consume from `error_handler_queue`.
- Failed-message consumer/alerts should monitor `failed_messages_queue`.
- For binary payload forensics, decode `payload` carefully because it is a lossy string representation of bytes.

## Least-Privilege RabbitMQ Permissions

Grant only the permissions this service needs.

### Resources used by this service

- Exchange: `pact.telemetry`
- Queue: `RABBITMQ_QUEUE` (ingest queue consumed by this service)
- Queue: `failed_messages_queue`
- Queue: `error_handler_queue`
- Default exchange: `""` (used when publishing directly to `error_handler_queue`)

### Required permissions by operation

- Configure:
  - `pact.telemetry` (exchange declare)
  - `RABBITMQ_QUEUE` (passive declare check and optional declare)
  - `failed_messages_queue` (declare)
  - `error_handler_queue` (declare)
- Write:
  - `pact.telemetry` (telemetry publish)
  - default exchange `""` (error publish to `error_handler_queue`)
- Read:
  - `RABBITMQ_QUEUE` (consume + ack/reject)

### Example ACL regexes

Replace `pact_ingest` with your real ingest queue name.

- Configure regex:

```text
^pact\.telemetry$|^pact_ingest$|^failed_messages_queue$|^error_handler_queue$
```

- Write regex:

```text
^pact\.telemetry$|^amq\.default$|^$
```

- Read regex:

```text
^pact_ingest$
```

### Example command

```bash
rabbitmqctl set_permissions -p / ingest_user "^pact\\.telemetry$|^pact_ingest$|^failed_messages_queue$|^error_handler_queue$" "^pact\\.telemetry$|^amq\\.default$|^$" "^pact_ingest$"
```
