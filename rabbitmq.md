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
- Exchange name: `RABBITMQ_TELEMETRY_EXCHANGE` (default `pact.telemetry`)
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
- `{version-major}` is derived from `version`; the current parser emits a single numeric string such as `0`
- Allowed characters preserved in `{version-major}`: `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`
- Any other character in `{version-major}` is replaced with `-`
- Empty/blank version major becomes `0`

Example:

- Device ID: `ABC-DEF-1234`
- Version: `0`
- Routing key: `pact.telemetry.0.ABC-DEF-1234`

### Telemetry JSON Schema (Draft 2020-12)

This is a copy of [`schemas/telemetry-data.schema.json`](schemas/telemetry-data.schema.json), which is the canonical definition.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pact.local/schemas/telemetry-data.json",
  "title": "TelemetryData",
  "description": "Parsed telemetry published to the topic exchange RABBITMQ_TELEMETRY_EXCHANGE (default \"pact.telemetry\") with routing key pact.telemetry.{version-major}.{device-id}. Field order and units mirror buildTelemetryPayload() in the device firmware (tcp.h); firmware_parity_test.go enforces that.",
  "type": "object",
  "additionalProperties": false,
  "required": ["id", "version", "flags", "readings", "has_raw", "has_gps", "has_cell", "signal_strength", "reset_reason", "reset_reason_name"],
  "properties": {
    "id": {
      "type": "string",
      "pattern": "^.{3}-.{3}-.{4}$",
      "description": "Device ID: the firmware's 10 raw bytes (up to 6 from DEVICE_MODEL with dashes stripped, then 4 from the short serial, zero-padded) formatted as XXX-XXX-XXXX"
    },
    "version": {
      "type": "string",
      "pattern": "^[0-9]+$",
      "description": "Version nibble (bits 0-3) of the payload flags byte, rendered as a decimal string. Only \"0\" is accepted by the ingest service"
    },
    "flags": {
      "type": "integer",
      "minimum": 0,
      "maximum": 255,
      "description": "Raw flags byte: bits 0-3 version, bit 4 reserved, bit 5 raw ADC fields present, bit 6 cell block present, bit 7 GPS block present"
    },
    "readings": {
      "type": "array",
      "minItems": 1,
      "maxItems": 255,
      "description": "Sensor samples ordered oldest to newest. The firmware batches at most MAX_READINGS_PER_PAYLOAD (24) per message; the parser accepts up to the 255 the count byte allows",
      "items": { "$ref": "#/$defs/reading" }
    },
    "has_raw": {
      "type": "boolean",
      "description": "True when the payload carried methane_raw and moisture_raw in every record (flags bit 5). When false, both fields are absent from every reading"
    },
    "has_gps": {
      "type": "boolean",
      "description": "True when the payload carried a GPS block (flags bit 7)"
    },
    "latitude": {
      "type": "number",
      "minimum": -90,
      "maximum": 90,
      "description": "Degrees; wire value is int32 scaled by 1e7. The firmware computes the scaled value in float32, so expect roughly 0.2-1 m of quantisation error"
    },
    "longitude": {
      "type": "number",
      "minimum": -180,
      "maximum": 180,
      "description": "Degrees; wire value is int32 scaled by 1e7. Same float32 quantisation as latitude"
    },
    "has_cell": {
      "type": "boolean",
      "description": "True when the payload carried a cell block (flags bit 6)"
    },
    "mobile_country_code": { "type": "integer", "minimum": 0, "maximum": 65535 },
    "mobile_network_code": {
      "type": "integer",
      "minimum": 0,
      "maximum": 65535,
      "description": "MNC 0 is a valid network code and is published as 0, not omitted"
    },
    "signal_strength": {
      "type": "integer",
      "minimum": -128,
      "maximum": 127,
      "description": "Cellular signal strength (RSSI) in dBm; wire value is a signed int8 in the always-present trailer"
    },
    "reset_reason": {
      "type": "integer",
      "minimum": 0,
      "maximum": 255,
      "description": "ESP reset reason, esp_reset_reason_t code from the always-present trailer"
    },
    "reset_reason_name": {
      "type": "string",
      "description": "Name for reset_reason: 0 unknown, 1 power_on, 2 external_pin, 3 software, 4 panic, 5 interrupt_watchdog, 6 task_watchdog, 7 other_watchdog, 8 deep_sleep_wake, 9 brownout, ...; codes beyond the table are published as unknown_<code>"
    }
  },
  "allOf": [
    {
      "if": {
        "properties": { "has_raw": { "const": true } },
        "required": ["has_raw"]
      },
      "then": {
        "properties": { "readings": { "items": { "required": ["methane_raw", "moisture_raw"] } } }
      },
      "else": {
        "properties": {
          "readings": {
            "items": {
              "not": { "anyOf": [{ "required": ["methane_raw"] }, { "required": ["moisture_raw"] }] }
            }
          }
        }
      }
    },
    {
      "if": {
        "properties": { "has_gps": { "const": true } },
        "required": ["has_gps"]
      },
      "then": { "required": ["latitude", "longitude"] },
      "else": {
        "not": { "anyOf": [{ "required": ["latitude"] }, { "required": ["longitude"] }] }
      }
    },
    {
      "if": {
        "properties": { "has_cell": { "const": true } },
        "required": ["has_cell"]
      },
      "then": { "required": ["mobile_country_code", "mobile_network_code"] },
      "else": {
        "not": {
          "anyOf": [
            { "required": ["mobile_country_code"] },
            { "required": ["mobile_network_code"] }
          ]
        }
      }
    }
  ],
  "$defs": {
    "reading": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "timestamp",
        "timedif",
        "temperature",
        "humidity",
        "carbon_dioxide",
        "methane",
        "offset",
        "depth",
        "moisture",
        "battery_voltage",
        "battery_percentage",
        "esp_temperature",
        "status",
        "status_flags"
      ],
      "properties": {
        "timestamp": {
          "type": "integer",
          "minimum": 0,
          "description": "Unix seconds. The newest reading carries an absolute timestamp on the wire; older readings are reconstructed by walking timedif backwards from it"
        },
        "timedif": {
          "type": "integer",
          "minimum": 0,
          "maximum": 65535,
          "description": "Raw wire value, passed through unchanged. For every reading except the newest it is the gap in seconds to the NEXT NEWER reading. On the newest reading the firmware instead writes the gap to the PREVIOUS OLDER reading (duplicating that reading's timedif), and 0 when the batch holds a single reading. A value of 65535 may be a saturated clamp rather than a true gap, in which case reconstructed timestamps for older readings are too recent"
        },
        "temperature": {
          "type": "number",
          "minimum": -327.68,
          "maximum": 327.67,
          "description": "Celsius; wire value is int16 centi-degrees divided by 100, so always an exact hundredth"
        },
        "humidity": {
          "type": "number",
          "minimum": 0,
          "maximum": 655.35,
          "description": "Percent; wire value is uint16 centi-percent divided by 100"
        },
        "carbon_dioxide": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "ppm" },
        "methane": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "ppm" },
        "methane_raw": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "Uncalibrated sensor count. Present only when has_raw is true" },
        "offset": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "Millimetres" },
        "depth": {
          "type": "integer",
          "minimum": 0,
          "maximum": 65535,
          "description": "Millimetres. This is the field the firmware calls `distance`; the two names refer to the same wire slot"
        },
        "moisture": {
          "type": "number",
          "minimum": 0,
          "maximum": 655.35,
          "description": "Percent; wire value is uint16 centi-percent divided by 100"
        },
        "moisture_raw": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "Uncalibrated sensor count. Present only when has_raw is true" },
        "battery_voltage": { "type": "integer", "minimum": 0, "maximum": 65535, "description": "Millivolts" },
        "battery_percentage": {
          "type": "number",
          "minimum": 0,
          "maximum": 655.35,
          "description": "Percent; wire value is uint16 centi-percent divided by 100"
        },
        "esp_temperature": {
          "type": "number",
          "minimum": -327.68,
          "maximum": 327.67,
          "description": "Celsius, ESP die (internal chip) temperature; wire value is int16 centi-degrees divided by 100"
        },
        "status": {
          "type": "integer",
          "minimum": 0,
          "maximum": 255,
          "description": "Raw sensor-health bitfield from the firmware's TelemetryStatus namespace. 0 means every sensor read succeeded"
        },
        "status_flags": {
          "type": "array",
          "uniqueItems": true,
          "description": "`status` decoded into names. Empty when status is 0. A set flag means that sensor's read failed and the corresponding value in this reading is a stale carry-over of the previous sample, not a fresh measurement",
          "items": {
            "type": "string",
            "anyOf": [
              {
                "enum": [
                  "battery_read_failed",
                  "ultrasonic_read_failed",
                  "temperature_read_failed",
                  "methane_read_failed",
                  "co2_read_failed",
                  "moisture_read_failed"
                ]
              },
              {
                "pattern": "^unknown_bit_[0-7]$",
                "description": "A status bit the firmware has defined but this service does not name yet"
              }
            ]
          }
        }
      }
    }
  },
  "examples": [
    {
      "id": "ABC-DEF-1234",
      "version": "0",
      "flags": 160,
      "readings": [
        {
          "timestamp": 1756512000,
          "timedif": 300,
          "temperature": 23.1,
          "humidity": 61.5,
          "carbon_dioxide": 412,
          "methane_raw": 1350,
          "methane": 8,
          "offset": 120,
          "depth": 940,
          "moisture_raw": 2100,
          "moisture": 34.5,
          "battery_voltage": 3980,
          "battery_percentage": 87.25,
          "esp_temperature": 41.25,
          "status": 0,
          "status_flags": []
        },
        {
          "timestamp": 1756512300,
          "timedif": 300,
          "temperature": 23.5,
          "humidity": 60.75,
          "carbon_dioxide": 418,
          "methane_raw": 1361,
          "methane": 9,
          "offset": 120,
          "depth": 938,
          "moisture_raw": 2094,
          "moisture": 34.2,
          "battery_voltage": 3975,
          "battery_percentage": 87.1,
          "esp_temperature": 41.5,
          "status": 2,
          "status_flags": ["ultrasonic_read_failed"]
        }
      ],
      "has_raw": true,
      "has_gps": true,
      "latitude": 13.7563,
      "longitude": 100.5018,
      "has_cell": false,
      "signal_strength": -85,
      "reset_reason": 9,
      "reset_reason_name": "brownout"
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

This is a copy of [`schemas/error-message.schema.json`](schemas/error-message.schema.json), which is the canonical definition.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pact.local/schemas/error-message.json",
  "title": "IngestErrorMessage",
  "description": "Failure record published to the default exchange with routing key error_handler_queue.",
  "type": "object",
  "additionalProperties": false,
  "required": ["source", "error", "payload"],
  "properties": {
    "source": {
      "type": "string",
      "enum": ["tcp_ingest", "json_publish", "rabbitmq_consumer", "rabbitmq_ack"],
      "description": "Logical stage that failed"
    },
    "error": {
      "type": "string",
      "description": "Go error text"
    },
    "payload": {
      "type": "string",
      "description": "Original binary payload coerced to a Go string; lossy for non-UTF8 bytes. Empty when no payload was read"
    }
  },
  "examples": [
    {
      "source": "tcp_ingest",
      "error": "payload size mismatch: received 60 bytes, need 90 for count=2 flags=0x80",
      "payload": "\u0000\u0001..."
    }
  ]
}
```

## Consumer Notes

- `status_flags` decodes the firmware's `status` bitfield. When a flag is set, the
  matching value in that reading is a stale carry-over of the previous sample,
  not a fresh measurement - alerting on the value alone will treat a dead sensor
  as a steady one.
- `latitude`/`longitude` are present exactly when `has_gps` is true, and
  `mobile_country_code`/`mobile_network_code` exactly when `has_cell` is true.
  Zero is a legitimate value for all four and is published rather than omitted.
- The newest reading's `timedif` is the gap to the *previous* reading, unlike
  every other reading's. See `telemetry.md`.
- Telemetry consumers should bind queues to exchange `pact.telemetry` using topic binding keys (for example `pact.telemetry.*.*` or `pact.telemetry.2.*`).
- Error consumer should consume from `error_handler_queue`.
- Failed-message consumer/alerts should monitor `failed_messages_queue`.
- For binary payload forensics, decode `payload` carefully because it is a lossy string representation of bytes.

## Least-Privilege RabbitMQ Permissions

Grant only the permissions this service needs.

### Resources used by this service

- Exchange: `RABBITMQ_TELEMETRY_EXCHANGE` (or `pact.telemetry` when unset)
- Queue: `RABBITMQ_QUEUE` (ingest queue consumed by this service)
- Queue: `failed_messages_queue`
- Queue: `error_handler_queue`
- Default exchange: `""` (used when publishing directly to `error_handler_queue`)

### Required permissions by operation

- Configure:
  - `RABBITMQ_TELEMETRY_EXCHANGE` (exchange declare)
  - `RABBITMQ_QUEUE` (passive declare check and optional declare)
  - `failed_messages_queue` (declare)
  - `error_handler_queue` (declare)
- Write:
  - `RABBITMQ_TELEMETRY_EXCHANGE` (telemetry publish)
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
