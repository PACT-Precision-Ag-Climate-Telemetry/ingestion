# Ingest Service — Data Flow

How one telemetry upload travels from the field unit, through the ingest service, into RabbitMQ.

## Overview

```mermaid
%%{init: {"flowchart": {"curve": "step", "nodeSpacing": 40, "rankSpacing": 60}}}%%
flowchart LR
    SENS("🌡️<br/><b>Sensors</b><br/>Temp · Humid · CO₂<br/>CH₄ · Moisture · Distance")
    GNSS("🛰️<br/><b>GNSS</b>")
    MCU("🧠<br/><b>MCU</b><br/>ESP32-S3")
    MODEM("📶<br/><b>4G Modem</b><br/>SIM7670G")

    READ("📥<br/><b>Read frame</b><br/>TCP :8080")
    PARSE("🔍<br/><b>Parse</b><br/>binary → JSON")
    PUB("📤<br/><b>Publish</b><br/>publisher confirm")

    EX("🔀<br/><b>Exchange</b><br/>pact.telemetry")
    CONS("🖥️<br/><b>Consumers</b><br/>backend · storage")
    ERR("⚠️<br/><b>Errors</b><br/>error_handler_queue")

    SENS --> MCU
    GNSS --> MCU
    MCU --> MODEM
    MODEM -- "binary frame" --> READ
    READ --> PARSE
    PARSE --> PUB
    PUB -- "JSON" --> EX
    EX -- "pact.telemetry.0.&lt;id&gt;" --> CONS
    PUB -. "ack 0x00" .-> MODEM
    READ -. "fail" .-> ERR
    PARSE -. "fail" .-> ERR
    PUB -. "fail" .-> ERR

    classDef box fill:#ffffff,stroke:#000000,stroke-width:1px,color:#000000
    classDef err fill:#ffffff,stroke:#c0392b,stroke-width:1px,color:#000000
    class SENS,GNSS,MCU,MODEM,READ,PARSE,PUB,EX,CONS box
    class ERR err
```

Solid arrows show telemetry moving forward. Dotted arrows show the acknowledgement sent back to the device and the error paths.

## Step by step

| # | Stage | Where | What happens |
|---|-------|-------|--------------|
| 1 | **Collect** | MCU | Sensors are read and averaged each minute, then buffered in flash. |
| 2 | **Upload** | 4G modem | Every 15–30 min the MCU opens a TCP connection to port `8080` and sends one binary frame holding all buffered readings. |
| 3 | **Read frame** | `telemetry.ReadMessage` | Reads the 14-byte header, works out the body length from the record count and GPS/cell flags, then reads exactly that many bytes. Read timeout is 1 min. |
| 4 | **Parse** | `telemetry.Parse` | Checks the magic bytes and version, then decodes each reading into structured telemetry. |
| 5 | **Publish** | `rabbit.PublishTelemetry` | Publishes persistent JSON to topic exchange `pact.telemetry` with routing key `pact.telemetry.<version-major>.<device-id>`, then waits for the RabbitMQ publisher confirm. |
| 6 | **Acknowledge** | `server.handleConnection` | Sends the single byte `0x00` back to the device and closes the connection. The device can now clear its buffer. |
| 7 | **Consume** | Downstream services | Bind queues to `pact.telemetry` and filter with routing keys, for example `pact.telemetry.0.#` for every v0 device. |

If any of steps 3–5 fails, no ack is sent. The error and the raw payload are published to `error_handler_queue`, and the device keeps its buffer to retry on the next upload.

## Queue consumer path

The service also consumes `RABBITMQ_QUEUE` and validates what arrives there.

```mermaid
%%{init: {"flowchart": {"curve": "step"}}}%%
flowchart LR
    Q("📨<br/><b>Ingest queue</b><br/>RABBITMQ_QUEUE")
    CHK("🔍<br/><b>Validate</b><br/>JSON + payload")
    ACK("✅<br/><b>Ack</b>")
    DLQ("🗄️<br/><b>Dead letter</b><br/>failed_messages_queue")
    ERR("⚠️<br/><b>Errors</b><br/>error_handler_queue")

    Q --> CHK
    CHK -- "valid" --> ACK
    CHK -. "reject" .-> DLQ
    CHK -. "reason" .-> ERR

    classDef box fill:#ffffff,stroke:#000000,stroke-width:1px,color:#000000
    classDef err fill:#ffffff,stroke:#c0392b,stroke-width:1px,color:#000000
    class Q,CHK,ACK,DLQ box
    class ERR err
```

Malformed messages are rejected with `requeue=false`, so RabbitMQ dead-letters them to `failed_messages_queue` when the queue has DLX arguments. The reason for the rejection goes to `error_handler_queue`.

## Related docs

- [telemetry.md](telemetry.md) — binary frame layout
- [rabbitmq.md](rabbitmq.md) — exchange, routing keys and JSON schemas
- [ARCHITECTURE.md](ARCHITECTURE.md) — full system architecture
