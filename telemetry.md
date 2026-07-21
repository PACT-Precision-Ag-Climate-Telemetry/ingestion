# Telemetry TCP Payload Format

This document explains how to build one complete telemetry payload for the TCP ingest server.

## Overview

A payload is a single binary frame with this structure:

1. Header (17 bytes)
2. Reading records (count-dependent)
3. Optional GPS block (8 bytes)
4. Optional Cell block (4 bytes)

Byte order for all numeric fields is little-endian.

## Header (17 bytes)

Layout:

1. Byte 0-1: magic bytes (`magic_byte_1`, `magic_byte_2`)
2. Byte 2: version major
3. Byte 3: version minor
4. Byte 4: version patch
5. Byte 5-14: device ID (10 bytes)
6. Byte 15: flags
7. Byte 16: record count

Notes:

1. `version_major` must be `1`. Any other major version is rejected.
2. `record count` must be at least `1`.
3. Device ID is exactly 10 bytes on the wire.

## Flags (byte 15)

Bit mapping:

1. Bit 0 (`1 << 0`): GPS block present
2. Bit 1 (`1 << 1`): Cell block present

Examples:

1. `0x00`: no GPS, no Cell
2. `0x01`: GPS only
3. `0x02`: Cell only
4. `0x03`: GPS and Cell

## Records

Records are encoded oldest to newest.

1. Older record size: 22 bytes
2. Newest record size: 28 bytes

Why sizes differ:

1. Older records start with `timedif` (2 bytes)
2. Newest record starts with absolute timestamp (8 bytes)

### Older record (22 bytes)

1. `timedif` (uint16): seconds to the next newer reading
2. `carbon_dioxide` (uint16)
3. `methane_raw` (uint16)
4. `methane` (uint16)
5. `level` (uint16)
6. `distance` (uint16)
7. `moisture_raw` (uint16)
8. `moisture` (uint16)
9. `battery_voltage` (uint16)
10. `battery_percentage` (uint16)
11. `status` (uint8)
12. `error_code` (uint8)

### Newest record (28 bytes)

1. `timestamp` (uint64): absolute Unix timestamp
2. `carbon_dioxide` (uint16)
3. `methane_raw` (uint16)
4. `methane` (uint16)
5. `level` (uint16)
6. `distance` (uint16)
7. `moisture_raw` (uint16)
8. `moisture` (uint16)
9. `battery_voltage` (uint16)
10. `battery_percentage` (uint16)
11. `status` (uint8)
12. `error_code` (uint8)

## Optional trailing blocks

These blocks appear after all records.

### GPS block (8 bytes, if flag bit 0 set)

1. `latitude` (float32)
2. `longitude` (float32)

### Cell block (4 bytes, if flag bit 1 set)

1. `mobile_country_code` (uint16)
2. `mobile_network_code` (uint16)

## Total payload size

Formula:

`total = 17 + (22 * (count - 1)) + 28 + gps + cell`

Where:

1. `gps = 8` if GPS flag set, else `0`
2. `cell = 4` if Cell flag set, else `0`

Equivalent simplified form:

`total = 23 + 22*count + gps + cell`

## Build steps

1. Build header with version bytes first, then 10-byte device ID.
2. Build `count` records oldest to newest.
3. For records `0..count-2`, write `timedif` + sensor fields.
4. For record `count-1`, write absolute `timestamp` + sensor fields.
5. Append GPS block if enabled.
6. Append Cell block if enabled.
7. Validate final byte length against the size formula.
8. Send as one TCP write.

## Validation behavior on server

The server rejects payloads when:

1. magic bytes mismatch
2. version major is not `1`
3. record count is `0`
4. payload length is shorter than expected for count/flags

Rejected payloads are logged to `error_handler_queue` through the existing error publishing path.

## Example configuration

Example values:

1. version: `1.0.0`
2. device ID: `ABCDEF1234` (10 bytes)
3. count: `5`
4. flags: `0x03` (GPS + Cell)

Expected size:

`23 + 22*5 + 8 + 4 = 145 bytes`

## Quick test with existing script

Use `test.py` in this repo to generate and send payloads:

```bash
python test.py --device-id ABCDEF1234 --version-major 1 --version-minor 0 --version-patch 0 --count 5
```
