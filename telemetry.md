# Telemetry TCP Payload Format

This document explains how to build one complete telemetry payload for the TCP ingest server.

## Overview

A payload is a single binary frame with this structure:

1. Header (14 bytes)
2. Historical records (`count - 1` entries, 34 bytes each)
3. One newest record (42 bytes before optional location/network data)
4. Optional GPS block (8 bytes)
5. Optional Cell block (4 bytes)

Byte order for all numeric fields is little-endian.

## Header (14 bytes)

Layout:

1. Byte 0-1: magic bytes (`magic_byte_1`, `magic_byte_2`)
2. Byte 2: flags/version byte
3. Byte 3-12: device ID (10 bytes)
4. Byte 13: record count

Notes:

1. Version is stored in bits `0..3` of the flags byte and must be `0`.
2. `record count` must be at least `1`.
3. Device ID is exactly 10 bytes on the wire.

## Flags/version byte (byte 2)

Bit mapping:

1. Bits `0..3`: version
2. Bits `4..5`: reserved
3. Bit 6 (`1 << 6`): Cell block present
4. Bit 7 (`1 << 7`): GPS block present

Examples:

1. `0x00`: version 0, no GPS, no Cell
2. `0x40`: version 0, Cell only
3. `0x80`: version 0, GPS only
4. `0xC0`: version 0, GPS and Cell

## Records

Records are encoded oldest to newest.

1. Historical record size: 34 bytes
2. Newest record base size: 42 bytes

Why sizes differ:

1. All records start with `timedif` (2 bytes)
2. The newest record additionally includes absolute `timestamp` (8 bytes)

### Historical record (34 bytes)

1. `timedif` (uint16): seconds to the next newer reading
2. `temperature` (float32, Celsius)
3. `humidity` (float32, percent)
4. `pressure` (float32, hPa)
5. `methane` (uint16, ppm)
6. `carbon_dioxide` (uint16, ppm)
7. `offset` (uint16, mm)
8. `depth` (uint16, mm)
9. `moisture` (uint16, divide by 100 for percent)
10. `battery_percentage` (uint16, divide by 100 for percent)
11. `battery_voltage` (uint16, millivolts)
12. `methane_raw` (uint16)
13. `moisture_raw` (uint16)
14. `status` (uint8)
15. `error_code` (uint8)

### Newest record (42 bytes before optional blocks)

1. `timedif` (uint16)
2. `timestamp` (uint64): absolute Unix timestamp
3. `temperature` (float32, Celsius)
4. `humidity` (float32, percent)
5. `pressure` (float32, hPa)
6. `methane` (uint16, ppm)
7. `carbon_dioxide` (uint16, ppm)
8. `offset` (uint16, mm)
9. `depth` (uint16, mm)
10. `moisture` (uint16, divide by 100 for percent)
11. `battery_percentage` (uint16, divide by 100 for percent)
12. `battery_voltage` (uint16, millivolts)
13. `methane_raw` (uint16)
14. `moisture_raw` (uint16)
15. `status` (uint8)
16. `error_code` (uint8)

## Optional trailing blocks

These blocks appear after all records.

### GPS block (8 bytes, if flag bit 7 set)

1. `latitude` (int32, divide by 10,000,000 for degrees)
2. `longitude` (int32, divide by 10,000,000 for degrees)

### Cell block (4 bytes, if flag bit 6 set)

1. `mobile_country_code` (uint16)
2. `mobile_network_code` (uint16)

## Total payload size

Formula:

`total = 14 + (34 * (count - 1)) + 42 + gps + cell`

Where:

1. `gps = 8` if GPS flag set, else `0`
2. `cell = 4` if Cell flag set, else `0`

Equivalent simplified form:

`total = 22 + 34*count + gps + cell`

## Build steps

1. Build the 14-byte header: magic, flags/version byte, 10-byte device ID, count.
2. Build `count - 1` historical records oldest to newest.
3. Build the final newest record with `timedif`, `timestamp`, and sensor fields.
4. Append GPS data if bit 7 is set.
5. Append Cell data if bit 6 is set.
6. Validate final byte length against the size formula.
7. Send as one TCP write.

## Validation behavior on server

The server rejects payloads when:

1. magic bytes mismatch
2. version is not `0`
3. record count is `0`
4. payload length does not exactly match the expected size for count/flags

Rejected payloads are logged to `error_handler_queue` through the existing error publishing path.

## Example configuration

Example values:

1. version: `0`
2. device ID: `ABCDEF1234` (10 bytes)
3. count: `5`
4. flags: `0xC0` (GPS + Cell, version 0)

Expected size:

`22 + 34*5 + 8 + 4 = 204 bytes`

## Quick test with existing script

Use `test.py` in this repo to generate and send payloads:

```bash
python test.py --device-id ABCDEF1234 --version 0 --count 5
```
