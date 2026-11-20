# Telemetry TCP Payload Format

How to build one telemetry frame that the ingest service accepts. This is the
reference for firmware (`buildTelemetryPayload()` in `tcp.h`) and for anyone
writing a test sender.

To experiment with the layout, open [`packet.html`](packet.html) in a browser.
It builds a frame live, shows the hex with every field color-coded, and
previews the JSON the server would publish.

## At a glance

One upload is one binary frame, sent as a single TCP write to port `8080`:

```text
+----------+--------------------------+----------------+-----------+-----------+
|  Header  |  Historical records      |  Newest record | GPS block | Cell block| Trailer |
|  14 B    |  (count - 1) x 23 / 27 B |  31 / 35 B     | 8 B, opt. | 4 B, opt. | 2 B     |
+----------+--------------------------+----------------+-----------+-----------+---------+
```

- **Byte order:** every multi-byte number is **little-endian**.
- **Record order:** records go **oldest first**, and the last record is the newest.
- **What the flags control:** the flags byte in the header decides the length
  of each record (the RAW bit) and whether the GPS and Cell blocks follow.
- **Record sizes:** "23 / 27" and "31 / 35" mean _without RAW / with RAW_.

## 1. Header (14 bytes)

| Offset | Size | Field        | Type     | Notes                                                                   |
| -----: | ---: | ------------ | -------- | ----------------------------------------------------------------------- |
|      0 |    1 | magic_byte_1 | uint8    | Must equal the server's `magic_byte_1` (e.g. `0x1A`)                    |
|      1 |    1 | magic_byte_2 | uint8    | Must equal the server's `magic_byte_2` (e.g. `0x2B`)                    |
|      2 |    1 | flags        | uint8    | Version and feature bits, see below                                     |
|      3 |   10 | device_id    | 10 ASCII | Exactly 10 bytes. Pad short IDs with `'0'`. Published as `XXX-XXX-XXXX` |
|     13 |    1 | count        | uint8    | Number of records, 1–255 (the firmware sends at most 24)                |

### Flags byte (offset 2)

```text
  bit:   7      6      5      4      3   2   1   0
       +------+------+------+------+---------------+
       | GPS  | CELL | RAW  | rsv  |    version    |
       +------+------+------+------+---------------+
```

| Bit | Mask   | Name     | Meaning when set                                                        |
| --: | ------ | -------- | ----------------------------------------------------------------------- |
| 0–3 | `0x0F` | version  | Protocol version. **Must be `0`.**                                      |
|   4 | `0x10` | reserved | Send `0`. The server ignores it.                                        |
|   5 | `0x20` | RAW      | Every record carries `methane_raw` and `moisture_raw` (+4 B per record) |
|   6 | `0x40` | CELL     | A 4-byte Cell block follows the records                                 |
|   7 | `0x80` | GPS      | An 8-byte GPS block follows the records                                 |

Common values: `0x00` (nothing extra), `0x20` (raw only), `0xA0` (GPS + raw),
`0xE0` (GPS + cell + raw).

## 2. Records

Every record starts with `timedif`. The **newest record** (the last one) also
carries the absolute `timestamp`. After that comes the sensor body, which is
the same in every record.

### Historical record: 23 B, or 27 B with RAW

| Offset | Field       | Type      |
| -----: | ----------- | --------- |
|     +0 | timedif     | uint16    |
|     +2 | sensor body | 21 / 25 B |

### Newest record: 31 B, or 35 B with RAW

| Offset | Field       | Type                 |
| -----: | ----------- | -------------------- |
|     +0 | timedif     | uint16               |
|     +2 | timestamp   | uint64, Unix seconds |
|    +10 | sensor body | 21 / 25 B            |

### Sensor body: 21 B, or 25 B with RAW

|    Offset | Field              | Type   | Encoding                                                                      |
| --------: | ------------------ | ------ | ----------------------------------------------------------------------------- |
|        +0 | temperature        | int16  | °C × 100, **signed** (−327.68 … 327.67). `28.50 °C → 2850`, `−3.75 °C → −375` |
|        +2 | humidity           | uint16 | % × 100. `65.25 % → 6525`                                                     |
|        +4 | methane            | uint16 | ppm                                                                           |
|        +6 | carbon_dioxide     | uint16 | ppm                                                                           |
|        +8 | offset             | uint16 | mm                                                                            |
|       +10 | depth              | uint16 | mm (the firmware calls it `distance`)                                         |
|       +12 | moisture           | uint16 | % × 100                                                                       |
|       +14 | battery_percentage | uint16 | % × 100                                                                       |
|       +16 | battery_voltage    | uint16 | mV                                                                            |
|       +18 | esp_temperature    | int16  | ESP die temperature, °C × 100, **signed**. `41.25 °C → 4125`                  |
|       +20 | methane_raw        | uint16 | raw ADC count. **Only if RAW is set**                                         |
|       +22 | moisture_raw       | uint16 | raw ADC count. **Only if RAW is set**                                         |
| +20 / +24 | status             | uint8  | Sensor-health bits, see below                                                 |

If RAW is clear, `status` comes straight after `esp_temperature`, at +20.

Round to the nearest hundredth before you scale. In C, use
`(int16_t)lroundf(t * 100)`. A plain cast truncates, so it turns `28.499` into
`2849`.

### `timedif`

The server rebuilds each older reading's timestamp by walking backwards from
the newest record's `timestamp`:

```text
ts[count-1] = timestamp
ts[i]       = ts[i+1] - timedif[i]      for i = count-2 … 0
```

- **Historical record:** `timedif` is the number of seconds until the **next
  newer** reading.
- **Newest record:** the firmware writes the gap to the **previous older**
  reading, which is the same value as the last historical record's `timedif`.
  With a single reading it writes `0`. The server publishes this value but does
  not use it.
- **Long gaps:** clamp at `0xFFFF` (65535 s, about 18.2 h). A longer gap
  saturates, and every older reading then comes out too recent.
- **Underflow:** if walking back would go below 0, the server rejects the frame.

### `status` bits

A set bit means that sensor's read failed. The value in the record is then the
previous sample carried over, not a fresh measurement.

| Bit | Mask   | Name                                       |
| --: | ------ | ------------------------------------------ |
|   0 | `0x01` | battery_read_failed                        |
|   1 | `0x02` | ultrasonic_read_failed                     |
|   2 | `0x04` | temperature_read_failed                    |
|   3 | `0x08` | methane_read_failed                        |
|   4 | `0x10` | co2_read_failed                            |
|   5 | `0x20` | moisture_read_failed                       |
| 6–7 |        | unassigned. Published as `unknown_bit_<n>` |

## 3. Optional trailing blocks

These go after the newest record, in this order.

**GPS block (8 B), only if flags bit 7 is set**

| Offset | Field     | Type  | Encoding             |
| -----: | --------- | ----- | -------------------- |
|     +0 | latitude  | int32 | degrees × 10,000,000 |
|     +4 | longitude | int32 | degrees × 10,000,000 |

**Cell block (4 B), only if flags bit 6 is set**

| Offset | Field               | Type                         |
| -----: | ------------------- | ---------------------------- |
|     +0 | mobile_country_code | uint16 (e.g. 520 = Thailand) |
|     +2 | mobile_network_code | uint16. `0` is a valid MNC   |

**Trailer (2 B), always present, always last**

| Offset | Field           | Type  | Encoding                                                                                                                                                                                                                           |
| -----: | --------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|     +0 | signal_strength | int8  | Cellular RSSI in dBm, signed. `-85 dBm → 0xAB`                                                                                                                                                                                     |
|     +1 | reset_reason    | uint8 | ESP-IDF `esp_reset_reason_t`: 0 unknown, 1 power*on, 2 external_pin, 3 software, 4 panic, 5 interrupt_watchdog, 6 task_watchdog, 7 other_watchdog, 8 deep_sleep_wake, 9 brownout. Anything higher is published as `unknown*<code>` |

## 4. Frame size

```text
body     = 21 + (RAW ? 4 : 0)
total    = 14 + (2 + body) × (count − 1) + (10 + body) + (GPS ? 8 : 0) + (CELL ? 4 : 0) + 2
         = 24 + (2 + body) × count + gps + cell
```

| count | no extras | RAW | RAW + GPS | RAW + GPS + CELL |
| ----: | --------: | --: | --------: | ---------------: |
|     1 |        47 |  51 |        59 |               63 |
|     3 |        93 | 105 |       113 |              117 |
|    24 |       576 | 672 |       680 |              684 |

The server reads the 14-byte header first. Then it uses `count` and `flags` to
work out exactly how many more bytes to read. If the frame is shorter, the read
times out after one minute. A frame whose length doesn't match its own header
is rejected.

## 5. Building a frame, step by step

1. Write the header: the two magic bytes, the flags byte, the 10-byte ID, and the count.
2. For each reading except the newest, oldest first, write `timedif` and then the sensor body.
3. For the newest reading, write `timedif`, then the 8-byte `timestamp`, then the sensor body.
4. If GPS is set, append latitude and longitude.
5. If CELL is set, append MCC and MNC.
6. Append the 2-byte trailer: signal_strength (int8 dBm), then reset_reason (uint8).
7. Check the length against the formula in section 4.
8. Send the frame in one write, then wait for the 1-byte ack `0x00`. Clear the
   device buffer only after the ack arrives. No ack means the frame was rejected
   or not published, so keep the buffer and retry on the next upload.

### Reference encoder (Python)

```python
import struct

def build_frame(magic, device_id, readings, raw=True, gps=None, cell=None):
    """readings: oldest -> newest, each a dict with a 'ts' (Unix seconds) key."""
    flags = 0x00 | (0x20 if raw else 0) | (0x40 if cell else 0) | (0x80 if gps else 0)
    out = bytes(magic) + bytes([flags]) + device_id.encode()[:10].ljust(10, b"0") + bytes([len(readings)])

    def sensor(r):
        b = struct.pack("<hHHHHHHHH",
                        round(r["temperature"] * 100), round(r["humidity"] * 100),
                        r["methane"], r["carbon_dioxide"], r["offset"], r["depth"],
                        r["moisture"], r["battery_percentage"], r["battery_voltage"])
        if raw:
            b += struct.pack("<HH", r["methane_raw"], r["moisture_raw"])
        return b + bytes([r["status"]])

    gap = lambda newer, older: min(max(newer - older, 0), 0xFFFF)
    for i, r in enumerate(readings[:-1]):
        out += struct.pack("<H", gap(readings[i + 1]["ts"], r["ts"])) + sensor(r)
    newest = readings[-1]
    prev = gap(newest["ts"], readings[-2]["ts"]) if len(readings) > 1 else 0
    out += struct.pack("<HQ", prev, newest["ts"]) + sensor(newest)

    if gps:
        out += struct.pack("<ii", round(gps[0] * 1e7), round(gps[1] * 1e7))
    if cell:
        out += struct.pack("<HH", *cell)
    return out
```

[`test.py`](test.py) is a runnable sender built the same way:
`python test.py --count 3`, or `--no-raw`, `--no-gps`, `--no-cell`.

## 6. Worked example

This example has 2 readings and flags `0xA0` (GPS + RAW), with no cell block.
The device ID is `TESMBO0001` and the newest timestamp is `1723000000`. The
frame is 80 bytes: `14 + 25 + 33 + 8`.

```text
off  field          bytes                       value
---  -------------  --------------------------  ------------------------------
  0  magic          1A 2B
  2  flags          A0                          GPS | RAW, version 0
  3  device_id      54 45 53 4D 42 4F 30 30 30 31   "TESMBO0001" -> TES-MBO-0001
 13  count          02

     -- record 0 (historical, 25 B) --
 14  timedif        2C 01                       300 s
 16  temperature    22 0B                       2850  -> 28.50 °C
 18  humidity       7D 19                       6525  -> 65.25 %
 20  methane        2D 00                       45 ppm
 22  carbon_dioxide A4 01                       420 ppm
 24  offset         43 00                       67 mm
 26  depth          AC 03                       940 mm
 28  moisture       31 15                       5425  -> 54.25 %
 30  battery_pct    34 21                       8500  -> 85.00 %
 32  battery_volt   74 0E                       3700 mV
 34  methane_raw    7B 00                       123
 36  moisture_raw   41 01                       321
 38  status         00                          all OK

     -- record 1 (newest, 33 B) --
 39  timedif        2C 01                       300 s (gap to previous)
 41  timestamp      C0 E4 B2 66 00 00 00 00     1723000000
 49  temperature    89 FE                       -375  -> -3.75 °C
 51  humidity       FA 19                       6650  -> 66.50 %
 53  methane        32 00                       50 ppm
 55  carbon_dioxide AE 01                       430 ppm
 57  offset         46 00                       70 mm
 59  depth          AA 03                       938 mm
 61  moisture       7C 15                       5500  -> 55.00 %
 63  battery_pct    28 23                       9000  -> 90.00 %
 65  battery_volt   88 0E                       3720 mV
 67  methane_raw    82 00                       130
 69  moisture_raw   4D 01                       333
 71  status         02                          ultrasonic_read_failed

     -- GPS block (8 B) --
 72  latitude       78 0B 33 08                 137563000  -> 13.7563°
 76  longitude      90 5B E7 3B                 1005018000 -> 100.5018°
```

The whole frame as one hex string:

```text
1a2ba05445534d424f30303031022c01220b7d192d00a4014300ac0331153421740e7b004101002c01c0e4b2660000000089fefa193200ae014600aa037c152823880e82004d0102780b3308905be73b
```

The server publishes it as follows. Record 0's timestamp is rebuilt as
`1723000000 − 300`.

```json
{
  "id": "TES-MBO-0001",
  "version": "0",
  "flags": 160,
  "readings": [
    {
      "timestamp": 1722999700,
      "timedif": 300,
      "temperature": 28.5,
      "humidity": 65.25,
      "carbon_dioxide": 420,
      "methane_raw": 123,
      "methane": 45,
      "offset": 67,
      "depth": 940,
      "moisture_raw": 321,
      "moisture": 54.25,
      "battery_voltage": 3700,
      "battery_percentage": 85,
      "status": 0,
      "status_flags": []
    },
    {
      "timestamp": 1723000000,
      "timedif": 300,
      "temperature": -3.75,
      "humidity": 66.5,
      "carbon_dioxide": 430,
      "methane_raw": 130,
      "methane": 50,
      "offset": 70,
      "depth": 938,
      "moisture_raw": 333,
      "moisture": 55,
      "battery_voltage": 3720,
      "battery_percentage": 90,
      "status": 2,
      "status_flags": ["ultrasonic_read_failed"]
    }
  ],
  "has_raw": true,
  "has_gps": true,
  "latitude": 13.7563,
  "longitude": 100.5018,
  "has_cell": false
}
```

When RAW is clear, `methane_raw` and `moisture_raw` are left out of every
reading, and `has_raw` is `false`. See
[`schemas/telemetry-data.schema.json`](schemas/telemetry-data.schema.json).

## 7. Why the server rejects a frame

The server sends no ack and logs the frame to `error_handler_queue` when:

1. the magic bytes don't match the configured `magic_byte_1` / `magic_byte_2`
2. the version (flags bits 0–3) is not `0`
3. `count` is `0`
4. the frame length doesn't exactly match what `count` and `flags` imply. The
   most common cause is a RAW bit that disagrees with the record bodies.
5. a `timedif` would rebuild a timestamp below zero

## Changes from the previous layout

The earlier v0 layout used 34-byte and 42-byte records. A frame in that layout
now fails with `payload size mismatch`.

| Was                                   | Now                                     |
| ------------------------------------- | --------------------------------------- |
| temperature float32 (4 B)             | int16 °C × 100 (2 B)                    |
| humidity float32 (4 B)                | uint16 % × 100 (2 B)                    |
| pressure float32 (4 B)                | removed                                 |
| methane_raw, moisture_raw always sent | sent only when flags bit 5 (RAW) is set |
| error_code uint8                      | removed                                 |
| flags bits 4–5 reserved               | bit 5 = RAW, bit 4 reserved             |
| sensor body 32 B                      | 19 B (23 B with RAW)                    |
