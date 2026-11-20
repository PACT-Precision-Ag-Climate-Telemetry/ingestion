import argparse
import os
import socket
import struct
import time
from typing import Optional


HEADER_SIZE = 14           # magic(2) + flags/version(1) + id(10) + count(1)
SENSOR_BASE_SIZE = 21      # temp, humid, ch4, co2, offset, depth, moisture, batt_p, batt_v, esp_temp (2 each) + status (1)
RAW_FIELDS_SIZE = 4        # methane_raw + moisture_raw, only when FLAG_RAW_PRESENT is set
GPS_BLOCK_SIZE = 8         # latitude(4) + longitude(4)
CELL_BLOCK_SIZE = 4        # mcc(2) + mnc(2)
TRAILER_SIZE = 2           # signal_strength(int8 dBm) + reset_reason(uint8), always last

FLAG_VERSION_MASK = 0x0F
FLAG_RAW_PRESENT = 1 << 5
FLAG_CELL_PRESENT = 1 << 6
FLAG_GPS_PRESENT = 1 << 7


def parse_int(value: Optional[str], default: str) -> int:
    raw = value if value is not None else default
    return int(raw, 0)


def expected_size(count: int, flags: int) -> int:
    sensor = SENSOR_BASE_SIZE + (RAW_FIELDS_SIZE if flags & FLAG_RAW_PRESENT else 0)
    historical = 2 + sensor      # timedif + sensor body
    newest = 2 + 8 + sensor      # timedif + timestamp + sensor body
    size = HEADER_SIZE + newest + historical * (count - 1)
    if flags & FLAG_GPS_PRESENT:
        size += GPS_BLOCK_SIZE
    if flags & FLAG_CELL_PRESENT:
        size += CELL_BLOCK_SIZE
    return size + TRAILER_SIZE


def build_record(is_newest: bool, args: argparse.Namespace, timestamp: int, timedif: int) -> bytes:
    fields = [struct.pack("<H", timedif)]
    if is_newest:
        fields.append(struct.pack("<Q", timestamp))

    fields += [
        struct.pack("<h", round(args.temperature * 100)),  # int16, degC x100
        struct.pack("<H", round(args.humidity * 100)),     # uint16, % x100
        struct.pack("<H", args.methane),
        struct.pack("<H", args.carbon_dioxide),
        struct.pack("<H", args.offset),
        struct.pack("<H", args.depth),
        struct.pack("<H", args.moisture),
        struct.pack("<H", args.battery_percentage),
        struct.pack("<H", args.battery_voltage),
        struct.pack("<h", round(args.esp_temperature * 100)),  # int16, ESP die degC x100
    ]
    if not args.no_raw:
        fields += [
            struct.pack("<H", args.methane_raw),
            struct.pack("<H", args.moisture_raw),
        ]
    fields.append(bytes((args.status,)))
    return b"".join(fields)


def build_payload(args: argparse.Namespace) -> bytes:
    magic_1 = parse_int(os.getenv("magic_byte_1"), "0x1A")
    magic_2 = parse_int(os.getenv("magic_byte_2"), "0x2B")

    device_id = args.device_id.encode("ascii")[:10].ljust(10, b"0")

    if args.version < 0 or args.version > FLAG_VERSION_MASK:
        raise ValueError("--version must fit in the low 4 bits (0-15)")

    flags = args.version & FLAG_VERSION_MASK
    if not args.no_raw:
        flags |= FLAG_RAW_PRESENT
    if not args.no_gps:
        flags |= FLAG_GPS_PRESENT
    if not args.no_cell:
        flags |= FLAG_CELL_PRESENT

    if args.count < 1:
        raise ValueError("--count must be at least 1")
    if args.count > 255:
        raise ValueError("--count must fit in a single byte (<= 255)")

    header = b"".join(
        [
            bytes((magic_1, magic_2)),
            bytes((flags,)),
            device_id,
            bytes((args.count,)),
        ]
    )

    # Records are emitted oldest -> newest. Historical records carry only the
    # timedif (gap in seconds to the next newer record); the newest record
    # carries both timedif and the absolute timestamp.
    records = b"".join(
        build_record(
            is_newest=(i == args.count - 1),
            args=args,
            timestamp=args.timestamp,
            timedif=args.interval_seconds,
        )
        for i in range(args.count)
    )

    gps_block = b""
    if not args.no_gps:
        gps_block = struct.pack("<i", int(round(args.latitude * 10_000_000))) + struct.pack("<i", int(round(args.longitude * 10_000_000)))

    cell_block = b""
    if not args.no_cell:
        cell_block = struct.pack("<H", args.mobile_country_code) + struct.pack("<H", args.mobile_network_code)

    trailer = struct.pack("<bB", args.signal_strength, args.reset_reason)

    body = header + records + gps_block + cell_block + trailer

    expected = expected_size(args.count, flags)
    if len(body) != expected:
        raise ValueError(f"payload must be {expected} bytes, got {len(body)}")

    return body


def send_payload(host: str, port: int, payload: bytes) -> bytes:
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(payload)
        return sock.recv(1024)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and send a TCP telemetry payload")
    parser.add_argument("--host", default=os.getenv("TCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("TCP_PORT", "8080")))
    parser.add_argument("--device-id", default="TESMBO0001")
    parser.add_argument("--version", type=int, default=0)

    parser.add_argument("--count", type=int, default=5, help="number of readings to pack into the message (oldest to newest)")
    parser.add_argument("--interval-seconds", type=int, default=300, help="gap between consecutive readings, used to derive timedif")
    parser.add_argument("--timestamp", type=int, default=int(time.time()), help="absolute timestamp of the newest reading")

    parser.add_argument("--no-gps", action="store_true", help="omit the optional GPS block")
    parser.add_argument("--latitude", type=float, default=13.7563)
    parser.add_argument("--longitude", type=float, default=100.5018)

    parser.add_argument("--no-cell", action="store_true", help="omit the optional cell (MCC/MNC) block")
    parser.add_argument("--mobile-country-code", type=int, default=520)
    parser.add_argument("--mobile-network-code", type=int, default=1)

    parser.add_argument("--signal-strength", type=int, default=-85, help="cellular RSSI in dBm (int8), last-block trailer")
    parser.add_argument("--reset-reason", type=int, default=1, help="esp_reset_reason_t code (uint8), 1=power_on 9=brownout")
    parser.add_argument("--no-raw", action="store_true", help="omit methane_raw/moisture_raw (clears flags bit 5)")
    parser.add_argument("--temperature", type=float, default=28.5, help="degrees C, sent as int16 x100")
    parser.add_argument("--esp-temperature", type=float, default=41.25, help="ESP die temperature in degrees C, sent as int16 x100")
    parser.add_argument("--humidity", type=float, default=65.25, help="percent, sent as uint16 x100")
    parser.add_argument("--carbon-dioxide", type=int, default=420)
    parser.add_argument("--methane-raw", type=int, default=123)
    parser.add_argument("--methane", type=int, default=45)
    parser.add_argument("--offset", type=int, default=67)
    parser.add_argument("--depth", type=int, default=89, help="millimetres")
    parser.add_argument("--moisture-raw", type=int, default=321)
    parser.add_argument("--moisture", type=int, default=5425, help="stored as hundredths of a percent")
    parser.add_argument("--battery-voltage", type=int, default=3700, help="stored as millivolts")
    parser.add_argument("--battery-percentage", type=int, default=8500, help="stored as hundredths of a percent")
    parser.add_argument("--status", type=int, default=0)
    args = parser.parse_args()

    payload = build_payload(args)
    print(f"payload ({len(payload)} bytes, {args.count} reading(s)): {payload.hex()}")

    response = send_payload(args.host, args.port, payload)
    print(f"response: {response.hex()}")


if __name__ == "__main__":
    main()