import argparse
import os
import socket
import struct
import time
from typing import Optional


HEADER_SIZE = 17          # magic(2) + version(3) + id(10) + flags(1) + count(1)
OLD_RECORD_SIZE = 22       # timedif(2) + fixed body(20)
NEWEST_RECORD_SIZE = 28    # timestamp(8) + fixed body(20)
GPS_BLOCK_SIZE = 8         # latitude(4) + longitude(4)
CELL_BLOCK_SIZE = 4        # mcc(2) + mnc(2)

FLAG_GPS_PRESENT = 1 << 0
FLAG_CELL_PRESENT = 1 << 1


def parse_int(value: Optional[str], default: str) -> int:
    raw = value if value is not None else default
    return int(raw, 0)


def expected_size(count: int, flags: int) -> int:
    size = HEADER_SIZE
    for i in range(count):
        size += NEWEST_RECORD_SIZE if i == count - 1 else OLD_RECORD_SIZE
    if flags & FLAG_GPS_PRESENT:
        size += GPS_BLOCK_SIZE
    if flags & FLAG_CELL_PRESENT:
        size += CELL_BLOCK_SIZE
    return size


def build_record(is_newest: bool, args: argparse.Namespace, timestamp: int, timedif: int) -> bytes:
    time_field = struct.pack("<Q", timestamp) if is_newest else struct.pack("<H", timedif)

    return b"".join(
        [
            time_field,
            struct.pack("<H", args.carbon_dioxide),
            struct.pack("<H", args.methane_raw),
            struct.pack("<H", args.methane),
            struct.pack("<H", args.level),
            struct.pack("<H", args.distance),
            struct.pack("<H", args.moisture_raw),
            struct.pack("<H", args.moisture),
            struct.pack("<H", args.battery_voltage),
            struct.pack("<H", args.battery_percentage),
            bytes((args.status,)),
            bytes((args.error_code,)),
        ]
    )


def build_payload(args: argparse.Namespace) -> bytes:
    magic_1 = parse_int(os.getenv("magic_byte_1"), "0x1A")
    magic_2 = parse_int(os.getenv("magic_byte_2"), "0x2B")

    device_id = args.device_id.encode("ascii")[:10].ljust(10, b"0")
    version = bytes((args.version_major, args.version_minor, args.version_patch))

    flags = 0
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
            version,
            device_id,
            bytes((flags,)),
            bytes((args.count,)),
        ]
    )

    # Records are emitted oldest -> newest. Every record but the last carries
    # a timedif (gap in seconds to the *next*, newer record); the last
    # record carries the absolute timestamp instead.
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
        gps_block = struct.pack("<f", args.latitude) + struct.pack("<f", args.longitude)

    cell_block = b""
    if not args.no_cell:
        cell_block = struct.pack("<H", args.mobile_country_code) + struct.pack("<H", args.mobile_network_code)

    body = header + records + gps_block + cell_block

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
    parser.add_argument("--device-id", default="ABCDEF1234")
    parser.add_argument("--version-major", type=int, default=1)
    parser.add_argument("--version-minor", type=int, default=0)
    parser.add_argument("--version-patch", type=int, default=0)

    parser.add_argument("--count", type=int, default=5, help="number of readings to pack into the message (oldest to newest)")
    parser.add_argument("--interval-seconds", type=int, default=300, help="gap between consecutive readings, used to derive timedif")
    parser.add_argument("--timestamp", type=int, default=int(time.time()), help="absolute timestamp of the newest reading")

    parser.add_argument("--no-gps", action="store_true", help="omit the optional GPS block")
    parser.add_argument("--latitude", type=float, default=13.7563)
    parser.add_argument("--longitude", type=float, default=100.5018)

    parser.add_argument("--no-cell", action="store_true", help="omit the optional cell (MCC/MNC) block")
    parser.add_argument("--mobile-country-code", type=int, default=520)
    parser.add_argument("--mobile-network-code", type=int, default=1)

    parser.add_argument("--carbon-dioxide", type=int, default=420)
    parser.add_argument("--methane-raw", type=int, default=123)
    parser.add_argument("--methane", type=int, default=45)
    parser.add_argument("--level", type=int, default=67)
    parser.add_argument("--distance", type=int, default=89)
    parser.add_argument("--moisture-raw", type=int, default=321)
    parser.add_argument("--moisture", type=int, default=54)
    parser.add_argument("--battery-voltage", type=int, default=3700)
    parser.add_argument("--battery-percentage", type=int, default=85)
    parser.add_argument("--status", type=int, default=0)
    parser.add_argument("--error-code", type=int, default=0)
    args = parser.parse_args()

    payload = build_payload(args)
    print(f"payload ({len(payload)} bytes, {args.count} reading(s)): {payload.hex()}")

    response = send_payload(args.host, args.port, payload)
    print(f"response: {response.hex()}")


if __name__ == "__main__":
    main()