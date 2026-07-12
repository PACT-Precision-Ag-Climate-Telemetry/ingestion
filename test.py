import argparse
import os
import socket
import struct
import time
from typing import Optional


PAYLOAD_SIZE = 54


def parse_int(value: Optional[str], default: str) -> int:
    raw = value if value is not None else default
    return int(raw, 0)


def build_payload(args: argparse.Namespace) -> bytes:
    magic_1 = parse_int(os.getenv("magic_byte_1"), "0x1A")
    magic_2 = parse_int(os.getenv("magic_byte_2"), "0x2B")

    device_id = args.device_id.encode("ascii")[:10].ljust(10, b"0")
    version = bytes((args.version_major, args.version_minor, args.version_patch))
    timestamp = struct.pack("<Q", args.timestamp)
    latitude = struct.pack("<f", args.latitude)
    longitude = struct.pack("<f", args.longitude)

    body = b"".join(
        [
            bytes((magic_1, magic_2)),
            device_id,
            version,
            timestamp,
            latitude,
            longitude,
            struct.pack("<H", args.carbon_dioxide),
            struct.pack("<H", args.methane_raw),
            struct.pack("<H", args.methane),
            struct.pack("<H", args.level),
            struct.pack("<H", args.distance),
            struct.pack("<H", args.moisture_raw),
            struct.pack("<H", args.moisture),
            struct.pack("<H", args.mobile_country_code),
            struct.pack("<H", args.mobile_network_code),
            struct.pack("<I", args.uptime),
            bytes((args.error_code,)),
        ]
    )

    if len(body) != PAYLOAD_SIZE:
        raise ValueError(f"payload must be {PAYLOAD_SIZE} bytes, got {len(body)}")

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
    parser.add_argument("--timestamp", type=int, default=int(time.time()))
    parser.add_argument("--latitude", type=float, default=13.7563)
    parser.add_argument("--longitude", type=float, default=100.5018)
    parser.add_argument("--carbon-dioxide", type=int, default=420)
    parser.add_argument("--methane-raw", type=int, default=123)
    parser.add_argument("--methane", type=int, default=45)
    parser.add_argument("--level", type=int, default=67)
    parser.add_argument("--distance", type=int, default=89)
    parser.add_argument("--moisture-raw", type=int, default=321)
    parser.add_argument("--moisture", type=int, default=54)
    parser.add_argument("--mobile-country-code", type=int, default=520)
    parser.add_argument("--mobile-network-code", type=int, default=1)
    parser.add_argument("--uptime", type=int, default=3600)
    parser.add_argument("--error-code", type=int, default=0)
    args = parser.parse_args()

    payload = build_payload(args)
    print(f"payload ({len(payload)} bytes): {payload.hex()}")

    response = send_payload(args.host, args.port, payload)
    print(f"response: {response.hex()}")


if __name__ == "__main__":
    main()