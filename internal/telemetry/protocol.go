// Package telemetry describes the binary telemetry wire format, the framing
// needed to read one complete message off a connection, and the decoding of a
// message into the model published to RabbitMQ. It mirrors
// buildTelemetryPayload() in the device firmware (tcp.h); firmware_parity_test.go
// enforces that the two stay in step.
package telemetry

import (
	"fmt"
	"io"
)

// Header layout (14 bytes):
//
//	0-1   magic bytes
//	2     flags/version
//	3-12  ID (10 bytes)
//	13    record count
const headerSize = 14

// Shared per-record sensor payload (always present):
// temp (int16, degC x100), humid, ch4_ppm, co2, offset, depth, moisture_pct,
// batt_p, batt_v, esp_temp (uint16 each, esp_temp is int16 degC x100 of the ESP
// die) = 20 bytes, then status (1 byte) => 21 bytes.
// When FlagRawPresent is set, ch4_raw + moisture_raw (uint16 each = 4 bytes)
// sit between esp_temp and status.
const recordSensorBaseSize = 21
const recordRawSize = 4

// Historical records carry timedif + sensor payload; the newest record adds
// an absolute timestamp while still carrying the timedif field.
const timedifSize = 2
const timestampSize = 8

// Optional trailing blocks, gated by bits in the header flags byte.
const gpsBlockSize = 8  // 4-byte latitude + 4-byte longitude
const cellBlockSize = 4 // 2-byte MCC + 2-byte MNC

// Fixed trailer, always last: int8 cellular signal strength (dBm) + uint8 ESP
// reset reason (esp_reset_reason_t).
const trailerSize = 2

const (
	flagVersionMask byte = 0x0F
	FlagRawPresent  byte = 1 << 5
	FlagCellPresent byte = 1 << 6
	FlagGPSPresent  byte = 1 << 7
)

// recordSensorSize is the per-record sensor payload size for the given flags:
// 21 bytes, or 25 when the raw ADC fields are present.
func recordSensorSize(flags byte) int {
	if flags&FlagRawPresent != 0 {
		return recordSensorBaseSize + recordRawSize
	}
	return recordSensorBaseSize
}

// historicalRecordSize is 23 bytes (27 with raw fields).
func historicalRecordSize(flags byte) int {
	return timedifSize + recordSensorSize(flags)
}

// newestRecordSize is 31 bytes (35 with raw fields).
func newestRecordSize(flags byte) int {
	return timedifSize + timestampSize + recordSensorSize(flags)
}

// ReadMessage reads one full variable-length telemetry payload off
// the connection: it reads the fixed header first, uses the count/flags in
// that header to work out how many more bytes to expect, then reads the
// rest. Returns the complete raw payload (header included).
func ReadMessage(reader io.Reader) ([]byte, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	flags := header[2]
	count := int(header[13])
	if count == 0 {
		return nil, fmt.Errorf("record count is zero")
	}

	remaining := expectedRemainingSize(count, flags)
	rest := make([]byte, remaining)
	if _, err := io.ReadFull(reader, rest); err != nil {
		return nil, fmt.Errorf("reading body (%d bytes): %w", remaining, err)
	}

	return append(header, rest...), nil
}

// expectedRemainingSize returns the number of bytes that follow the header
// for a payload with the given record count and flags.
func expectedRemainingSize(count int, flags byte) int {
	size := newestRecordSize(flags)
	if count > 1 {
		size += (count - 1) * historicalRecordSize(flags)
	}
	if flags&FlagGPSPresent != 0 {
		size += gpsBlockSize
	}
	if flags&FlagCellPresent != 0 {
		size += cellBlockSize
	}
	return size + trailerSize
}
