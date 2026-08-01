package main

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestParseTelemetryPayloadNewFormat(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	deviceID := []byte("TESMBO0001")
	flags := byte(FlagGPSPresent | FlagCellPresent)
	count := byte(2)

	payload := make([]byte, 0, headerSize+expectedRemainingSize(int(count), flags))
	payload = append(payload, 0x1A, 0x2B, flags)
	payload = append(payload, deviceID...)
	payload = append(payload, count)

	payload = appendUint16(payload, 300)
	payload = appendFloat32(payload, 28.5)
	payload = appendFloat32(payload, 65.25)
	payload = appendFloat32(payload, 1012.75)
	payload = appendUint16(payload, 45)
	payload = appendUint16(payload, 420)
	payload = appendUint16(payload, 67)
	payload = appendUint16(payload, 89)
	payload = appendUint16(payload, 5425)
	payload = appendUint16(payload, 8500)
	payload = appendUint16(payload, 3700)
	payload = appendUint16(payload, 123)
	payload = appendUint16(payload, 321)
	payload = append(payload, 0x01, 0x02)

	payload = appendUint16(payload, 60)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendFloat32(payload, 29.75)
	payload = appendFloat32(payload, 66.5)
	payload = appendFloat32(payload, 1013.5)
	payload = appendUint16(payload, 50)
	payload = appendUint16(payload, 430)
	payload = appendUint16(payload, 70)
	payload = appendUint16(payload, 95)
	payload = appendUint16(payload, 5500)
	payload = appendUint16(payload, 9000)
	payload = appendUint16(payload, 3720)
	payload = appendUint16(payload, 130)
	payload = appendUint16(payload, 333)
	payload = append(payload, 0x03, 0x04)

	payload = appendInt32(payload, 137563000)
	payload = appendInt32(payload, 1005018000)
	payload = appendUint16(payload, 520)
	payload = appendUint16(payload, 1)

	data, err := parseTelemetryPayload(payload)
	if err != nil {
		t.Fatalf("parseTelemetryPayload returned error: %v", err)
	}

	if data.Version != "0" {
		t.Fatalf("version = %q, want 0", data.Version)
	}
	if data.ID != "TES-MBO-0001" {
		t.Fatalf("id = %q, want TES-MBO-0001", data.ID)
	}
	if len(data.Readings) != 2 {
		t.Fatalf("readings = %d, want 2", len(data.Readings))
	}
	if got := data.Readings[0].Timestamp; got != 1_722_999_700 {
		t.Fatalf("historical timestamp = %d, want 1722999700", got)
	}
	if got := data.Readings[1].Timestamp; got != 1_723_000_000 {
		t.Fatalf("newest timestamp = %d, want 1723000000", got)
	}
	if got := data.Readings[0].Temperature; math.Abs(got-28.5) > 0.001 {
		t.Fatalf("historical temperature = %f, want 28.5", got)
	}
	if got := data.Readings[1].BatteryPercentage; math.Abs(got-90.0) > 0.001 {
		t.Fatalf("battery percentage = %f, want 90.0", got)
	}
	if got := data.Readings[1].BatteryVoltage; got != 3720 {
		t.Fatalf("battery voltage = %d, want 3720", got)
	}
	if !data.HasGPS || !data.HasCell {
		t.Fatalf("expected gps and cell flags to be true: %+v", data)
	}
	if math.Abs(data.Latitude-13.7563) > 0.0000001 {
		t.Fatalf("latitude = %f, want 13.7563", data.Latitude)
	}
	if math.Abs(data.Longitude-100.5018) > 0.0000001 {
		t.Fatalf("longitude = %f, want 100.5018", data.Longitude)
	}
	if data.MobileCountryCode != 520 || data.MobileNetworkCode != 1 {
		t.Fatalf("unexpected cell data: mcc=%d mnc=%d", data.MobileCountryCode, data.MobileNetworkCode)
	}

}

func appendUint16(dst []byte, value uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, value)
	return append(dst, buf...)
}

func appendUint64(dst []byte, value uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return append(dst, buf...)
}

func appendInt32(dst []byte, value int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(value))
	return append(dst, buf...)
}

func appendFloat32(dst []byte, value float32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, math.Float32bits(value))
	return append(dst, buf...)
}