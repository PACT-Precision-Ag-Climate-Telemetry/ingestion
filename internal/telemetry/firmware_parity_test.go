package telemetry

import (
	"math"
	"testing"
)

// This file is a direct transcription of buildTelemetryPayload() from the
// device firmware (tcp.h). It exists so that any future change to either the
// firmware writer or the parser here shows up as a failing test rather than as
// silently misaligned fields in production telemetry.
//
// Keep the append order below identical to the firmware.
//
// Wire format change (int16/uint16 x100 temperature/humidity, no pressure, no
// error_code, *_raw gated by flags bit 5): this transcription was updated
// ahead of the firmware. buildTelemetryPayload() in tcp.h must be changed to
// match it before devices running the old layout are pointed at this service.

const (
	fwPayloadVersion  byte = 0
	fwFlagVersionMask byte = 0x0F
	fwFlagWithRaw     byte = 1 << 5
	fwFlagWithCell    byte = 1 << 6
	fwFlagWithGPS     byte = 1 << 7
	fwMaxReadings          = 24
)

// fwReading mirrors the firmware's Reading struct.
type fwReading struct {
	timestamp         uint64
	temperature       float32
	humidity          float32
	carbonDioxide     uint16
	methaneRaw        uint16
	methane           uint16
	offset            uint16
	distance          uint16
	moistureRaw       uint16
	moisture          uint16 // centi-percent
	batteryVoltage    uint16
	batteryPercentage uint16 // centi-percent
	status            byte
}

// fwLocation mirrors the firmware's LocationInfo struct.
type fwLocation struct {
	latitude          float32
	longitude         float32
	hasFix            bool
	mobileCountryCode uint16
	mobileNetworkCode uint16
	hasCellInfo       bool
}

// fwEncodePercentTimes100 mirrors encodePercentTimes100() in the firmware,
// including its lossy "value <= 100 means whole percent" heuristic.
func fwEncodePercentTimes100(value uint16) uint16 {
	if value <= 100 {
		return value * 100
	}
	return value
}

// fwEncodeTimes100 scales a float reading to the fixed-point x100 wire value,
// rounding to the nearest hundredth.
func fwEncodeTimes100(value float32) float64 {
	return math.Round(float64(value) * 100)
}

// fwEncodeCoordinateScaled mirrors encodeCoordinateScaled(), including the
// float32 arithmetic the firmware performs before rounding.
func fwEncodeCoordinateScaled(degrees float32) int32 {
	return int32(math.Round(float64(degrees * 1e7)))
}

func fwAppendSensorBody(dst []byte, r fwReading, withRaw bool) []byte {
	dst = appendInt16(dst, int16(fwEncodeTimes100(r.temperature)))
	dst = appendUint16(dst, uint16(fwEncodeTimes100(r.humidity)))
	dst = appendUint16(dst, r.methane)
	dst = appendUint16(dst, r.carbonDioxide)
	dst = appendUint16(dst, r.offset)
	dst = appendUint16(dst, r.distance)
	dst = appendUint16(dst, fwEncodePercentTimes100(r.moisture))
	dst = appendUint16(dst, fwEncodePercentTimes100(r.batteryPercentage))
	dst = appendUint16(dst, r.batteryVoltage)
	if withRaw {
		dst = appendUint16(dst, r.methaneRaw)
		dst = appendUint16(dst, r.moistureRaw)
	}
	return append(dst, r.status)
}

// fwBuildTelemetryPayload transcribes buildTelemetryPayload() from tcp.h.
func fwBuildTelemetryPayload(readings []fwReading, location fwLocation, withRaw bool, magic1, magic2 byte, deviceID string) []byte {
	if len(readings) == 0 {
		return nil
	}

	payload := []byte{magic1, magic2}

	flag := fwPayloadVersion & fwFlagVersionMask
	if withRaw {
		flag |= fwFlagWithRaw
	}
	if location.hasCellInfo {
		flag |= fwFlagWithCell
	}
	if location.hasFix {
		flag |= fwFlagWithGPS
	}
	payload = append(payload, flag)

	id := []byte(deviceID)
	for len(id) < 10 {
		id = append(id, '0')
	}
	payload = append(payload, id[:10]...)
	payload = append(payload, byte(len(readings)))

	clampDiff := func(newer, older uint64) uint16 {
		var diff uint64
		if newer > older {
			diff = newer - older
		}
		if diff > 0xFFFF {
			diff = 0xFFFF
		}
		return uint16(diff)
	}

	// Historical records: timedif is the gap to the NEXT NEWER reading.
	for i := 0; i+1 < len(readings); i++ {
		payload = appendUint16(payload, clampDiff(readings[i+1].timestamp, readings[i].timestamp))
		payload = fwAppendSensorBody(payload, readings[i], withRaw)
	}

	// Newest record: the firmware writes the gap to the PREVIOUS OLDER reading
	// here, then the absolute timestamp.
	newest := readings[len(readings)-1]
	var newestTimedif uint16
	if len(readings) > 1 {
		newestTimedif = clampDiff(newest.timestamp, readings[len(readings)-2].timestamp)
	}
	payload = appendUint16(payload, newestTimedif)
	payload = appendUint64(payload, newest.timestamp)
	payload = fwAppendSensorBody(payload, newest, withRaw)

	if location.hasFix {
		payload = appendInt32(payload, fwEncodeCoordinateScaled(location.latitude))
		payload = appendInt32(payload, fwEncodeCoordinateScaled(location.longitude))
	}
	if location.hasCellInfo {
		payload = appendUint16(payload, location.mobileCountryCode)
		payload = appendUint16(payload, location.mobileNetworkCode)
	}

	return payload
}

func TestFirmwarePayloadRoundTrip(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	base := uint64(1_756_512_000)
	readings := []fwReading{
		{
			timestamp: base, temperature: 28.5, humidity: 65.25,
			carbonDioxide: 420, methaneRaw: 123, methane: 45, offset: 67, distance: 89,
			moistureRaw: 321, moisture: 5425, batteryVoltage: 3700, batteryPercentage: 8500,
			status: 0,
		},
		{
			timestamp: base + 300, temperature: -3.75, humidity: 66.5,
			carbonDioxide: 430, methaneRaw: 130, methane: 50, offset: 70, distance: 95,
			moistureRaw: 333, moisture: 5500, batteryVoltage: 3720, batteryPercentage: 9000,
			status: 0,
		},
		{
			timestamp: base + 600, temperature: 30.0, humidity: 67.0,
			carbonDioxide: 440, methaneRaw: 140, methane: 55, offset: 72, distance: 99,
			moistureRaw: 340, moisture: 5600, batteryVoltage: 3730, batteryPercentage: 9100,
			status: 0x0A,
		},
	}
	location := fwLocation{
		latitude: 13.7563, longitude: 100.5018, hasFix: true,
		mobileCountryCode: 520, mobileNetworkCode: 1, hasCellInfo: true,
	}

	payload := fwBuildTelemetryPayload(readings, location, true, 0x1A, 0x2B, "TESMBO0001")

	wantSize := headerSize + expectedRemainingSize(len(readings), payload[2])
	if len(payload) != wantSize {
		t.Fatalf("firmware payload is %d bytes, parser expects %d", len(payload), wantSize)
	}

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse rejected a firmware-built payload: %v", err)
	}

	if data.ID != "TES-MBO-0001" {
		t.Errorf("id = %q, want TES-MBO-0001", data.ID)
	}
	if data.Version != "0" {
		t.Errorf("version = %q, want 0", data.Version)
	}
	if len(data.Readings) != len(readings) {
		t.Fatalf("readings = %d, want %d", len(data.Readings), len(readings))
	}

	for i, want := range readings {
		got := data.Readings[i]
		if got.Timestamp != want.timestamp {
			t.Errorf("reading %d: timestamp = %d, want %d", i, got.Timestamp, want.timestamp)
		}
		if math.Abs(got.Temperature-float64(want.temperature)) > 1e-4 {
			t.Errorf("reading %d: temperature = %v, want %v", i, got.Temperature, want.temperature)
		}
		if math.Abs(got.Humidity-float64(want.humidity)) > 1e-4 {
			t.Errorf("reading %d: humidity = %v, want %v", i, got.Humidity, want.humidity)
		}
		if got.CarbonDioxide != want.carbonDioxide {
			t.Errorf("reading %d: carbon_dioxide = %d, want %d", i, got.CarbonDioxide, want.carbonDioxide)
		}
		if got.Methane != want.methane {
			t.Errorf("reading %d: methane = %d, want %d", i, got.Methane, want.methane)
		}
		if got.MethaneRaw == nil || *got.MethaneRaw != want.methaneRaw {
			t.Errorf("reading %d: methane_raw = %v, want %d", i, got.MethaneRaw, want.methaneRaw)
		}
		if got.Offset != want.offset {
			t.Errorf("reading %d: offset = %d, want %d", i, got.Offset, want.offset)
		}
		if got.Depth != want.distance {
			t.Errorf("reading %d: depth = %d, want firmware distance %d", i, got.Depth, want.distance)
		}
		if got.MoistureRaw == nil || *got.MoistureRaw != want.moistureRaw {
			t.Errorf("reading %d: moisture_raw = %v, want %d", i, got.MoistureRaw, want.moistureRaw)
		}
		if math.Abs(got.Moisture-float64(want.moisture)/100) > 1e-9 {
			t.Errorf("reading %d: moisture = %v, want %v", i, got.Moisture, float64(want.moisture)/100)
		}
		if got.BatteryVoltage != want.batteryVoltage {
			t.Errorf("reading %d: battery_voltage = %d, want %d", i, got.BatteryVoltage, want.batteryVoltage)
		}
		if math.Abs(got.BatteryPercentage-float64(want.batteryPercentage)/100) > 1e-9 {
			t.Errorf("reading %d: battery_percentage = %v, want %v", i, got.BatteryPercentage, float64(want.batteryPercentage)/100)
		}
		if got.Status != want.status {
			t.Errorf("reading %d: status = %d, want %d", i, got.Status, want.status)
		}
	}

	if !data.HasRaw || !data.HasGPS || !data.HasCell {
		t.Fatalf("expected raw, gps and cell blocks: %+v", data)
	}
}

// The firmware never sends more than MAX_READINGS_PER_PAYLOAD readings, but the
// parser must handle the largest batch it can produce.
func TestFirmwareMaxBatchRoundTrip(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	readings := make([]fwReading, fwMaxReadings)
	base := uint64(1_756_512_000)
	for i := range readings {
		readings[i] = fwReading{timestamp: base + uint64(i)*300, temperature: float32(20 + i)}
	}

	payload := fwBuildTelemetryPayload(readings, fwLocation{}, false, 0x1A, 0x2B, "TESMBO0001")
	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse rejected a max-size firmware batch: %v", err)
	}
	if len(data.Readings) != fwMaxReadings {
		t.Fatalf("readings = %d, want %d", len(data.Readings), fwMaxReadings)
	}
	for i := range readings {
		if data.Readings[i].Timestamp != readings[i].timestamp {
			t.Fatalf("reading %d: timestamp = %d, want %d", i, data.Readings[i].Timestamp, readings[i].timestamp)
		}
	}
}

// A single-reading payload is the common case for the lowest reporting tier.
func TestFirmwareSingleReadingRoundTrip(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	readings := []fwReading{{timestamp: 1_756_512_000, temperature: 25.5, distance: 940}}
	payload := fwBuildTelemetryPayload(readings, fwLocation{}, false, 0x1A, 0x2B, "TESMBO0001")

	if len(payload) != headerSize+newestRecordSize(0) {
		t.Fatalf("single-reading payload is %d bytes, want %d", len(payload), headerSize+newestRecordSize(0))
	}

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse rejected a single-reading payload: %v", err)
	}
	if data.Readings[0].Timestamp != 1_756_512_000 {
		t.Fatalf("timestamp = %d, want 1756512000", data.Readings[0].Timestamp)
	}
	if data.Readings[0].Depth != 940 {
		t.Fatalf("depth = %d, want 940", data.Readings[0].Depth)
	}
	if data.HasGPS || data.HasCell {
		t.Fatalf("expected no gps/cell blocks: %+v", data)
	}
}
