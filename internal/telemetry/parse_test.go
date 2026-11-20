// Tests for decoding binary payloads into the published telemetry model.

package telemetry

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// testSensor is one record's sensor body in wire units.
type testSensor struct {
	temp        int16 // degC x100
	humid       uint16
	ch4         uint16
	co2         uint16
	offset      uint16
	depth       uint16
	moisture    uint16
	battP       uint16
	battV       uint16
	espTemp     int16 // ESP die degC x100
	ch4Raw      uint16
	moistureRaw uint16
	status      byte
}

func appendTestSensor(dst []byte, s testSensor, withRaw bool) []byte {
	dst = appendInt16(dst, s.temp)
	dst = appendUint16(dst, s.humid)
	dst = appendUint16(dst, s.ch4)
	dst = appendUint16(dst, s.co2)
	dst = appendUint16(dst, s.offset)
	dst = appendUint16(dst, s.depth)
	dst = appendUint16(dst, s.moisture)
	dst = appendUint16(dst, s.battP)
	dst = appendUint16(dst, s.battV)
	dst = appendInt16(dst, s.espTemp)
	if withRaw {
		dst = appendUint16(dst, s.ch4Raw)
		dst = appendUint16(dst, s.moistureRaw)
	}
	return append(dst, s.status)
}

func testHeader(flags byte, count byte) []byte {
	payload := []byte{0x1A, 0x2B, flags}
	payload = append(payload, []byte("TESMBO0001")...)
	return append(payload, count)
}

func TestParseTelemetryPayloadNewFormat(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	flags := FlagRawPresent | FlagGPSPresent | FlagCellPresent
	payload := testHeader(flags, 2)

	payload = appendUint16(payload, 300)
	payload = appendTestSensor(payload, testSensor{
		temp: 2850, humid: 6525, ch4: 45, co2: 420, offset: 67, depth: 89,
		moisture: 5425, battP: 8500, battV: 3700, espTemp: 4125, ch4Raw: 123, moistureRaw: 321, status: 0x01,
	}, true)

	payload = appendUint16(payload, 300)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendTestSensor(payload, testSensor{
		temp: 2975, humid: 6650, ch4: 50, co2: 430, offset: 70, depth: 95,
		moisture: 5500, battP: 9000, battV: 3720, espTemp: -550, ch4Raw: 130, moistureRaw: 333, status: 0x03,
	}, true)

	payload = appendInt32(payload, 137563000)
	payload = appendInt32(payload, 1005018000)
	payload = appendUint16(payload, 520)
	payload = appendUint16(payload, 1)
	payload = appendTestTrailer(payload)

	// 14 header + 27 historical + 35 newest + 8 GPS + 4 cell + 2 trailer
	if len(payload) != 90 {
		t.Fatalf("test payload is %d bytes, want 90", len(payload))
	}

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if data.Version != "0" {
		t.Fatalf("version = %q, want 0", data.Version)
	}
	if data.ID != "TES-MBO-0001" {
		t.Fatalf("id = %q, want TES-MBO-0001", data.ID)
	}
	if !data.HasRaw || !data.HasGPS || !data.HasCell {
		t.Fatalf("expected raw, gps and cell flags to be true: %+v", data)
	}
	if len(data.Readings) != 2 {
		t.Fatalf("readings = %d, want 2", len(data.Readings))
	}

	old, newest := data.Readings[0], data.Readings[1]
	if old.Timestamp != 1_722_999_700 {
		t.Fatalf("historical timestamp = %d, want 1722999700", old.Timestamp)
	}
	if newest.Timestamp != 1_723_000_000 {
		t.Fatalf("newest timestamp = %d, want 1723000000", newest.Timestamp)
	}
	if old.Temperature != 28.5 || old.Humidity != 65.25 {
		t.Fatalf("historical temp/humid = %v/%v, want 28.5/65.25", old.Temperature, old.Humidity)
	}
	if newest.Temperature != 29.75 || newest.Humidity != 66.5 {
		t.Fatalf("newest temp/humid = %v/%v, want 29.75/66.5", newest.Temperature, newest.Humidity)
	}
	if old.Methane != 45 || old.CarbonDioxide != 420 || old.Offset != 67 || old.Depth != 89 {
		t.Fatalf("historical gas/level fields wrong: %+v", old)
	}
	if old.Moisture != 54.25 || newest.BatteryPercentage != 90 || newest.BatteryVoltage != 3720 {
		t.Fatalf("scaled fields wrong: moisture=%v batt_p=%v batt_v=%d", old.Moisture, newest.BatteryPercentage, newest.BatteryVoltage)
	}
	if old.MethaneRaw == nil || *old.MethaneRaw != 123 || old.MoistureRaw == nil || *old.MoistureRaw != 321 {
		t.Fatalf("historical raw fields wrong: %+v", old)
	}
	if newest.MethaneRaw == nil || *newest.MethaneRaw != 130 || newest.MoistureRaw == nil || *newest.MoistureRaw != 333 {
		t.Fatalf("newest raw fields wrong: %+v", newest)
	}
	if old.EspTemperature != 41.25 || newest.EspTemperature != -5.5 {
		t.Fatalf("esp temperature = %v/%v, want 41.25/-5.5", old.EspTemperature, newest.EspTemperature)
	}
	if data.SignalStrength != -85 || data.ResetReason != 9 || data.ResetReasonName != "brownout" {
		t.Fatalf("trailer = %d dBm / %d %q, want -85 / 9 brownout", data.SignalStrength, data.ResetReason, data.ResetReasonName)
	}
	if old.Status != 0x01 || newest.Status != 0x03 {
		t.Fatalf("status = %d/%d, want 1/3", old.Status, newest.Status)
	}

	if data.Latitude == nil || math.Abs(*data.Latitude-13.7563) > 0.0000001 {
		t.Fatalf("latitude = %v, want 13.7563", data.Latitude)
	}
	if data.Longitude == nil || math.Abs(*data.Longitude-100.5018) > 0.0000001 {
		t.Fatalf("longitude = %v, want 100.5018", data.Longitude)
	}
	if data.MobileCountryCode == nil || data.MobileNetworkCode == nil ||
		*data.MobileCountryCode != 520 || *data.MobileNetworkCode != 1 {
		t.Fatalf("unexpected cell data: %+v", data)
	}
}

// With flags bit 5 clear the record omits methane_raw and moisture_raw, so
// status follows battery_voltage directly and the JSON drops both fields.
func TestParseTelemetryPayloadWithoutRaw(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	payload := testHeader(0x00, 2)
	payload = appendUint16(payload, 60)
	payload = appendTestSensor(payload, testSensor{temp: 2500, humid: 5000, battV: 3900, status: 0x04}, false)
	payload = appendUint16(payload, 60)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendTestSensor(payload, testSensor{temp: 2510, humid: 5010, battV: 3899, status: 0x00}, false)

	payload = appendTestTrailer(payload)

	// 14 header + 23 historical + 31 newest + 2 trailer
	if len(payload) != 70 {
		t.Fatalf("test payload is %d bytes, want 70", len(payload))
	}

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if data.HasRaw {
		t.Fatalf("has_raw = true, want false")
	}
	for i, r := range data.Readings {
		if r.MethaneRaw != nil || r.MoistureRaw != nil {
			t.Fatalf("reading %d: raw fields set without raw flag: %+v", i, r)
		}
	}
	if data.Readings[0].Status != 0x04 || data.Readings[0].BatteryVoltage != 3900 {
		t.Fatalf("status/battery misaligned: %+v", data.Readings[0])
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}
	for _, field := range []string{"methane_raw", "moisture_raw"} {
		if strings.Contains(string(encoded), field) {
			t.Errorf("%s present in JSON without raw flag: %s", field, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"has_raw":false`) {
		t.Errorf("has_raw missing from JSON: %s", encoded)
	}
}

// Temperature is a signed int16, so sub-zero readings must survive.
func TestParseTelemetryPayloadNegativeTemperature(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	payload := testHeader(0x00, 1)
	payload = appendUint16(payload, 0)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendTestSensor(payload, testSensor{temp: -1250}, false)
	payload = appendTestTrailer(payload)

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if got := data.Readings[0].Temperature; got != -12.5 {
		t.Fatalf("temperature = %v, want -12.5", got)
	}
}

// The raw flag changes the record size, so a frame whose flag disagrees with
// its body length must be rejected rather than parsed misaligned.
func TestParseTelemetryPayloadRawFlagSizeMismatch(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	payload := testHeader(0x00, 1) // raw flag clear...
	payload = appendUint16(payload, 0)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendTestSensor(payload, testSensor{}, true) // ...but raw bytes present
	payload = appendTestTrailer(payload)

	if _, err := Parse(payload); err == nil || !strings.Contains(err.Error(), "payload size mismatch") {
		t.Fatalf("Parse error = %v, want payload size mismatch", err)
	}
}

// A cell block with MNC 0 is valid, and coordinates of exactly 0 are a real
// (if unlikely) fix. Both used to vanish from the published JSON.
func TestParseTelemetryPayloadKeepsZeroLocationValues(t *testing.T) {
	t.Setenv("magic_byte_1", "0x1A")
	t.Setenv("magic_byte_2", "0x2B")

	payload := testHeader(FlagGPSPresent|FlagCellPresent, 1)
	payload = appendUint16(payload, 0)
	payload = appendUint64(payload, 1_723_000_000)
	payload = appendTestSensor(payload, testSensor{temp: 2500, humid: 5000}, false)

	payload = appendInt32(payload, 0)
	payload = appendInt32(payload, 0)
	payload = appendUint16(payload, 520)
	payload = appendUint16(payload, 0)
	payload = appendTestTrailer(payload)

	data, err := Parse(payload)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}

	for _, field := range []string{"latitude", "longitude", "mobile_country_code", "mobile_network_code"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("%s missing from published JSON: %s", field, encoded)
		}
	}
}
