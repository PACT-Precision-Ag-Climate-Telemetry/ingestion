// Tests for the telemetry model and the sensor-health bitfield decoding.

package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeStatusFlags(t *testing.T) {
	tests := []struct {
		status byte
		want   []string
	}{
		{0x00, []string{}},
		{0x01, []string{"battery_read_failed"}},
		{0x02, []string{"ultrasonic_read_failed"}},
		{0x20, []string{"moisture_read_failed"}},
		{0x3F, []string{
			"battery_read_failed",
			"ultrasonic_read_failed",
			"temperature_read_failed",
			"methane_read_failed",
			"co2_read_failed",
			"moisture_read_failed",
		}},
		{0x40, []string{"unknown_bit_6"}},
	}

	for _, tc := range tests {
		got := decodeStatusFlags(tc.status)
		if len(got) != len(tc.want) {
			t.Errorf("decodeStatusFlags(0x%02x) = %v, want %v", tc.status, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("decodeStatusFlags(0x%02x) = %v, want %v", tc.status, got, tc.want)
				break
			}
		}
	}
}

// An all-clear status must publish an empty array, never null, so consumers can
// range over the field unconditionally.
func TestStatusFlagsMarshalsAsEmptyArray(t *testing.T) {
	encoded, err := json.Marshal(Reading{StatusFlags: decodeStatusFlags(0)})
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}
	if !strings.Contains(string(encoded), `"status_flags":[]`) {
		t.Fatalf("status_flags did not marshal as an empty array: %s", encoded)
	}
}
