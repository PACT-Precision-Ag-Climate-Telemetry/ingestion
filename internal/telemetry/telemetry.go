// The parsed telemetry model: the shapes published to RabbitMQ, and the
// decoding of the firmware's sensor-health bitfield.

package telemetry

import (
	"fmt"
)

// Status bit meanings, mirroring TelemetryStatus in the device firmware
// (tcp.h). A set bit means that sensor's read failed, so the value carried in
// the reading is the previous sample rather than a fresh measurement — without
// decoding these, a dead sensor is indistinguishable from a value that
// genuinely did not change.
var telemetryStatusFlagNames = [8]string{
	0: "battery_read_failed",
	1: "ultrasonic_read_failed",
	2: "temperature_read_failed",
	3: "methane_read_failed",
	4: "co2_read_failed",
	5: "moisture_read_failed",
}

// decodeStatusFlags expands the raw status byte into names. Bits the firmware
// has not defined yet are still reported, so a firmware that adds a sensor
// before this table is updated does not silently drop the signal.
func decodeStatusFlags(status byte) []string {
	flags := make([]string, 0, 8)
	for bit := 0; bit < 8; bit++ {
		if status&(1<<bit) == 0 {
			continue
		}
		if name := telemetryStatusFlagNames[bit]; name != "" {
			flags = append(flags, name)
			continue
		}
		flags = append(flags, fmt.Sprintf("unknown_bit_%d", bit))
	}
	return flags
}

// Reading is a single sample within a telemetry payload. Payloads
// carry multiple readings (oldest to newest) in one message.
//
// MethaneRaw and MoistureRaw are only on the wire when the payload sets
// FlagRawPresent (flags bit 5); otherwise they are nil and omitted from JSON.
type Reading struct {
	Timestamp         uint64   `json:"timestamp"`
	TimeDifference    uint16   `json:"timedif"`
	Temperature       float64  `json:"temperature"`
	Humidity          float64  `json:"humidity"`
	CarbonDioxide     uint16   `json:"carbon_dioxide"`
	MethaneRaw        *uint16  `json:"methane_raw,omitempty"`
	Methane           uint16   `json:"methane"`
	Offset            uint16   `json:"offset"`
	Depth             uint16   `json:"depth"`
	MoistureRaw       *uint16  `json:"moisture_raw,omitempty"`
	Moisture          float64  `json:"moisture"`
	BatteryVoltage    uint16   `json:"battery_voltage"`
	BatteryPercentage float64  `json:"battery_percentage"`
	Status            byte     `json:"status"`
	StatusFlags       []string `json:"status_flags"`
}

// The optional location fields are pointers so that a legitimate zero value
// (null-island coordinates, or an MNC of 0, which is a real network code) is
// still published. With plain values, encoding/json's omitempty dropped them
// and consumers saw has_cell=true with no mobile_network_code at all.
type Data struct {
	ID                string    `json:"id"`
	Version           string    `json:"version"`
	Flags             byte      `json:"flags"`
	Readings          []Reading `json:"readings"` // oldest to newest
	HasRaw            bool      `json:"has_raw"`
	HasGPS            bool      `json:"has_gps"`
	Latitude          *float64  `json:"latitude,omitempty"`
	Longitude         *float64  `json:"longitude,omitempty"`
	HasCell           bool      `json:"has_cell"`
	MobileCountryCode *uint16   `json:"mobile_country_code,omitempty"`
	MobileNetworkCode *uint16   `json:"mobile_network_code,omitempty"`
}
