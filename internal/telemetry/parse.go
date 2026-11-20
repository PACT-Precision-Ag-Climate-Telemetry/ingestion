// Decoding of a complete binary telemetry payload into the published model.

package telemetry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"ingest/internal/config"
)

// cursor reads little-endian values sequentially from a payload whose length
// has already been validated, so reads never run past the end.
type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) u8() byte {
	v := c.buf[c.pos]
	c.pos++
	return v
}

func (c *cursor) u16() uint16 {
	v := binary.LittleEndian.Uint16(c.buf[c.pos:])
	c.pos += 2
	return v
}

func (c *cursor) i16() int16 { return int16(c.u16()) }

func (c *cursor) i32() int32 {
	v := int32(binary.LittleEndian.Uint32(c.buf[c.pos:]))
	c.pos += 4
	return v
}

func (c *cursor) u64() uint64 {
	v := binary.LittleEndian.Uint64(c.buf[c.pos:])
	c.pos += 8
	return v
}

// Parse parses a complete, self-contained telemetry
// payload (header + records + optional GPS/cell blocks).
func Parse(payload []byte) (*Data, error) {
	if len(payload) < headerSize {
		return nil, fmt.Errorf("payload too short: received %d bytes, need at least %d for header", len(payload), headerSize)
	}

	magicByte1, magicByte2, err := config.MagicBytes()
	if err != nil {
		return nil, err
	}

	if payload[0] != magicByte1 || payload[1] != magicByte2 {
		return nil, fmt.Errorf("invalid magic bytes: %x", payload[:2])
	}

	flags := payload[2]
	version := int(flags & flagVersionMask)
	if version != 0 {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	rawId := payload[3:13]
	id := fmt.Sprintf("%c%c%c-%c%c%c-%c%c%c%c", rawId[0], rawId[1], rawId[2], rawId[3], rawId[4], rawId[5], rawId[6], rawId[7], rawId[8], rawId[9])

	versionText := strconv.Itoa(version)

	count := int(payload[13])
	if count == 0 {
		return nil, fmt.Errorf("record count is zero")
	}

	hasRaw := flags&FlagRawPresent != 0
	hasGPS := flags&FlagGPSPresent != 0
	hasCell := flags&FlagCellPresent != 0

	expectedSize := headerSize + expectedRemainingSize(count, flags)
	if len(payload) != expectedSize {
		return nil, fmt.Errorf("payload size mismatch: received %d bytes, need %d for count=%d flags=0x%02x", len(payload), expectedSize, count, flags)
	}

	c := &cursor{buf: payload, pos: headerSize}
	readings := make([]Reading, count)
	var newestTimestamp uint64

	// Record field order must match buildTelemetryPayload() in the firmware.
	for i := 0; i < count; i++ {
		r := &readings[i]
		r.TimeDifference = c.u16()
		if i == count-1 {
			newestTimestamp = c.u64()
		}
		r.Temperature = float64(c.i16()) / 100
		r.Humidity = float64(c.u16()) / 100
		r.Methane = c.u16()
		r.CarbonDioxide = c.u16()
		r.Offset = c.u16()
		r.Depth = c.u16()
		r.Moisture = float64(c.u16()) / 100
		r.BatteryPercentage = float64(c.u16()) / 100
		r.BatteryVoltage = c.u16()
		r.EspTemperature = float64(c.i16()) / 100
		if hasRaw {
			methaneRaw, moistureRaw := c.u16(), c.u16()
			r.MethaneRaw = &methaneRaw
			r.MoistureRaw = &moistureRaw
		}
		r.Status = c.u8()
		r.StatusFlags = decodeStatusFlags(r.Status)
	}

	// Rebuild older timestamps by walking timedif back from the newest one.
	readings[count-1].Timestamp = newestTimestamp
	for i := count - 2; i >= 0; i-- {
		next := readings[i+1].Timestamp
		if uint64(readings[i].TimeDifference) > next {
			return nil, fmt.Errorf("timedif underflows timestamp reconstruction at record %d", i)
		}
		readings[i].Timestamp = next - uint64(readings[i].TimeDifference)
	}

	data := &Data{
		ID:       id,
		Version:  versionText,
		Flags:    flags,
		Readings: readings,
		HasRaw:   hasRaw,
		HasGPS:   hasGPS,
		HasCell:  hasCell,
	}

	if hasGPS {
		latitude := float64(c.i32()) / 10000000
		longitude := float64(c.i32()) / 10000000
		data.Latitude = &latitude
		data.Longitude = &longitude
	}

	if hasCell {
		mcc := c.u16()
		mnc := c.u16()
		data.MobileCountryCode = &mcc
		data.MobileNetworkCode = &mnc
	}

	data.SignalStrength = int8(c.u8())
	data.ResetReason = c.u8()
	data.ResetReasonName = resetReasonName(data.ResetReason)

	// Logged as JSON rather than %+v: the optional location fields are
	// pointers, which %+v renders as addresses.
	if logLine, err := json.Marshal(data); err == nil {
		fmt.Printf("Parsed Data: %s\n", logLine)
	}

	return data, nil
}
