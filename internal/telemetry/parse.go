// Decoding of a complete binary telemetry payload into the published model.

package telemetry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"ingest/internal/config"
)

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

	hasGPS := flags&FlagGPSPresent != 0
	hasCell := flags&FlagCellPresent != 0

	expectedSize := headerSize + expectedRemainingSize(count, flags)
	if len(payload) != expectedSize {
		return nil, fmt.Errorf("payload size mismatch: received %d bytes, need %d for count=%d flags=0x%02x", len(payload), expectedSize, count, flags)
	}

	type rawRecord struct {
		timedif     uint16
		timestamp   uint64
		temp        float64
		humid       float64
		pressure    float64
		ch4Ppm      uint16
		co2         uint16
		offset      uint16
		depth       uint16
		moisturePct uint16
		battP       uint16
		battV       uint16
		ch4Raw      uint16
		moistureRaw uint16
		status      byte
		errorCode   byte
	}

	bytesOffset := headerSize
	rawRecords := make([]rawRecord, 0, count)
	for i := 0; i < count; i++ {
		isNewest := i == count-1

		timedif := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2

		var timestamp uint64
		if isNewest {
			timestamp = binary.LittleEndian.Uint64(payload[bytesOffset : bytesOffset+8])
			bytesOffset += 8
		}

		temp := float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[bytesOffset : bytesOffset+4])))
		bytesOffset += 4
		humid := float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[bytesOffset : bytesOffset+4])))
		bytesOffset += 4
		pressure := float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[bytesOffset : bytesOffset+4])))
		bytesOffset += 4
		ch4Ppm := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		co2 := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		offset := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		depth := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		moisturePct := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		battP := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		battV := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		ch4Raw := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		moistureRaw := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		status := payload[bytesOffset]
		bytesOffset++
		errorCode := payload[bytesOffset]
		bytesOffset++

		rawRecords = append(rawRecords, rawRecord{
			timedif:     timedif,
			timestamp:   timestamp,
			temp:        temp,
			humid:       humid,
			pressure:    pressure,
			ch4Ppm:      ch4Ppm,
			co2:         co2,
			ch4Raw:      ch4Raw,
			offset:      offset,
			depth:       depth,
			moistureRaw: moistureRaw,
			moisturePct: moisturePct,
			battP:       battP,
			battV:       battV,
			status:      status,
			errorCode:   errorCode,
		})
	}

	newestTimestamp := rawRecords[count-1].timestamp
	timestamps := make([]uint64, count)
	timestamps[count-1] = newestTimestamp

	for i := count - 2; i >= 0; i-- {
		if uint64(rawRecords[i].timedif) > timestamps[i+1] {
			return nil, fmt.Errorf("timedif underflows timestamp reconstruction at record %d", i)
		}
		timestamps[i] = timestamps[i+1] - uint64(rawRecords[i].timedif)
	}

	readings := make([]Reading, count)
	for i, r := range rawRecords {
		readings[i] = Reading{
			Timestamp:         timestamps[i],
			TimeDifference:    r.timedif,
			Temperature:       r.temp,
			Humidity:          r.humid,
			Pressure:          r.pressure,
			CarbonDioxide:     r.co2,
			MethaneRaw:        r.ch4Raw,
			Methane:           r.ch4Ppm,
			Offset:            r.offset,
			Depth:             r.depth,
			MoistureRaw:       r.moistureRaw,
			Moisture:          float64(r.moisturePct) / 100,
			BatteryVoltage:    r.battV,
			BatteryPercentage: float64(r.battP) / 100,
			Status:            r.status,
			StatusFlags:       decodeStatusFlags(r.status),
			ErrorCode:         r.errorCode,
		}
	}

	data := &Data{
		ID:       id,
		Version:  versionText,
		Flags:    flags,
		Readings: readings,
		HasGPS:   hasGPS,
		HasCell:  hasCell,
	}

	if hasGPS {
		lat := int32(binary.LittleEndian.Uint32(payload[bytesOffset : bytesOffset+4]))
		bytesOffset += 4
		lon := int32(binary.LittleEndian.Uint32(payload[bytesOffset : bytesOffset+4]))
		bytesOffset += 4
		latitude := float64(lat) / 10000000
		longitude := float64(lon) / 10000000
		data.Latitude = &latitude
		data.Longitude = &longitude
	}

	if hasCell {
		mcc := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		mnc := binary.LittleEndian.Uint16(payload[bytesOffset : bytesOffset+2])
		bytesOffset += 2
		data.MobileCountryCode = &mcc
		data.MobileNetworkCode = &mnc
	}

	// Logged as JSON rather than %+v: the optional location fields are
	// pointers, which %+v renders as addresses.
	if logLine, err := json.Marshal(data); err == nil {
		fmt.Printf("Parsed Data: %s\n", logLine)
	}

	return data, nil
}
