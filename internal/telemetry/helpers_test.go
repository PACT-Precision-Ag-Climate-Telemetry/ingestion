// Little-endian append helpers shared by the payload-building tests.

package telemetry

import (
	"encoding/binary"
)

func appendUint16(dst []byte, value uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, value)
	return append(dst, buf...)
}

func appendInt16(dst []byte, value int16) []byte {
	return appendUint16(dst, uint16(value))
}

func appendUint64(dst []byte, value uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return append(dst, buf...)
}

func appendTestTrailer(dst []byte) []byte {
	return append(dst, byte(0xAB), 9) // -85 dBm, brownout
}

func appendInt32(dst []byte, value int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(value))
	return append(dst, buf...)
}
