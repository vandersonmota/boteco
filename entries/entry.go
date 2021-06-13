package entries

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/vandersonmota/boteco/config"
)

const (
	KeySize                   = 4
	ValueSize                 = 8
	ChecksumSize              = 4
	TimestampSize             = 8
	FileIDSize                = 8
	EntryHeaderSize           = ChecksumSize + TimestampSize + KeySize + ValueSize
	MaxKeySize                = 255
	EntryKeyOverflowErrorCode = 1
)

type Error struct {
	err  string
	code int
}

func (e *Error) Error() string {
	return fmt.Sprintf("Error: %s - Code: %d", e.err, e.code)
}

type EntryHeader struct {
	checksum  uint32
	tstamp    uint64
	KeySize   uint32
	ValueSize uint32
}

type Entry struct {
	*EntryHeader
	Key   []byte
	Value []byte
}

func (e *Entry) Encode() (headers, key, value []byte) {
	buffer := make([]byte, ChecksumSize+TimestampSize+KeySize+ValueSize)
	binary.BigEndian.PutUint32(buffer[:ChecksumSize], e.checksum)
	binary.BigEndian.PutUint64(buffer[ChecksumSize:ChecksumSize+TimestampSize], e.tstamp)
	binary.BigEndian.PutUint32(buffer[ChecksumSize+TimestampSize:ChecksumSize+TimestampSize+KeySize], e.KeySize)
	binary.BigEndian.PutUint32(buffer[ChecksumSize+TimestampSize+KeySize:ChecksumSize+TimestampSize+KeySize+ValueSize], e.ValueSize)
	return buffer, e.Key, e.Value
}

func (e *Entry) Size() int {
	return ChecksumSize + TimestampSize + KeySize + ValueSize + len(e.Key) + len(e.Value)
}

func NewEntry(key string, value []byte) (Entry, error) {
	k := []byte(key)
	if len(k) > MaxKeySize {
		return Entry{}, &Error{err: "Key too large", code: EntryKeyOverflowErrorCode}
	}
	return Entry{
		EntryHeader: &EntryHeader{
			checksum:  crc32.ChecksumIEEE(value),
			tstamp:    uint64(time.Now().UnixNano()),
			KeySize:   uint32(len(k)),
			ValueSize: uint32(len(value)),
		},
		Key:   []byte(key),
		Value: value,
	}, nil
}

func RebuildHeaders(buffer []byte) (EntryHeader, error) {
	keyLength := binary.BigEndian.Uint32(buffer[ChecksumSize+TimestampSize : ChecksumSize+TimestampSize+KeySize])
	valueLength := binary.BigEndian.Uint32(buffer[ChecksumSize+TimestampSize+KeySize : config.EntryHeaderSize])
	e := EntryHeader{
		checksum:  binary.BigEndian.Uint32(buffer[:ChecksumSize]),
		tstamp:    binary.BigEndian.Uint64(buffer[ChecksumSize : ChecksumSize+TimestampSize]),
		KeySize:   keyLength,
		ValueSize: valueLength,
	}

	return e, nil

}

func RebuildEntry(buffer []byte) (Entry, error) {
	// TODO: Add CRC check
	keyLength := binary.BigEndian.Uint32(buffer[ChecksumSize+TimestampSize : ChecksumSize+TimestampSize+KeySize])
	valueLength := binary.BigEndian.Uint32(buffer[ChecksumSize+TimestampSize+KeySize : ChecksumSize+TimestampSize+KeySize+ValueSize])
	e := Entry{
		EntryHeader: &EntryHeader{
			checksum:  binary.BigEndian.Uint32(buffer[:ChecksumSize]),
			tstamp:    binary.BigEndian.Uint64(buffer[ChecksumSize : ChecksumSize+TimestampSize]),
			KeySize:   keyLength,
			ValueSize: valueLength,
		},
		Key:   buffer[ChecksumSize+TimestampSize+KeySize+ValueSize : ChecksumSize+TimestampSize+KeySize+ValueSize+keyLength],
		Value: buffer[ChecksumSize+TimestampSize+KeySize+ValueSize+keyLength : ChecksumSize+TimestampSize+KeySize+ValueSize+keyLength+valueLength],
	}

	return e, nil

}
