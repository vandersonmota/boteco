package potatomq

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

const (
	keySize       = 4
	valueSize     = 8
	checksumSize  = 4
	timestampSize = 8
)

type Entry struct {
	checksum  uint32
	tstamp    uint64
	keySize   uint32
	valueSize uint32
	key       []byte
	value     []byte
}

func (e *Entry) Encode() (headers, key, value []byte) {
	buffer := make([]byte, checksumSize+timestampSize+keySize+valueSize)
	binary.BigEndian.PutUint32(buffer[:checksumSize], e.checksum)
	binary.BigEndian.PutUint64(buffer[checksumSize:checksumSize+timestampSize], e.tstamp)
	binary.BigEndian.PutUint32(buffer[timestampSize:checksumSize+timestampSize+keySize], e.keySize)
	binary.BigEndian.PutUint32(buffer[keySize:checksumSize+timestampSize+keySize+valueSize], e.valueSize)
	return buffer, e.key, e.value
}

func NewEntry(key string, value []byte) Entry {
	k := []byte(key)
	return Entry{
		checksum:  crc32.ChecksumIEEE(value),
		tstamp:    uint64(time.Now().UnixNano()),
		keySize:   uint32(len(k)),
		valueSize: uint32(len(value)),
		key:       []byte(key),
		value:     value,
	}
}

type DataFile interface {
	Id() int
	Name() string
	Offset() int
	Write(e *Entry) error
}

type datafile struct {
	id     int
	name   string
	offset int
	fd     *os.File
}

func (d *datafile) Id() int {
	return d.id
}

func (d *datafile) Name() string {
	return d.name
}

func (d *datafile) Offset() int {
	return d.offset
}

func (d *datafile) Write(entry *Entry) error {
	headers, key, value := entry.Encode()
	_, err := d.fd.Write(headers)
	if err != nil {
		// TODO: log
		return err
	}
	_, err = d.fd.Write(key)
	if err != nil {
		// TODO: log
		return err
	}
	_, err = d.fd.Write(value)
	if err != nil {
		// TODO: log
		return err
	}
	return nil
}

func NewDataFile(path string) (DataFile, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 660)
	if err != nil {
		return nil, err
	}
	return &datafile{
		id:     0, // TODO: dynamic ids
		fd:     f,
		offset: 0, // TODO change it
		name:   path,
	}, nil

}

type DB struct {
	df DataFile
}

func NewDB(datadir string) (*DB, error) {
	err := os.MkdirAll(datadir, 0755)
	if err != nil {
		return nil, err
	}
	df, _ := NewDataFile(filepath.Join(datadir, "datafile"))
	return &DB{
		df: df,
	}, nil
}

func (mq *DB) Put(key string, value []byte) error {
	e := NewEntry(key, value)
	err := mq.df.Write(&e)
	return err
}
