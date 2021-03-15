package potatomq

import (
	"encoding/binary"
	"fmt"
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

type Item struct {
	fileID int
	offset int
	size   int
}

type KeyDir struct {
	m map[string]Item // TODO: not thread safe
}

func (kd *KeyDir) Get(key string) Item {
	item, exists := kd.m[key]

	if !exists {
		return Item{}
	}

	return item
}

func (kd *KeyDir) Set(key string, offset, size, fileID int) {
	kd.m[key] = Item{
		fileID: fileID,
		offset: offset - size, //Means file was just written, then offset moved
		size:   size,
	}
}

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
	binary.BigEndian.PutUint32(buffer[checksumSize+timestampSize:checksumSize+timestampSize+keySize], e.keySize)
	binary.BigEndian.PutUint32(buffer[checksumSize+timestampSize+keySize:checksumSize+timestampSize+keySize+valueSize], e.valueSize)
	return buffer, e.key, e.value
}

func (e *Entry) Size() int {
	return checksumSize + timestampSize + keySize + valueSize + len(e.key) + len(e.value)
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
	Write(e *Entry) (int, error)
	WriteHeader(version, size int) (int, error)
	ReadEntry(string, Item) (Entry, error)
	Close() error
}

type datafile struct {
	id      int
	name    string
	offset  int
	maxSize int
	fd      *os.File
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

func (d *datafile) Write(entry *Entry) (int, error) {
	headers, key, value := entry.Encode()
	bytesWritten := 0
	n, err := d.fd.Write(headers)
	if err != nil {
		// TODO: log
		return 0, err
	}
	bytesWritten += n
	n, err = d.fd.Write(key)
	if err != nil {
		// TODO: log
		return 0, err
	}
	bytesWritten += n
	n, err = d.fd.Write(value)
	if err != nil {
		// TODO: log
		return 0, err
	}
	bytesWritten += n
	d.offset += bytesWritten

	return bytesWritten, nil
}

/*
 TODO: Be explicit about int size on config
 as integers change their size according to the operating
 system (32 and 64 bits)
*/
func (d *datafile) WriteHeader(version, size int) (int, error) {
	buffer := make([]byte, 24) // 64 bit is assumed
	binary.BigEndian.PutUint64(buffer[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(version))
	binary.BigEndian.PutUint64(buffer[16:], uint64(size))
	bytesWritten := 0
	n, err := d.fd.Write(buffer)
	if err != nil {
		// TODO: log
		return 0, err
	}
	bytesWritten += n
	d.offset += bytesWritten

	return bytesWritten, nil
}

func (d *datafile) ReadEntry(datadir string, item Item) (Entry, error) {
	f, err := os.OpenFile(filepath.Join(datadir, fmt.Sprint(item.fileID)), os.O_RDONLY, 664)
	defer f.Close()
	if err != nil {
		return Entry{}, err
	}
	buf := make([]byte, item.size)
	_, err = f.ReadAt(buf, int64(item.offset))
	if err != nil {
		return Entry{}, err
	}
	e, err := RebuildEntry(buf)
	if err != nil {
		return Entry{}, err
	}
	return e, nil
}

func (d *datafile) Close() error {
	return d.fd.Close()
}

func RebuildEntry(buffer []byte) (Entry, error) {
	// TODO: Add CRC check
	keyLength := binary.BigEndian.Uint32(buffer[checksumSize+timestampSize : checksumSize+timestampSize+keySize])
	valueLength := binary.BigEndian.Uint32(buffer[checksumSize+timestampSize+keySize : checksumSize+timestampSize+keySize+valueSize])
	e := Entry{
		checksum:  binary.BigEndian.Uint32(buffer[:checksumSize]),
		tstamp:    binary.BigEndian.Uint64(buffer[checksumSize : checksumSize+timestampSize]),
		keySize:   keyLength,
		valueSize: valueLength,
		key:       buffer[checksumSize+timestampSize+keySize+valueSize : checksumSize+timestampSize+keySize+valueSize+keyLength],
		value:     buffer[checksumSize+timestampSize+keySize+valueSize+keyLength : checksumSize+timestampSize+keySize+valueSize+keyLength+valueLength],
	}

	return e, nil

}

func NewDataFile(path string, size int) (DataFile, error) {
	fileName := int(time.Now().UnixNano())
	f, err := os.OpenFile(filepath.Join(path, fmt.Sprint(fileName)), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	return &datafile{
		id:      fileName, // TODO: maybe uuids?
		fd:      f,
		offset:  0,
		name:    path,
		maxSize: size,
	}, nil

}

func BuildKeyDir() (KeyDir, error) {
	return KeyDir{
		m: map[string]Item{},
	}, nil
}

type DB struct {
	df      DataFile
	kd      KeyDir
	datadir string
	config  Config
}

func NewDB(cfg Config) (*DB, error) {
	err := os.MkdirAll(cfg.Datadir, 0755)
	if err != nil {
		return nil, err
	}
	df, err := NewDataFile(cfg.Datadir, cfg.MaxDataFileSize)
	if err != nil {
		return &DB{}, err
	}
	_, err = df.WriteHeader(Version, cfg.MaxDataFileSize)
	if err != nil {
		return &DB{}, err
	}
	kd, err := BuildKeyDir()
	if err != nil {
		return &DB{}, err
	}
	return &DB{
		df:      df,
		kd:      kd,
		datadir: cfg.Datadir, // TODO: remove redundancy with config
		config:  cfg,
	}, nil
}

func (mq *DB) Put(key string, value []byte) error {
	e := NewEntry(key, value)
	if (mq.df.Offset() + e.Size()) >= mq.config.MaxDataFileSize {
		df, err := NewDataFile(mq.config.Datadir, mq.config.MaxDataFileSize)
		if err != nil {
			return err
		}
		mq.df = df

	}
	bytesWritten, err := mq.df.Write(&e)
	if err != nil {
		return err
	}
	mq.kd.Set(key, mq.df.Offset(), bytesWritten, mq.df.Id())
	return nil
}

func (mq *DB) Get(key string) ([]byte, error) {
	item := mq.kd.Get(key)
	entry, err := mq.df.ReadEntry(mq.datadir, item)
	if err != nil {
		return make([]byte, 0), err
	}

	return entry.value, err
}

func (mq *DB) Shutdown() error {
	err := mq.df.Close()
	if err != nil {
		// TODO: log
		return err
	}
	return nil
}
