package boteco

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vandersonmota/boteco/config"
	"github.com/vandersonmota/boteco/entries"
	"github.com/vandersonmota/boteco/keydir"
)

type DataFile interface {
	Id() int
	Name() string
	Offset() int
	Write(e *entries.Entry) (int, error)
	WriteHeader(version, size int) (int, error)
	ReadEntry(string, keydir.Item) (entries.Entry, error)
	Close() error
	CurrentSize() (int64, error)
}

type datafile struct {
	id      int // FIXME: don't use timestamp
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

func (d *datafile) CurrentSize() (int64, error) {
	info, err := d.fd.Stat()
	if err != nil {
		return 0, errors.New("Could not stat datafile file descriptor")
	}
	return info.Size(), nil
}

func (d *datafile) Write(entry *entries.Entry) (int, error) {
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
	headerSize := 24
	buffer := make([]byte, headerSize) // 64 bit is assumed
	binary.BigEndian.PutUint64(buffer[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(version))
	binary.BigEndian.PutUint64(buffer[16:], uint64(size))
	bytesWritten, err := d.fd.Write(buffer)
	if err != nil {
		// TODO: log
		return 0, err
	}
	if bytesWritten != headerSize {
		return 0, errors.New("Error when writing file headers")
	}

	d.offset += bytesWritten

	return bytesWritten, nil
}

func (d *datafile) ReadEntry(datadir string, item keydir.Item) (entries.Entry, error) {
	f, err := os.OpenFile(filepath.Join(datadir, fmt.Sprint(item.FileID)), os.O_RDONLY, 664)
	defer f.Close()
	if err != nil {
		return entries.Entry{}, err
	}
	buf := make([]byte, item.Size)
	_, err = f.ReadAt(buf, int64(item.Offset))
	if err != nil {
		return entries.Entry{}, err
	}
	e, err := entries.RebuildEntry(buf)
	if err != nil {
		return entries.Entry{}, err
	}
	return e, nil
}

func (d *datafile) Close() error {
	return d.fd.Close()
}

func NewDataFile(path string, size int) (DataFile, error) {
	fileName := int(time.Now().UnixNano())
	f, err := os.OpenFile(filepath.Join(path, fmt.Sprint(fileName)), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	df := &datafile{
		id:      fileName, // TODO: maybe uuids?
		fd:      f,
		offset:  0,
		name:    path,
		maxSize: size,
	}

	_, err = df.WriteHeader(config.Version, size)
	if err != nil {
		return nil, err
	}

	return df, nil

}
