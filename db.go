package potatomq

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vandersonmota/potatomq/config"
	"github.com/vandersonmota/potatomq/entries"
	"github.com/vandersonmota/potatomq/keydir"
)

type DataFile interface {
	Id() int
	Name() string
	Offset() int
	Write(e *entries.Entry) (int, error)
	WriteHeader(version, size int) (int, error)
	ReadEntry(string, keydir.Item) (entries.Entry, error)
	Close() error
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
	buffer := make([]byte, 24) // 64 bit is assumed
	binary.BigEndian.PutUint64(buffer[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(version))
	binary.BigEndian.PutUint64(buffer[16:], uint64(size))
	bytesWritten, err := d.fd.Write(buffer)
	if err != nil {
		// TODO: log
		return 0, err
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
	return &datafile{
		id:      fileName, // TODO: maybe uuids?
		fd:      f,
		offset:  0,
		name:    path,
		maxSize: size,
	}, nil

}

type DB struct {
	df      DataFile
	kd      keydir.KeyDir
	datadir string
	config  config.Config
}

func NewDB(cfg config.Config) (*DB, error) {
	err := os.MkdirAll(cfg.Datadir, 0755)
	if err != nil {
		return nil, err
	}
	df, err := NewDataFile(cfg.Datadir, cfg.MaxDataFileSize)
	if err != nil {
		return &DB{}, err
	}
	_, err = df.WriteHeader(config.Version, cfg.MaxDataFileSize)
	if err != nil {
		return &DB{}, err
	}
	kd, err := keydir.BuildKeyDir(cfg.Datadir)
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
	e := entries.NewEntry(key, value)
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

	return entry.Value, err
}

func (mq *DB) Shutdown() error {
	err := mq.df.Close()
	if err != nil {
		// TODO: log
		return err
	}
	return nil
}
