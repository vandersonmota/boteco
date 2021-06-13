package boteco

import (
	"os"

	"github.com/vandersonmota/boteco/config"
	"github.com/vandersonmota/boteco/entries"
	"github.com/vandersonmota/boteco/keydir"
)

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
	e, err := entries.NewEntry(key, value)
	if err != nil {
		return err
	}
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
	if item.IsEmpty() {
		return make([]byte, 0), nil
	}
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
