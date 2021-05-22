package keydir

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/vandersonmota/potatomq/config"
	"github.com/vandersonmota/potatomq/entries"
)

const fileHeaderSize = 24

type Item struct {
	FileID int
	Offset int
	Size   int
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
		FileID: fileID,
		Offset: offset - size, //Means file was just written, then offset moved
		Size:   size,
	}
}

type EntryLocation struct {
	key    string
	offset int
	size   int
}

type DataFileMapping struct {
	id   uint64
	path string
}

func (mapping *DataFileMapping) listEntriesLocation() ([]EntryLocation, error) {
	entriesLocation := []EntryLocation{}
	fd, err := os.OpenFile(mapping.path, os.O_RDONLY, 0665)
	defer fd.Close()
	if err != nil {
		return entriesLocation, err
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		return entriesLocation, err
	}

	if fileInfo.Size() < fileHeaderSize {
		return entriesLocation, errors.New("Corrupted file")
	}

	offset := fileHeaderSize
	for {
		buf := make([]byte, config.EntryHeaderSize)
		_, err = fd.ReadAt(buf, int64(offset))
		if err != nil {
			if err == io.EOF {
				break
			}
			return entriesLocation, err
		}
		h, err := entries.RebuildHeaders(buf)
		if err != nil {
			return entriesLocation, err
		}

		// need to readthe file sequentially, for all records
		keyBuf := make([]byte, h.KeySize)
		_, err = fd.ReadAt(keyBuf, int64(offset+config.EntryHeaderSize))
		if err != nil {
			if err == io.EOF {
				break
			}
			return entriesLocation, err
		}

		// This is dangerous, need to normalize everything to an integer type
		size := config.EntryHeaderSize + int(h.KeySize) + int(h.ValueSize)
		key := string(keyBuf[:])

		offset += size
		entriesLocation = append(entriesLocation, EntryLocation{key, offset, size})
	}

	return entriesLocation, nil
}

func listFileMappings(path string) ([]DataFileMapping, error) {
	fileMappings := []DataFileMapping{}
	datafiles, err := ioutil.ReadDir(path)
	if err != nil {
		return fileMappings, err
	}

	// TODO strong candidate for paralellization
	for _, f := range datafiles {

		path := filepath.Join(path, fmt.Sprint(f.Name()))
		if err != nil {
			return fileMappings, err
		}

		id, err := strconv.ParseUint(f.Name(), 10, 64)
		if err != nil {
			return fileMappings, err
		}
		fileMappings = append(fileMappings, DataFileMapping{id: id, path: path})
	}

	sort.SliceStable(fileMappings, func(l, r int) bool {
		return fileMappings[l].id < fileMappings[r].id
	})

	return fileMappings, nil
}

func BuildKeyDir(path string) (KeyDir, error) {
	kd := KeyDir{
		m: map[string]Item{},
	}

	fileMappings, err := listFileMappings(path)
	if err != nil {
		// Better error handling
		return kd, err
	}

	for _, mapping := range fileMappings {
		locations, err := mapping.listEntriesLocation()
		if err != nil {
			// TODO: better error handling
			return kd, nil
		}
		for _, location := range locations {
			kd.Set(location.key, location.offset, location.size, int(mapping.id))
		}
	}

	return kd, nil

}
