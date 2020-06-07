package storage

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

type Q interface {
	Enqueue(QName string, data []byte) error
	Dequeue(QName string) ([]byte, error)
}

type PotatoQ struct {
	f *os.File
}

func (q *PotatoQ) Enqueue(QName string, data []byte) error {
	return nil
}

func (q *PotatoQ) Dequeue(QName string) ([]byte, error) {
	return []byte{}, nil
}

func NewPotatoQ(dataDir string) (*PotatoQ, error) {
	p := path.Join(dataDir, "q.data")
	fd, err := os.OpenFile(p, syscall.O_RDWR, 0600)
	if err != nil {
		fmt.Printf("Could not open data file at: %s - check your permissions\n", p)
		return &PotatoQ{}, err
	}

	return &PotatoQ{f: fd}, nil

}
