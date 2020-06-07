package storage

type Q interface {
	Enqueue(QName string, data []byte) error
	Dequeue(QName string) ([]byte, error)
}
