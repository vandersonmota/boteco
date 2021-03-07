package potatomq

const Version = 1

type Config struct {
	Datadir         string
	MaxDataFileSize int // bytes
}
