package config

const Version = 1

type Config struct {
	Datadir         string
	MaxDataFileSize int // bytes
}

const (
	KeySize         = 4
	ValueSize       = 8
	ChecksumSize    = 4
	TimestampSize   = 8
	FileIDSize      = 8
	EntryHeaderSize = ChecksumSize + TimestampSize + KeySize + ValueSize
)
