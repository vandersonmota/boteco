package potatomq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	mq := NewMQ()
	err := mq.Push("foo", []byte("fooo"))
	assert.Nil(t, err)
}
