package potatomq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	mq, err := NewDB("data")
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
}
