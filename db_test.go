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
	err = mq.Put("foo", []byte("bar"))
	assert.Nil(t, err)
}
func TestGet(t *testing.T) {
	mq, err := NewDB("data2")
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	val, err := mq.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, []byte("fooo"), val)
}
