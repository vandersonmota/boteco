package potatomq

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/vandersonmota/potatomq/config"
)

type DBTestSuite struct {
	suite.Suite
	Datadir string
}

func (suite *DBTestSuite) SetupTest() {
	suite.Datadir = "data"
	os.RemoveAll(suite.Datadir)
}
func (suite *DBTestSuite) TearDownTest() {
	os.RemoveAll(suite.Datadir)
}

func (suite *DBTestSuite) TestPut() {
	t := suite.T()
	mq, err := NewDB(config.Config{
		Datadir: suite.Datadir,
	})
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("bar"))
	assert.Nil(t, err)
}
func (suite *DBTestSuite) TestGet() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir})
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	val, err := mq.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, []byte("fooo"), val)
}
func (suite *DBTestSuite) TestGetMultiple() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir})
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	err = mq.Put("spam", []byte("spam"))
	assert.Nil(t, err)
	err = mq.Put("eggs", []byte("eggs"))
	assert.Nil(t, err)
	err = mq.Put("bar", []byte("12345"))
	assert.Nil(t, err)
	val, err := mq.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, []byte("12345"), val)
}

func (suite *DBTestSuite) TestGetMultipleDataFiles() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 30})
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	err = mq.Put("bar", []byte("12345"))
	assert.Nil(t, err)
	val, err := mq.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, []byte("12345"), val)

	files, _ := ioutil.ReadDir(suite.Datadir)

	assert.Equal(t, 3, len(files))
}
func (suite *DBTestSuite) TestPutCloseAndGet() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 30})
	assert.Nil(t, err)
	err = mq.Put("bar", []byte("12345"))
	assert.Nil(t, err)

	size, err := mq.df.CurrentSize()
	assert.Nil(t, err)
	assert.Equal(t, size, int64(32)) // Size of file headers

	err = mq.Shutdown()
	assert.Nil(t, err)

	mq, err = NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 30})
	assert.Nil(t, err)
	val, err := mq.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, []byte("12345"), val)

}
func (suite *DBTestSuite) TestPutWriteDatafileHeaders() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 30})
	assert.Nil(t, err)
	size, err := mq.df.CurrentSize()

	assert.Nil(t, err)
	assert.Equal(t, size, int64(24)) // Size of file headers

	err = mq.Shutdown()
	assert.Nil(t, err)

}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(DBTestSuite))
}
