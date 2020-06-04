package potatomq

import (
	"fmt"
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
func (suite *DBTestSuite) TestUTF8Keys() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir})
	assert.Nil(t, err)
	err = mq.Put("cajá", []byte("manjericão"))
	assert.Nil(t, err)
	val, err := mq.Get("cajá")
	assert.Nil(t, err)
	assert.Equal(t, []byte("manjericão"), val)

	err = mq.Put("大", []byte("Heaven"))
	assert.Nil(t, err)
	val, err = mq.Get("大")
	assert.Nil(t, err)
	assert.Equal(t, []byte("Heaven"), val)
}
func (suite *DBTestSuite) TestBigKeysNotAllowed() {
	// limit is 255
	bigKey := `
	xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	uuuuuuuuuuuuuuuuuuuiiiiiiiiiiiiiiippppppppppppppooooo
	pipipippipiipipipiipipipiipiipiippipipipipipipipiipip
	`
	fmt.Println(len(bigKey))
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir})
	assert.Nil(t, err)
	err = mq.Put(bigKey, []byte("manjericão"))
	assert.Error(t, err)
	val, err := mq.Get(bigKey)
	assert.Nil(t, err)
	assert.Equal(t, make([]byte, 0), val)
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
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 64})
	assert.Nil(t, err)
	err = mq.Put("foo", []byte("fooo"))
	assert.Nil(t, err)
	err = mq.Put("bar", []byte("12345"))
	assert.Nil(t, err)
	val, err := mq.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, []byte("12345"), val)

	files, _ := ioutil.ReadDir(suite.Datadir)

	assert.Equal(t, 2, len(files))
}
func (suite *DBTestSuite) TestPutCloseAndGet() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 60})
	assert.Nil(t, err)
	err = mq.Put("bar", []byte("12345"))
	assert.Nil(t, err)

	size, err := mq.df.CurrentSize()
	assert.Nil(t, err)
	assert.Equal(t, size, int64(56)) // Size of file headers + record tuple

	err = mq.Shutdown()
	assert.Nil(t, err)

	mq, err = NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 30})
	assert.Nil(t, err)
	val, err := mq.Get("bar")
	assert.Nil(t, err)
	assert.Equal(t, []byte("12345"), val)

}

func (suite *DBTestSuite) TestWriteDatafileHeaders() {
	t := suite.T()
	mq, err := NewDB(config.Config{Datadir: suite.Datadir, MaxDataFileSize: 64})
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
