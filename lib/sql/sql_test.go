package sql_test

import (
	"mini-rdbms/lib/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	x := []byte("x")
	result := sql.Encode(x)
	assert.Equal(t, []byte{0x78, 0, 0, 0, 0, 0, 0, 0, 0x1}, result)
}

func TestEncodeElems(t *testing.T) {
	value := [][]byte{[]byte("Bob"), []byte("Johnson")}
	result := sql.EncodeElems(value)
	assert.Equal(t, []byte{0x42, 0x6f, 0x62, 0, 0, 0, 0, 0, 0x03, 0x4a, 0x6f, 0x68, 0x6e, 0x73, 0x6f, 0x6e, 0, 0x07}, result)
}

func TestDecode(t *testing.T) {
	result := sql.Decode([]byte{0x78, 0, 0, 0, 0, 0, 0, 0, 0x1})
	assert.Equal(t, []byte("x"), result)
	result = sql.Decode([]byte{0x42, 0x6f, 0x62, 0x4a, 0x6f, 0x68, 0x6e, 0x73, 0x09, 0x6f, 0x6e, 0, 0, 0, 0, 0, 0, 0x02})
	assert.Equal(t, []byte("BobJohnson"), result)
}

func TestDecodeElems(t *testing.T) {
	result := sql.DecodeElems([]byte{0x42, 0x6f, 0x62, 0, 0, 0, 0, 0, 0x03, 0x4a, 0x6f, 0x68, 0x6e, 0x73, 0x6f, 0x6e, 0, 0x07})
	assert.Equal(t, [][]byte{[]byte("Bob"), []byte("Johnson")}, result)
}
