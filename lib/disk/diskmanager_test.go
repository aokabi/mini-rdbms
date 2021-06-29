package disk

import (
	"reflect"
	"testing"
)

func TestPageID_Bytes(t *testing.T) {
	tests := []struct {
		name string
		i    PageID
		want []byte
	}{
		{
			"zero",
			PageID(0),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"one",
			PageID(1),
			[]byte{1, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.i.Bytes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PageID.Bytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
