package btree

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
)

func Test_meta_Read(t *testing.T) {
	type fields struct {
		RootPageID      PageID
		FirstLeafPageID PageID
	}
	type args struct {
		dst []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"simple",
			fields{PageID(1), PageID(2)},
			args{make([]byte, 4098)},
			62,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &meta{
				RootPageID:      tt.fields.RootPageID,
				FirstLeafPageID: tt.fields.FirstLeafPageID,
			}
			got, err := n.Read(tt.args.dst)
			buffer := new(bytes.Buffer)
			enc := gob.NewEncoder(buffer)
			_ = enc.Encode(n)
			if (err != nil) != tt.wantErr {
				t.Errorf("meta.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("meta.Read() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.args.dst[:buffer.Len()-1], buffer.Bytes()) {
				t.Errorf("meta.Read() = %v, want %v", tt.args.dst[:buffer.Len()], buffer.Bytes())
			}
		})
	}
}

func Test_meta_Write(t *testing.T) {
	type fields struct {
		RootPageID      PageID
		FirstLeafPageID PageID
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"simple",
			fields{},
			args{[]byte{53, 255, 129, 3, 1, 1, 4, 109, 101, 116, 97, 1, 255, 130, 0, 1, 2, 1, 10, 82, 111, 111, 116, 80, 97, 103, 101, 73, 68, 1, 6, 0, 1, 15, 70, 105, 114, 115, 116, 76, 101, 97, 102, 80, 97, 103, 101, 73, 68, 1, 6, 0, 0, 0, 7, 255, 130, 1, 1, 1, 2, 0}},
			62,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &meta{
				RootPageID:      tt.fields.RootPageID,
				FirstLeafPageID: tt.fields.FirstLeafPageID,
			}
			got, err := n.Write(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("meta.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("meta.Write() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(*n, meta{1, 2}) {
				t.Errorf("meta.Write() = %v, want %v", *n, meta{1, 2})
			}
		})
	}
}
