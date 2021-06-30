package buffer

import (
	"bytes"
	"mini-rdbms/lib/disk"
	"testing"
)

func TestBuffer_GetPage(t *testing.T) {
	type fields struct {
		PageID  PageID
		page    disk.Page
		ref     uint
		isDirty bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   [disk.PageSize]byte
		wantErr bool
	}{
		{
			"one character",
			fields{1, disk.Page([disk.PageSize]byte{97}), 1, false},
			[disk.PageSize]byte{97},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Buffer{
				PageID:  tt.fields.PageID,
				page:    tt.fields.page,
				ref:     tt.fields.ref,
				isDirty: tt.fields.isDirty,
			}
			w := &bytes.Buffer{}
			if err := b.GetPage(w); (err != nil) != tt.wantErr {
				t.Errorf("Buffer.GetPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if w.Len() != len(tt.wantW) {
				t.Errorf("Buffer.GetPage() len = %v, wantLen %v", w.Len(), len(tt.wantW))
				return
			}
			if !bytes.Equal(w.Bytes(), tt.wantW[:]) {
				t.Errorf("Buffer.GetPage() = %v, want %v", w.Bytes(), tt.wantW)
			}
		})
	}
}
