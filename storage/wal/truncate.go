//go:build darwin && linux
// +build darwin,linux

package wal

import "os"

func (w *recordWriter) Truncate(offset int64) error {
	w.Lock()
	defer w.Unlock()
	if err := w.f.Truncate(offset); err != nil {
		return err
	}
	w.offset = offset
	_, err = w.f.Seek(offset, os.SEEK_SET)
	return err
}
