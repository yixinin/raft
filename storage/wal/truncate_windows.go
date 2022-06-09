package wal

import "os"

func (w *recordWriter) Truncate(offset int64) error {
	w.Lock()
	defer w.Unlock()
	var filename = w.f.Name()
	if err := w.f.Close(); err != nil {
		return err
	}
	err := os.Truncate(filename, offset)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	*w.f = *f
	w.offset = offset
	_, err = w.f.Seek(offset, os.SEEK_SET)
	return err
}
