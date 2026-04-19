//go:build !linux

package golog

import "os"

func fdatasync(f *os.File) error {
	return f.Sync()
}
