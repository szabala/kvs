package fs

import (
	"fmt"
	"math/rand/v2"
	"os"
)

// SaveData atomically saves data to a file at the specified path.
// It creates a temporary file with a unique name, writes the data to it,
// and then renames it to the original file name. This ensures that the
// original file is not modified until the new data is fully written,
// preventing data corruption in case of a failure during the write process.
func SaveData(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Int32())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	if _, err = fp.Write(data); err != nil {
		return err
	}
	if err = fp.Sync(); err != nil {
		return err
	}
	err = os.Rename(tmp, path)
	if err != nil {
		return err
	}
	dir, err := os.OpenFile(".", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return err
	}
	return nil

}
