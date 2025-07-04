package fs

import (
	"fmt"
	"math/rand/v2"
	"os"
)

// ┌───🬼  create   ┌───🬼   ┌───🬼  rename   ┌───🬼
//  1 │ ────────→  1 │ +  2 │ ────────→  2 │
// └───┘           └───┘   └───┘           └───┘
//
//	old             old     temp            new
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
