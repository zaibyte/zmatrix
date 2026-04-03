package neo

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zaibyte/zaipkg/xmath/xrand"
)

func TestCreateStartLv0(t *testing.T) {

	fs := testFS

	dbPath := filepath.Join(os.TempDir(), "neo.lv1", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(dbPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(dbPath)

	l, err := createLv0(dbPath, fs)
	if err != nil {
		t.Fatal(err)
	}

	err = l.set([]byte("a"), []byte("b"))
	if err != nil {
		t.Fatal(err)
	}

	err = l.close()
	if err != nil {
		t.Fatal(err)
	}

	ll, err := loadLv0(dbPath, fs, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ll.close()

	v, closer, err := ll.get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	assert.Equal(t, []byte("b"), v)
}
