package doltlite_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	doltlite "github.com/dolthub/doltlite-driver"
)

const sqliteNotFound = 12

type emptySource struct{}

func (emptySource) GetChunks(hashes [][20]byte) ([][]byte, error) {
	return make([][]byte, len(hashes)), nil
}

func setSource(t *testing.T, c interface{ Raw(func(any) error) error }, src doltlite.ChunkSource) {
	t.Helper()
	if err := c.Raw(func(dc any) error { return dc.(doltlite.ChunkSourceSetter).SetChunkSource(src) }); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSourceAbsentChunkIsNotFound(t *testing.T) {
	c := openConn(t, "file:/absent-chunk?vfs=memdb")
	setSource(t, c, emptySource{})

	var hash string
	err := c.QueryRowContext(context.Background(),
		`SELECT commit_hash FROM dolt_log('0123456789abcdef0123456789abcdef01234567') LIMIT 1`).Scan(&hash)
	var dlErr *doltlite.Error
	if !errors.As(err, &dlErr) || dlErr.Code&0xff != sqliteNotFound {
		t.Fatalf("missing commit through a chunk source = %v, want not found", err)
	}
}

func TestLazyFetchSkipsUnreadableRemoteTag(t *testing.T) {
	ctx := context.Background()
	srvDir := t.TempDir()

	up := openConn(t, "file:"+filepath.Join(srvDir, "up.db"))
	mustExec(t, up, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, up, `INSERT INTO t VALUES (1, 'a')`)
	mustExec(t, up, `SELECT dolt_commit('-Am', 'first')`)
	up.Close()
	data, err := os.ReadFile(filepath.Join(srvDir, "up.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srvDir, "fork.db"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	fork := openConn(t, "file:"+filepath.Join(srvDir, "fork.db"))
	mustExec(t, fork, `SELECT dolt_checkout('-b', 'tmp')`)
	mustExec(t, fork, `UPDATE t SET v = 'tagged' WHERE id = 1`)
	mustExec(t, fork, `SELECT dolt_commit('-am', 'tagged only')`)
	mustExec(t, fork, `SELECT dolt_tag('stray', 'tmp')`)
	mustExec(t, fork, `SELECT dolt_checkout('main')`)
	mustExec(t, fork, `SELECT dolt_branch('-D', 'tmp')`)
	mustExec(t, fork, `INSERT INTO t VALUES (2, 'b')`)
	mustExec(t, fork, `SELECT dolt_commit('-am', 'fork main')`)
	fork.Close()

	base := startRemotesrv(t, srvDir)
	upURL := base + "/up.db"

	lazy := openConn(t, "file:/lazy-fetch?vfs=memdb")
	setSource(t, lazy, httpSource{upURL})
	if err := lazy.Raw(func(dc any) error { return dc.(doltlite.LazyInitializer).InitLazy(getRefs(t, upURL)) }); err != nil {
		t.Fatal(err)
	}
	mustExec(t, lazy, `SELECT dolt_remote('add', 'fork', '`+base+`/fork.db')`)

	if _, err := lazy.ExecContext(ctx, `SELECT dolt_fetch('fork', 'main')`); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if n := queryInt(t, lazy, `SELECT count(*) FROM dolt_tags WHERE tag_name = 'stray'`); n != 0 {
		t.Fatalf("a tag whose commit was not fetched was installed")
	}
	if n := queryInt(t, lazy, `SELECT count(*) FROM dolt_log('fork/main')`); n != 3 {
		t.Fatalf("fetched fork/main has %d commits, want 3", n)
	}
}
