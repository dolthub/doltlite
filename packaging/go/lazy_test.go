package doltlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	doltlite "github.com/dolthub/doltlite-driver"
)

func openConn(t *testing.T, dsn string) *sql.Conn {
	t.Helper()
	db, err := sql.Open("doltlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustExec(t *testing.T, c *sql.Conn, q string) {
	t.Helper()
	if _, err := c.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func queryInt(t *testing.T, c *sql.Conn, q string) int {
	t.Helper()
	var n int
	if err := c.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// startRemotesrv serves dir with the doltlite-remotesrv binary named by
// DOLTLITE_REMOTESRV and returns its base URL.
func startRemotesrv(t *testing.T, dir string) string {
	t.Helper()
	bin := os.Getenv("DOLTLITE_REMOTESRV")
	if bin == "" {
		t.Skip("DOLTLITE_REMOTESRV not set")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "-p", fmt.Sprint(port), dir)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			return base
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("remotesrv did not start on port %d", port)
	return ""
}

func getRefs(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url + "/refs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(b) == 0 {
		t.Fatalf("GET %s/refs: %d %q", url, resp.StatusCode, b)
	}
	return b
}

// httpSource reads chunks from a doltlite-remotesrv database. A refs blob
// fetched over HTTP does not name origin, so until origin is recorded the
// lazy database needs another source to load the branch the blob points to.
type httpSource struct{ url string }

func (s httpSource) GetChunks(hashes [][20]byte) ([][]byte, error) {
	body := make([]byte, 0, len(hashes)*20)
	for _, h := range hashes {
		body = append(body, h[:]...)
	}
	resp, err := http.Post(s.url+"/get-chunks", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get-chunks: %d", resp.StatusCode)
	}
	out := make([][]byte, len(hashes))
	for i := range out {
		if len(b) < 4 {
			return nil, fmt.Errorf("get-chunks: short reply")
		}
		n := binary.BigEndian.Uint32(b)
		b = b[4:]
		if n == 0xFFFFFFFF {
			continue
		}
		if uint32(len(b)) < n {
			return nil, fmt.Errorf("get-chunks: short chunk")
		}
		out[i], b = b[:n], b[n:]
	}
	return out, nil
}

// installRefs installs refs into the database at path and records origin if it
// is not recorded yet, so a later open with lazy_origin=1 reads missing chunks
// from url.
func installRefs(t *testing.T, path, url string, refs []byte) {
	t.Helper()
	c := openConn(t, "file:"+path)
	err := c.Raw(func(dc any) error {
		if err := dc.(doltlite.ChunkSourceSetter).SetChunkSource(httpSource{url}); err != nil {
			return err
		}
		return dc.(doltlite.LazyInitializer).InitLazy(refs)
	})
	if err != nil {
		t.Fatalf("InitLazy: %v", err)
	}
	if queryInt(t, c, `SELECT count(*) FROM dolt_remotes WHERE name = 'origin'`) == 0 {
		mustExec(t, c, fmt.Sprintf(`SELECT dolt_remote('add', 'origin', '%s')`, url))
	}
	if err := c.Raw(func(dc any) error {
		return dc.(doltlite.ChunkSourceSetter).SetChunkSource(nil)
	}); err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestInitLazy(t *testing.T) {
	srvDir := t.TempDir()
	lazyPath := filepath.Join(t.TempDir(), "lazy.db")

	src := openConn(t, "file:"+filepath.Join(srvDir, "src.db"))
	mustExec(t, src, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, src, `WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<5000)
		INSERT INTO t SELECT i, printf('%0200d', i) FROM c`)
	mustExec(t, src, `SELECT dolt_commit('-Am', 'first')`)

	url := startRemotesrv(t, srvDir) + "/src.db"
	installRefs(t, lazyPath, url, getRefs(t, url))

	// The table's rows were never cached: without lazy_origin they are
	// missing, with it they come from origin.
	plain := openConn(t, "file:"+lazyPath)
	if _, err := plain.ExecContext(context.Background(), `SELECT sum(length(v)) FROM t`); err == nil {
		t.Fatalf("uncached rows read without lazy_origin")
	}
	plain.Close()
	lazy := openConn(t, "file:"+lazyPath+"?lazy_origin=1")
	if n := queryInt(t, lazy, `SELECT count(*) FROM t WHERE length(v)=200`); n != 5000 {
		t.Fatalf("lazy rows = %d, want 5000", n)
	}
	logBefore := queryInt(t, lazy, `SELECT count(*) FROM dolt_log`)
	lazy.Close()

	mustExec(t, src, `INSERT INTO t VALUES (5001, 'new')`)
	mustExec(t, src, `SELECT dolt_commit('-am', 'second')`)
	installRefs(t, lazyPath, url, getRefs(t, url))

	lazy = openConn(t, "file:"+lazyPath+"?lazy_origin=1")
	if n := queryInt(t, lazy, `SELECT count(*) FROM t`); n != 5001 {
		t.Fatalf("lazy rows after refresh = %d, want 5001", n)
	}
	if n := queryInt(t, lazy, `SELECT count(*) FROM dolt_log`); n != logBefore+1 {
		t.Fatalf("lazy log after refresh = %d, want %d", n, logBefore+1)
	}
	var msg string
	if err := lazy.QueryRowContext(context.Background(),
		`SELECT message FROM dolt_log LIMIT 1`).Scan(&msg); err != nil || msg != "second" {
		t.Fatalf("latest commit = %q, %v", msg, err)
	}

	err := lazy.Raw(func(dc any) error {
		return dc.(doltlite.LazyInitializer).InitLazy(nil)
	})
	if err == nil {
		t.Fatalf("InitLazy(nil) succeeded")
	}
}
