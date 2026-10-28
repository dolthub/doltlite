package doltlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	doltlite "github.com/dolthub/doltlite-driver"
)

func openGuardedConn(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("doltlite", "file:"+filepath.Join(t.TempDir(), "guards.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, stmt := range []string{
		"CREATE TABLE open(id INTEGER PRIMARY KEY, v TEXT)",
		"CREATE TABLE secret(id INTEGER PRIMARY KEY, v TEXT)",
		"INSERT INTO open VALUES (1, 'a')",
		"INSERT INTO secret VALUES (1, 'b')",
		"CREATE VIEW peek AS SELECT v FROM secret",
	} {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return conn
}

func raw[T any](t *testing.T, conn *sql.Conn, fn func(T) error) {
	t.Helper()
	if err := conn.Raw(func(dc any) error { return fn(dc.(T)) }); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizer(t *testing.T) {
	ctx := context.Background()
	conn := openGuardedConn(t)

	var reads []string
	raw(t, conn, func(s doltlite.AuthorizerSetter) error {
		return s.SetAuthorizer(func(action int, arg1, arg2, database, trigger string) int {
			switch {
			case action == doltlite.AuthRead && arg1 == "secret":
				return doltlite.AuthDeny
			case action == doltlite.AuthRead:
				reads = append(reads, arg1+"."+arg2)
			case action == doltlite.AuthInsert:
				return doltlite.AuthDeny
			}
			return doltlite.AuthOK
		})
	})

	var v string
	if err := conn.QueryRowContext(ctx, "SELECT v FROM open").Scan(&v); err != nil || v != "a" {
		t.Fatalf("allowed read: %q, %v", v, err)
	}
	if len(reads) == 0 || reads[0] != "open.v" {
		t.Errorf("authorizer saw reads %v", reads)
	}
	for _, query := range []string{"SELECT v FROM secret", "SELECT v FROM peek"} {
		if err := conn.QueryRowContext(ctx, query).Scan(&v); !isAuthError(err) {
			t.Errorf("%s: want an authorization error, got %v", query, err)
		}
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO open VALUES (2, 'c')"); !isAuthError(err) {
		t.Errorf("denied insert: want an authorization error, got %v", err)
	}

	raw(t, conn, func(s doltlite.AuthorizerSetter) error { return s.SetAuthorizer(nil) })
	if err := conn.QueryRowContext(ctx, "SELECT v FROM secret").Scan(&v); err != nil || v != "b" {
		t.Fatalf("after removing the authorizer: %q, %v", v, err)
	}
}

func TestSetLimit(t *testing.T) {
	ctx := context.Background()
	conn := openGuardedConn(t)

	raw(t, conn, func(l doltlite.Limiter) error {
		prev, err := l.SetLimit(doltlite.LimitLength, 1<<20)
		if err == nil && prev <= 1<<20 {
			t.Errorf("previous length limit %d", prev)
		}
		return err
	})

	var n int
	err := conn.QueryRowContext(ctx, "SELECT length(randomblob(2000000))").Scan(&n)
	if err == nil || !strings.Contains(err.Error(), "too big") {
		t.Fatalf("over the limit: %d, %v", n, err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT length(randomblob(1000))").Scan(&n); err != nil || n != 1000 {
		t.Fatalf("under the limit: %d, %v", n, err)
	}
	raw(t, conn, func(l doltlite.Limiter) error {
		cur, err := l.SetLimit(doltlite.LimitLength, -1)
		if err == nil && cur != 1<<20 {
			t.Errorf("reading the limit returned %d", cur)
		}
		return err
	})
}

const endless = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

func TestContextInterruptsQuery(t *testing.T) {
	conn := openGuardedConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	var n int
	err := conn.QueryRowContext(ctx, endless).Scan(&n)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("query ran %v past its deadline", elapsed)
	}

	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM open").Scan(&n); err != nil || n != 1 {
		t.Fatalf("connection after an interrupt: %d, %v", n, err)
	}
}

func TestContextInterruptsExec(t *testing.T) {
	conn := openGuardedConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.ExecContext(ctx, "CREATE TABLE big AS "+endless)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "INSERT INTO open VALUES (2, 'c')"); err != nil {
		t.Fatalf("connection after an interrupt: %v", err)
	}
}

func TestContextInterruptsRowsInProgress(t *testing.T) {
	conn := openGuardedConn(t)

	ctx, cancel := context.WithCancel(context.Background())
	rows, err := conn.QueryContext(ctx, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT x FROM c")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3 && rows.Next(); i++ {
	}
	cancel()
	for rows.Next() {
	}
	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	rows.Close()
}

func TestCloseDuringInterrupt(t *testing.T) {
	db, err := sql.Open("doltlite", "file:"+filepath.Join(t.TempDir(), "close.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxIdleConns(0)
	for i := 0; i < 20; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i)*time.Millisecond)
		var n int
		_ = conn.QueryRowContext(ctx, endless).Scan(&n)
		conn.Close()
		cancel()
	}
	db.Close()
}

func isAuthError(err error) bool {
	var e *doltlite.Error
	return errors.As(err, &e) && e.Code == 23
}
