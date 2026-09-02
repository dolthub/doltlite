// Package-level smoke test for doltlite-driver. Runs from a consumer module
// against the staged package, covering the driver surface end to end: every
// value type, transactions, error paths, and a version-control round trip
// (commit, branch, checkout, merge, dolt_log reads).
package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	doltlite "github.com/dolthub/doltlite-driver"
)

var failures int

func check(name string, got, want any) {
	ok := fmt.Sprint(got) == fmt.Sprint(want)
	if b1, k1 := got.([]byte); k1 {
		if b2, k2 := want.([]byte); k2 {
			ok = bytes.Equal(b1, b2)
		}
	}
	if ok {
		fmt.Printf("  PASS: %s\n", name)
		return
	}
	failures++
	fmt.Printf("  FAIL: %s\n    want: %v\n    got:  %v\n", name, want, got)
}

func main() {
	dir := os.TempDir()
	dbPath := filepath.Join(dir, fmt.Sprintf("doltlite-driver-smoke-%d.db", os.Getpid()))
	cleanup := func() {
		os.Remove(dbPath)
		os.Remove(filepath.Join(dir, "."+filepath.Base(dbPath)+"-lock"))
	}
	cleanup()
	defer cleanup()

	check("engine version reports", doltlite.Version() != "", true)

	db, err := sql.Open("doltlite", dbPath)
	must(err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t(pk INTEGER PRIMARY KEY, s TEXT, f REAL, b BLOB)`)
	must(err)

	res, err := db.Exec(`INSERT INTO t(pk, s, f, b) VALUES (?, ?, ?, ?)`,
		1, "one", 1.5, []byte{0, 255, 16})
	must(err)
	n, err := res.RowsAffected()
	must(err)
	check("insert rows affected", n, int64(1))
	id, err := res.LastInsertId()
	must(err)
	check("last insert id", id, int64(1))

	_, err = db.Exec(`INSERT INTO t(pk, s, f, b) VALUES (?, ?, ?, ?)`,
		2, "twö", 2.25, []byte{1, 0, 2})
	must(err)
	_, err = db.Exec(`INSERT INTO t(pk, s) VALUES (?, ?)`, 3, nil)
	must(err)

	var s string
	var f float64
	var b []byte
	must(db.QueryRow(`SELECT s, f, b FROM t WHERE pk = 2`).Scan(&s, &f, &b))
	check("text round trip (utf8)", s, "twö")
	check("float round trip", f, 2.25)
	check("blob round trip", b, []byte{1, 0, 2})

	var ns sql.NullString
	must(db.QueryRow(`SELECT s FROM t WHERE pk = 3`).Scan(&ns))
	check("null round trip", ns.Valid, false)

	var empty string
	must(db.QueryRow(`SELECT ''`).Scan(&empty))
	check("empty string binds", empty, "")

	err = db.QueryRow(`SELECT s FROM t WHERE pk = 99`).Scan(&s)
	check("no rows is ErrNoRows", err == sql.ErrNoRows, true)

	rows, err := db.Query(`SELECT pk, s FROM t ORDER BY pk`)
	must(err)
	cols, err := rows.Columns()
	must(err)
	check("column names", fmt.Sprint(cols), "[pk s]")
	count := 0
	for rows.Next() {
		count++
	}
	must(rows.Err())
	rows.Close()
	check("row count", count, 3)

	_, err = db.Query(`SELECT * FROM missing_table`)
	check("bad SQL errors", err != nil, true)

	// Transactions.
	tx, err := db.Begin()
	must(err)
	_, err = tx.Exec(`INSERT INTO t(pk, s) VALUES (?, ?)`, 4, "rolled back")
	must(err)
	must(tx.Rollback())
	var c int
	must(db.QueryRow(`SELECT count(*) FROM t WHERE pk = 4`).Scan(&c))
	check("rollback discards", c, 0)

	tx, err = db.Begin()
	must(err)
	_, err = tx.Exec(`INSERT INTO t(pk, s) VALUES (?, ?)`, 5, "committed")
	must(err)
	must(tx.Commit())
	must(db.QueryRow(`SELECT count(*) FROM t WHERE pk = 5`).Scan(&c))
	check("commit persists", c, 1)

	// Version control is plain SQL on the same connection.
	var commit string
	must(db.QueryRow(`SELECT dolt_commit('-A', '-m', ?)`, "first commit").Scan(&commit))
	check("dolt_commit", commit != "", true)

	scalar := func(q string) string {
		var v sql.NullString
		must(db.QueryRow(q).Scan(&v))
		return v.String
	}
	check("dolt_branch", scalar(`SELECT dolt_branch('feature')`), "0")
	check("dolt_checkout feature", scalar(`SELECT dolt_checkout('feature')`), "0")
	_, err = db.Exec(`UPDATE t SET s = 'branched' WHERE pk = 1`)
	must(err)
	check("dolt_commit on branch",
		scalar(`SELECT dolt_commit('-A', '-m', 'feature work')`) != "", true)
	check("dolt_checkout main", scalar(`SELECT dolt_checkout('main')`), "0")
	check("main unchanged", scalar(`SELECT s FROM t WHERE pk = 1`), "one")
	scalar(`SELECT dolt_merge('feature')`)
	check("dolt_merge fast-forwards", scalar(`SELECT s FROM t WHERE pk = 1`), "branched")
	var logCount int
	must(db.QueryRow(`SELECT count(*) FROM dolt_log`).Scan(&logCount))
	check("dolt_log sees both commits", logCount >= 2, true)

	// Transactions through the pool, which is how database/sql is actually
	// used. The engine attributes its store lock to the thread that took it,
	// so without the driver pinning a thread per connection these fail with
	// "database is locked" -- every time at MaxOpenConns(1), where nothing is
	// even concurrent.
	concurrentTx(dbPath)

	if failures == 0 {
		fmt.Println("smoke-test: all checks passed")
		return
	}
	fmt.Printf("smoke-test: %d failure(s)\n", failures)
	os.Exit(1)
}

func must(err error) {
	if err != nil {
		fmt.Println("fatal:", err)
		os.Exit(1)
	}
}

func concurrentTx(dbPath string) {
	for _, maxConns := range []int{1, 4, 16} {
		db, err := sql.Open("doltlite", dbPath)
		if err != nil {
			fmt.Printf("  FAIL: pool open (max %d): %v\n", maxConns, err)
			failures++
			continue
		}
		db.SetMaxOpenConns(maxConns)
		table := fmt.Sprintf("pool%d", maxConns)
		if _, err := db.Exec(`CREATE TABLE ` + table + `(pk INTEGER PRIMARY KEY)`); err != nil {
			fmt.Printf("  FAIL: pool table (max %d): %v\n", maxConns, err)
			failures++
			db.Close()
			continue
		}

		const writers = 16
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tx, err := db.Begin()
				if err != nil {
					errs <- err
					return
				}
				if _, err := tx.Exec(`INSERT INTO `+table+`(pk) VALUES (?)`, i); err != nil {
					tx.Rollback()
					errs <- err
					return
				}
				if err := tx.Commit(); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)

		failed := 0
		var first error
		for err := range errs {
			if first == nil {
				first = err
			}
			failed++
		}
		if failed > 0 {
			fmt.Printf("  FAIL: %d of %d pooled transactions (max %d conns): %v\n",
				failed, writers, maxConns, first)
			failures++
			db.Close()
			continue
		}
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
			fmt.Printf("  FAIL: pool count (max %d): %v\n", maxConns, err)
			failures++
			db.Close()
			continue
		}
		check(fmt.Sprintf("%d pooled transactions (max %d conns)", writers, maxConns),
			n, writers)
		db.Close()
	}
}
