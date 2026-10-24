// Package-level smoke test for doltlite-driver. Runs from a consumer module
// against the staged package, covering the driver surface end to end: every
// value type, transactions, error paths, and a version-control round trip
// (commit, branch, checkout, merge, dolt_log reads).
package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	check("build version reports", doltlite.BuildVersion != "", true)

	db, err := sql.Open("doltlite", dbPath)
	must(err)
	defer db.Close()
	var buildVersion string
	must(db.QueryRow(`SELECT dolt_version()`).Scan(&buildVersion))
	check("engine version is stamped", buildVersion, doltlite.BuildVersion)

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

	_, err = db.Exec(`CREATE TABLE m(pk INTEGER PRIMARY KEY, s TEXT);
		INSERT INTO m VALUES (1, 'a');`)
	must(err)
	err = db.QueryRow(`SELECT s FROM m`).Scan(&s)
	check("multi-statement exec runs every statement", err == nil && s == "a", true)
	res, err = db.Exec(`DELETE FROM m;
		INSERT INTO m VALUES (?, ?); -- first row
		INSERT INTO m VALUES (?, 'b'), (3, 'c');`, 1, "a", 2)
	must(err)
	n, err = res.RowsAffected()
	must(err)
	check("multi-statement exec rows affected", n, int64(2))
	must(db.QueryRow(`SELECT group_concat(s, '') FROM (SELECT s FROM m ORDER BY pk)`).Scan(&s))
	check("multi-statement exec binds args in order", s, "abc")
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'd'); INSERT INTO m VALUES (?, 'e')`, 4)
	check("multi-statement exec with too few args errors", err != nil, true)
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'f')`, 6, 7)
	check("exec with extra args errors", err != nil, true)
	var nRow int
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'z'); -- trailing`, 8, 9)
	check("trailing comment with extra args errors", err != nil, true)
	must(db.QueryRow(`SELECT count(*) FROM m WHERE pk = 8`).Scan(&nRow))
	check("trailing comment with extra args does not write", nRow, 0)
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'y');;`, 9, 10)
	check("extra semicolon with extra args errors", err != nil, true)
	must(db.QueryRow(`SELECT count(*) FROM m WHERE pk = 9`).Scan(&nRow))
	check("extra semicolon with extra args does not write", nRow, 0)
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'w'); /* done */`, 10, 11)
	check("block comment with extra args errors", err != nil, true)
	must(db.QueryRow(`SELECT count(*) FROM m WHERE pk = 10`).Scan(&nRow))
	check("block comment with extra args does not write", nRow, 0)
	_, err = db.Exec(`INSERT INTO m VALUES (?, 'kept'); -- note`, 12)
	must(err)
	must(db.QueryRow(`SELECT s FROM m WHERE pk = 12`).Scan(&s))
	check("trailing comment with exact args runs", s, "kept")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.ExecContext(ctx, `INSERT INTO m VALUES (11, 'no')`)
	check("cancelled exec errors", err != nil, true)
	must(db.QueryRow(`SELECT count(*) FROM m WHERE pk = 11`).Scan(&nRow))
	check("cancelled exec does not write", nRow, 0)
	_, err = db.Exec(` -- nothing`)
	check("exec with no statement errors", err != nil, true)
	_, err = db.Exec(`INSERT INTO m VALUES (:pk, 'g')`, sql.Named("pk", 7))
	check("exec with named args errors", err != nil, true)
	_, err = db.Exec(`DELETE FROM m; DELETE FROM m WHERE pk = ?`, 1, 2)
	check("multi-statement exec with extra args errors", err != nil, true)

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
	closedConnCleanup(dbPath)

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

// A statement can outlive the connection that made it. Closing it must return
// rather than block: the connection's thread is gone by then, so a cleanup
// call with nowhere to run would wedge the goroutine for good.
func closedConnCleanup(dbPath string) {
	path := dbPath + ".closed"
	os.Remove(path)
	defer os.Remove(path)

	c, err := doltlite.Driver{}.Open(path)
	if err != nil {
		fmt.Println("  FAIL: open for close test:", err)
		failures++
		return
	}
	st, err := c.Prepare("SELECT 1")
	if err != nil {
		fmt.Println("  FAIL: prepare for close test:", err)
		failures++
		c.Close()
		return
	}
	if err := c.Close(); err != nil {
		fmt.Println("  FAIL: conn close:", err)
		failures++
		return
	}

	done := make(chan error, 1)
	go func() { done <- st.(driver.Stmt).Close() }()
	select {
	case <-done:
		check("statement close after connection close returns", true, true)
	case <-time.After(10 * time.Second):
		fmt.Println("  FAIL: statement close after connection close blocked")
		failures++
	}

	// Rows outliving the connection: must error rather than block or read a
	// finalized statement.
	c2, err := doltlite.Driver{}.Open(path)
	if err != nil {
		fmt.Println("  FAIL: reopen for rows test:", err)
		failures++
		return
	}
	st2, err := c2.Prepare("SELECT 1")
	if err != nil {
		fmt.Println("  FAIL: prepare for rows test:", err)
		failures++
		c2.Close()
		return
	}
	rws, err := st2.(driver.Stmt).Query(nil)
	if err != nil {
		fmt.Println("  FAIL: query for rows test:", err)
		failures++
		c2.Close()
		return
	}
	c2.Close()
	rowsDone := make(chan error, 1)
	go func() {
		dest := make([]driver.Value, 1)
		rowsDone <- rws.Next(dest)
	}()
	select {
	case err := <-rowsDone:
		check("rows read after connection close errors", err != nil, true)
	case <-time.After(10 * time.Second):
		fmt.Println("  FAIL: rows read after connection close blocked")
		failures++
	}
	rws.Close()
}
