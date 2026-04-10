// Doltlite Quickstart — Go
//
// Demonstrates Dolt version control features using Go's database/sql with
// the mattn/go-sqlite3 driver. Doltlite is a drop-in replacement for SQLite,
// so the standard driver works unchanged — just link against libdoltlite.
//
// Build (from the build/ directory):
//
//	cd ../examples/go
//	CGO_CFLAGS="-I../../build" CGO_LDFLAGS="-L../../build -ldoltlite -lz" \
//	    go build -tags libsqlite3 -o quickstart .
//	LD_LIBRARY_PATH=../../build ./quickstart
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

const dbPath = "/tmp/doltlite_quickstart_go.db"

func exec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("SQL error: %s\n  Statement: %s\n", err, query)
	}
}

func queryVal(db *sql.DB, query string) string {
	var val string
	if err := db.QueryRow(query).Scan(&val); err != nil {
		log.Fatalf("Query error: %s\n  Statement: %s\n", err, query)
	}
	return val
}

func main() {
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Force a single connection so branch state is preserved across queries.
	db.SetMaxOpenConns(1)

	// ---- 1. Create schema and first commit ----
	fmt.Println("== Create schema and first commit ==\n")
	exec(db, `CREATE TABLE employees(
		id INTEGER PRIMARY KEY,
		first_name TEXT,
		last_name TEXT
	)`)
	exec(db, "INSERT INTO employees VALUES(0, 'Tim',   'Sehn')")
	exec(db, "INSERT INTO employees VALUES(1, 'Brian', 'Hendriks')")
	exec(db, "INSERT INTO employees VALUES(2, 'Aaron', 'Son')")

	hash := queryVal(db, "SELECT dolt_commit('-A', '-m', 'Initial schema and data')")
	fmt.Printf("  commit: %s\n\n", hash)

	fmt.Println("Log after first commit:")
	rows, _ := db.Query("SELECT commit_hash, message FROM dolt_log")
	for rows.Next() {
		var h, m string
		rows.Scan(&h, &m)
		fmt.Printf("  %s... %s\n", h[:12], m)
	}
	rows.Close()
	fmt.Println()

	// ---- 2. Create a branch and make changes ----
	fmt.Println("== Branch and modify ==\n")
	exec(db, "SELECT dolt_branch('feature')")
	exec(db, "SELECT dolt_checkout('feature')")

	exec(db, "UPDATE employees SET first_name='Timothy' WHERE id=0")
	exec(db, "INSERT INTO employees VALUES(3, 'Daylon', 'Wilkins')")

	hash = queryVal(db, "SELECT dolt_commit('-A', '-m', 'Rename Tim, add Daylon')")
	fmt.Printf("  commit: %s\n\n", hash)

	// ---- 3. Point-in-time query ----
	fmt.Println("== Point-in-time query (main vs feature) ==\n")

	fmt.Println("Employees on feature (current branch):")
	rows, _ = db.Query("SELECT id, first_name, last_name FROM employees ORDER BY id")
	for rows.Next() {
		var id int
		var first, last string
		rows.Scan(&id, &first, &last)
		fmt.Printf("  %d: %s %s\n", id, first, last)
	}
	rows.Close()
	fmt.Println()

	fmt.Println("Employees on main (without switching):")
	rows, _ = db.Query("SELECT * FROM dolt_at_employees('main') ORDER BY id")
	for rows.Next() {
		var id int
		var first, last string
		rows.Scan(&id, &first, &last)
		fmt.Printf("  %d: %s %s\n", id, first, last)
	}
	rows.Close()
	fmt.Println()

	// ---- 4. Merge ----
	fmt.Println("== Merge feature into main ==\n")
	exec(db, "SELECT dolt_checkout('main')")
	exec(db, "SELECT dolt_merge('feature')")

	fmt.Println("Employees on main after merge:")
	rows, _ = db.Query("SELECT id, first_name, last_name FROM employees ORDER BY id")
	for rows.Next() {
		var id int
		var first, last string
		rows.Scan(&id, &first, &last)
		fmt.Printf("  %d: %s %s\n", id, first, last)
	}
	rows.Close()
	fmt.Println()

	// ---- 5. Audit log ----
	fmt.Println("== Audit log: all changes to employees ==\n")
	rows, _ = db.Query("SELECT diff_type, to_id, to_first_name, to_last_name FROM dolt_diff_employees")
	for rows.Next() {
		var diffType, first, last string
		var id int
		rows.Scan(&diffType, &id, &first, &last)
		fmt.Printf("  %-10s id=%d %s %s\n", diffType, id, first, last)
	}
	rows.Close()
	fmt.Println()

	// ---- 6. Commit log ----
	fmt.Println("== Full commit log ==\n")
	rows, _ = db.Query("SELECT commit_hash, message FROM dolt_log")
	for rows.Next() {
		var h, m string
		rows.Scan(&h, &m)
		fmt.Printf("  %s... %s\n", h[:12], m)
	}
	rows.Close()
	fmt.Println()

	// ---- 7. Tag ----
	fmt.Println("== Tag current state ==\n")
	exec(db, "SELECT dolt_tag('v1.0')")
	rows, _ = db.Query("SELECT tag_name, tag_hash FROM dolt_tags")
	for rows.Next() {
		var name, h string
		rows.Scan(&name, &h)
		fmt.Printf("  %s: %s...\n", name, h[:12])
	}
	rows.Close()
	fmt.Println()

	fmt.Println("Done.")
}
