//! DoltLite for Rust: SQLite with Git-style version control.
//!
//! The engine is compiled into this crate, so there is no system library to
//! install or point at. Version control is plain SQL on the connection:
//!
//! ```no_run
//! use doltlite::{Connection, Value};
//!
//! let db = Connection::open("app.db")?;
//! db.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)")?;
//! db.execute("INSERT INTO users(name) VALUES (?)", &[Value::from("Ada")])?;
//! db.query_row("SELECT dolt_commit('-A', '-m', ?)", &[Value::from("add ada")])?;
//! # Ok::<(), doltlite::Error>(())
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::ptr;

mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    pub enum Sqlite3 {}
    pub enum Stmt {}

    pub const OK: c_int = 0;
    pub const ROW: c_int = 100;
    pub const DONE: c_int = 101;

    pub const INTEGER: c_int = 1;
    pub const FLOAT: c_int = 2;
    pub const TEXT: c_int = 3;
    pub const BLOB: c_int = 4;
    pub const NULL: c_int = 5;

    pub const OPEN_READWRITE: c_int = 0x00000002;
    pub const OPEN_CREATE: c_int = 0x00000004;

    // Tells the engine to copy bound text and blobs rather than borrow them.
    pub const TRANSIENT: isize = -1;

    extern "C" {
        pub fn sqlite3_open_v2(
            filename: *const c_char,
            db: *mut *mut Sqlite3,
            flags: c_int,
            vfs: *const c_char,
        ) -> c_int;
        pub fn sqlite3_close_v2(db: *mut Sqlite3) -> c_int;
        pub fn sqlite3_errmsg(db: *mut Sqlite3) -> *const c_char;
        pub fn sqlite3_extended_errcode(db: *mut Sqlite3) -> c_int;
        pub fn sqlite3_changes(db: *mut Sqlite3) -> c_int;
        pub fn sqlite3_last_insert_rowid(db: *mut Sqlite3) -> i64;
        pub fn sqlite3_busy_timeout(db: *mut Sqlite3, ms: c_int) -> c_int;
        pub fn sqlite3_libversion() -> *const c_char;

        pub fn sqlite3_prepare_v2(
            db: *mut Sqlite3,
            sql: *const c_char,
            n: c_int,
            stmt: *mut *mut Stmt,
            tail: *mut *const c_char,
        ) -> c_int;
        pub fn sqlite3_step(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_reset(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_finalize(stmt: *mut Stmt) -> c_int;

        pub fn sqlite3_bind_null(stmt: *mut Stmt, i: c_int) -> c_int;
        pub fn sqlite3_bind_int64(stmt: *mut Stmt, i: c_int, v: i64) -> c_int;
        pub fn sqlite3_bind_double(stmt: *mut Stmt, i: c_int, v: f64) -> c_int;
        pub fn sqlite3_bind_text(
            stmt: *mut Stmt,
            i: c_int,
            v: *const c_char,
            n: c_int,
            d: isize,
        ) -> c_int;
        pub fn sqlite3_bind_blob(
            stmt: *mut Stmt,
            i: c_int,
            v: *const c_void,
            n: c_int,
            d: isize,
        ) -> c_int;

        pub fn sqlite3_column_count(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_column_name(stmt: *mut Stmt, i: c_int) -> *const c_char;
        pub fn sqlite3_column_type(stmt: *mut Stmt, i: c_int) -> c_int;
        pub fn sqlite3_column_int64(stmt: *mut Stmt, i: c_int) -> i64;
        pub fn sqlite3_column_double(stmt: *mut Stmt, i: c_int) -> f64;
        pub fn sqlite3_column_text(stmt: *mut Stmt, i: c_int) -> *const u8;
        pub fn sqlite3_column_blob(stmt: *mut Stmt, i: c_int) -> *const c_void;
        pub fn sqlite3_column_bytes(stmt: *mut Stmt, i: c_int) -> c_int;
    }
}

/// A dynamically typed SQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(v) => Some(*v),
            Value::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(v) => Some(v),
            _ => None,
        }
    }
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// An error from the engine, or from converting arguments to pass to it.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for Error {}

impl Error {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Error {
            code,
            message: message.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// One result row, addressable by position or column name.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_named(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == name)
            .and_then(|i| self.values.get(i))
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

/// A connection to a DoltLite database.
pub struct Connection {
    db: *mut ffi::Sqlite3,
}

// The engine is compiled with SQLITE_THREADSAFE=1, which serializes access
// internally, but a Connection still must not be used from two threads at
// once -- hence Send without Sync.
unsafe impl Send for Connection {}

impl Connection {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        let path = path.as_ref().to_string_lossy().into_owned();
        Connection::open_raw(&path)
    }

    pub fn open_in_memory() -> Result<Connection> {
        Connection::open_raw(":memory:")
    }

    fn open_raw(path: &str) -> Result<Connection> {
        let c_path = CString::new(path)
            .map_err(|_| Error::new(0, "database path contains a NUL byte"))?;
        let mut db: *mut ffi::Sqlite3 = ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                c_path.as_ptr(),
                &mut db,
                ffi::OPEN_READWRITE | ffi::OPEN_CREATE,
                ptr::null(),
            )
        };
        if rc != ffi::OK {
            let msg = if db.is_null() {
                format!("unable to open database ({rc})")
            } else {
                unsafe { errmsg(db) }
            };
            if !db.is_null() {
                unsafe { ffi::sqlite3_close_v2(db) };
            }
            return Err(Error::new(rc, msg));
        }
        Ok(Connection { db })
    }

    /// Runs one or more statements, discarding any rows they produce.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let mut rest = sql.to_owned();
        while !rest.trim().is_empty() {
            let c_sql = CString::new(rest.as_str())
                .map_err(|_| Error::new(0, "SQL contains a NUL byte"))?;
            let mut stmt: *mut ffi::Stmt = ptr::null_mut();
            let mut tail: *const c_char = ptr::null();
            let rc = unsafe {
                ffi::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, &mut tail)
            };
            if rc != ffi::OK {
                return Err(self.error(rc));
            }
            let consumed = if tail.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(tail) }
                    .to_string_lossy()
                    .into_owned()
            };
            if !stmt.is_null() {
                let mut step = unsafe { ffi::sqlite3_step(stmt) };
                while step == ffi::ROW {
                    step = unsafe { ffi::sqlite3_step(stmt) };
                }
                unsafe { ffi::sqlite3_finalize(stmt) };
                if step != ffi::DONE {
                    return Err(self.error(step));
                }
            }
            rest = consumed;
        }
        Ok(())
    }

    /// Runs one statement with parameters, returning the number of rows it
    /// changed.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<usize> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        while stmt.step()? {}
        Ok(unsafe { ffi::sqlite3_changes(self.db) } as usize)
    }

    /// Runs one statement and returns its first row, if any.
    pub fn query_row(&self, sql: &str, params: &[Value]) -> Result<Option<Row>> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        if stmt.step()? {
            Ok(Some(stmt.row()))
        } else {
            Ok(None)
        }
    }

    /// Runs one statement and returns every row it produces.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        let mut rows = Vec::new();
        while stmt.step()? {
            rows.push(stmt.row());
        }
        Ok(rows)
    }

    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let c_sql =
            CString::new(sql).map_err(|_| Error::new(0, "SQL contains a NUL byte"))?;
        let mut stmt: *mut ffi::Stmt = ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut())
        };
        if rc != ffi::OK {
            return Err(self.error(rc));
        }
        if stmt.is_null() {
            return Err(Error::new(0, "statement contains no SQL"));
        }
        Ok(Statement { conn: self, stmt })
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    pub fn changes(&self) -> usize {
        unsafe { ffi::sqlite3_changes(self.db) as usize }
    }

    /// Waits up to `ms` for a competing writer instead of failing immediately.
    pub fn busy_timeout(&self, ms: i32) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.db, ms) };
        if rc == ffi::OK {
            Ok(())
        } else {
            Err(self.error(rc))
        }
    }

    fn error(&self, rc: i32) -> Error {
        let code = unsafe { ffi::sqlite3_extended_errcode(self.db) };
        Error::new(
            if code != 0 { code } else { rc },
            unsafe { errmsg(self.db) },
        )
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_close_v2(self.db) };
    }
}

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    stmt: *mut ffi::Stmt,
}

impl<'conn> Statement<'conn> {
    pub fn bind(&mut self, index: usize, value: &Value) -> Result<()> {
        let i = index as c_int;
        let rc = unsafe {
            match value {
                Value::Null => ffi::sqlite3_bind_null(self.stmt, i),
                Value::Integer(v) => ffi::sqlite3_bind_int64(self.stmt, i, *v),
                Value::Real(v) => ffi::sqlite3_bind_double(self.stmt, i, *v),
                Value::Text(v) => ffi::sqlite3_bind_text(
                    self.stmt,
                    i,
                    v.as_ptr() as *const c_char,
                    v.len() as c_int,
                    ffi::TRANSIENT,
                ),
                Value::Blob(v) => ffi::sqlite3_bind_blob(
                    self.stmt,
                    i,
                    v.as_ptr() as *const c_void,
                    v.len() as c_int,
                    ffi::TRANSIENT,
                ),
            }
        };
        if rc == ffi::OK {
            Ok(())
        } else {
            Err(self.conn.error(rc))
        }
    }

    fn bind_all(&mut self, params: &[Value]) -> Result<()> {
        for (i, p) in params.iter().enumerate() {
            self.bind(i + 1, p)?;
        }
        Ok(())
    }

    /// Advances to the next row, returning false once there are none left.
    pub fn step(&mut self) -> Result<bool> {
        let rc = unsafe { ffi::sqlite3_step(self.stmt) };
        match rc {
            ffi::ROW => Ok(true),
            ffi::DONE => Ok(false),
            _ => Err(self.conn.error(rc)),
        }
    }

    pub fn reset(&mut self) -> Result<()> {
        unsafe { ffi::sqlite3_reset(self.stmt) };
        Ok(())
    }

    /// The row the last `step` landed on.
    pub fn row(&self) -> Row {
        let n = unsafe { ffi::sqlite3_column_count(self.stmt) };
        let mut columns = Vec::with_capacity(n as usize);
        let mut values = Vec::with_capacity(n as usize);
        for i in 0..n {
            columns.push(unsafe {
                let p = ffi::sqlite3_column_name(self.stmt, i);
                if p.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(p).to_string_lossy().into_owned()
                }
            });
            values.push(self.value(i));
        }
        Row { columns, values }
    }

    fn value(&self, i: c_int) -> Value {
        unsafe {
            match ffi::sqlite3_column_type(self.stmt, i) {
                ffi::NULL => Value::Null,
                ffi::INTEGER => Value::Integer(ffi::sqlite3_column_int64(self.stmt, i)),
                ffi::FLOAT => Value::Real(ffi::sqlite3_column_double(self.stmt, i)),
                ffi::BLOB => {
                    let n = ffi::sqlite3_column_bytes(self.stmt, i) as usize;
                    let p = ffi::sqlite3_column_blob(self.stmt, i);
                    if p.is_null() || n == 0 {
                        Value::Blob(Vec::new())
                    } else {
                        Value::Blob(
                            std::slice::from_raw_parts(p as *const u8, n).to_vec(),
                        )
                    }
                }
                ffi::TEXT => self.text_value(i),
                // An unrecognized type code still has a text rendering.
                _ => self.text_value(i),
            }
        }
    }

    /// Reads column `i` as bytes rather than as a C string: text values may
    /// contain embedded NULs.
    unsafe fn text_value(&self, i: c_int) -> Value {
        let n = ffi::sqlite3_column_bytes(self.stmt, i) as usize;
        let p = ffi::sqlite3_column_text(self.stmt, i);
        if p.is_null() || n == 0 {
            Value::Text(String::new())
        } else {
            let bytes = std::slice::from_raw_parts(p, n);
            Value::Text(String::from_utf8_lossy(bytes).into_owned())
        }
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_finalize(self.stmt) };
    }
}

/// The bundled engine's SQLite version string.
pub fn version() -> String {
    unsafe {
        CStr::from_ptr(ffi::sqlite3_libversion())
            .to_string_lossy()
            .into_owned()
    }
}

unsafe fn errmsg(db: *mut ffi::Sqlite3) -> String {
    let p = ffi::sqlite3_errmsg(db);
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}
