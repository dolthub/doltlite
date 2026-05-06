#include "statement.h"
#include "database.h"
#include <cstring>

Napi::FunctionReference Statement::constructor;

Napi::Function Statement::Init(Napi::Env env) {
  Napi::Function func = DefineClass(env, "Statement", {
    InstanceMethod<&Statement::Run>("run"),
    InstanceMethod<&Statement::Get>("get"),
    InstanceMethod<&Statement::All>("all"),
    InstanceMethod<&Statement::Finalize_>("finalize"),
  });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  return func;
}

Statement::Statement(const Napi::CallbackInfo& info)
  : Napi::ObjectWrap<Statement>(info), stmt_(nullptr), db_(nullptr) {
  Napi::Env env = info.Env();

  // Constructor invoked by Database::Prepare with (databaseObject, sqlString).
  if (info.Length() < 2 || !info[0].IsObject() || !info[1].IsString()) {
    Napi::TypeError::New(env, "Statement is created via Database.prepare(sql)")
        .ThrowAsJavaScriptException();
    return;
  }

  Database* dbWrap = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
  if (!dbWrap || !dbWrap->Handle()) {
    Napi::Error::New(env, "database is closed").ThrowAsJavaScriptException();
    return;
  }
  db_ = dbWrap->Handle();

  std::string sql = info[1].As<Napi::String>().Utf8Value();
  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr);
  if (rc != SQLITE_OK) {
    std::string msg = std::string("prepare failed: ") + sqlite3_errmsg(db_);
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    stmt_ = nullptr;
    return;
  }
}

Statement::~Statement() {
  if (stmt_) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }
}

// Bind args from the JS call into the prepared statement. Each arg
// becomes a positional ?N binding. Arrays/objects pass through as JSON
// strings (caller's responsibility — keep the binding minimal). Returns
// false on a binding error (and throws).
bool Statement::BindArgs(const Napi::CallbackInfo& info, Napi::Env env) {
  if (!stmt_) {
    Napi::Error::New(env, "statement has been finalized").ThrowAsJavaScriptException();
    return false;
  }
  sqlite3_reset(stmt_);
  sqlite3_clear_bindings(stmt_);

  for (size_t i = 0; i < info.Length(); i++) {
    int idx = (int)i + 1;
    Napi::Value v = info[i];
    int rc;
    if (v.IsNull() || v.IsUndefined()) {
      rc = sqlite3_bind_null(stmt_, idx);
    } else if (v.IsBoolean()) {
      rc = sqlite3_bind_int(stmt_, idx, v.As<Napi::Boolean>().Value() ? 1 : 0);
    } else if (v.IsNumber()) {
      double d = v.As<Napi::Number>().DoubleValue();
      // Use INTEGER bind if the JS number is an integer in i64 range.
      if (d == (double)(int64_t)d && d >= (double)INT64_MIN && d <= (double)INT64_MAX) {
        rc = sqlite3_bind_int64(stmt_, idx, (int64_t)d);
      } else {
        rc = sqlite3_bind_double(stmt_, idx, d);
      }
    } else if (v.IsBigInt()) {
      bool lossless = false;
      int64_t big = v.As<Napi::BigInt>().Int64Value(&lossless);
      rc = sqlite3_bind_int64(stmt_, idx, big);
    } else if (v.IsString()) {
      std::string s = v.As<Napi::String>().Utf8Value();
      rc = sqlite3_bind_text(stmt_, idx, s.c_str(), (int)s.size(), SQLITE_TRANSIENT);
    } else if (v.IsBuffer()) {
      Napi::Buffer<uint8_t> buf = v.As<Napi::Buffer<uint8_t>>();
      rc = sqlite3_bind_blob(stmt_, idx, buf.Data(), (int)buf.Length(), SQLITE_TRANSIENT);
    } else {
      Napi::TypeError::New(env, "unsupported bind type at parameter "
                                + std::to_string(idx)).ThrowAsJavaScriptException();
      return false;
    }
    if (rc != SQLITE_OK) {
      std::string msg = std::string("bind failed: ") + sqlite3_errmsg(db_);
      Napi::Error::New(env, msg).ThrowAsJavaScriptException();
      return false;
    }
  }
  return true;
}

// Pull the current row out of stmt_ into a JS object keyed by column name.
Napi::Value Statement::RowToObject(Napi::Env env) {
  Napi::Object row = Napi::Object::New(env);
  int n = sqlite3_column_count(stmt_);
  for (int i = 0; i < n; i++) {
    const char* name = sqlite3_column_name(stmt_, i);
    if (!name) name = "";
    int type = sqlite3_column_type(stmt_, i);
    switch (type) {
      case SQLITE_NULL:
        row.Set(name, env.Null());
        break;
      case SQLITE_INTEGER: {
        sqlite3_int64 v = sqlite3_column_int64(stmt_, i);
        // JS numbers lose precision past 2^53 — return a BigInt above.
        if (v <= 9007199254740992LL && v >= -9007199254740992LL) {
          row.Set(name, Napi::Number::New(env, (double)v));
        } else {
          row.Set(name, Napi::BigInt::New(env, (int64_t)v));
        }
        break;
      }
      case SQLITE_FLOAT:
        row.Set(name, Napi::Number::New(env, sqlite3_column_double(stmt_, i)));
        break;
      case SQLITE_TEXT: {
        const unsigned char* t = sqlite3_column_text(stmt_, i);
        int sz = sqlite3_column_bytes(stmt_, i);
        row.Set(name, Napi::String::New(env, (const char*)t, (size_t)sz));
        break;
      }
      case SQLITE_BLOB: {
        const void* b = sqlite3_column_blob(stmt_, i);
        int sz = sqlite3_column_bytes(stmt_, i);
        row.Set(name, Napi::Buffer<uint8_t>::Copy(env, (const uint8_t*)b, (size_t)sz));
        break;
      }
    }
  }
  return row;
}

Napi::Value Statement::Run(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!BindArgs(info, env)) return env.Undefined();

  int rc = sqlite3_step(stmt_);
  if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
    std::string msg = std::string("run failed: ") + sqlite3_errmsg(db_);
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object result = Napi::Object::New(env);
  result.Set("changes", Napi::Number::New(env, sqlite3_changes(db_)));
  result.Set("lastInsertRowid",
             Napi::Number::New(env, (double)sqlite3_last_insert_rowid(db_)));
  return result;
}

Napi::Value Statement::Get(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!BindArgs(info, env)) return env.Undefined();

  int rc = sqlite3_step(stmt_);
  if (rc == SQLITE_ROW) {
    return RowToObject(env);
  }
  if (rc == SQLITE_DONE) {
    return env.Undefined();
  }
  std::string msg = std::string("get failed: ") + sqlite3_errmsg(db_);
  Napi::Error::New(env, msg).ThrowAsJavaScriptException();
  return env.Undefined();
}

Napi::Value Statement::All(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!BindArgs(info, env)) return env.Undefined();

  Napi::Array out = Napi::Array::New(env);
  uint32_t i = 0;
  for (;;) {
    int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
      out.Set(i++, RowToObject(env));
      continue;
    }
    if (rc == SQLITE_DONE) break;
    std::string msg = std::string("all failed: ") + sqlite3_errmsg(db_);
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return out;
}

Napi::Value Statement::Finalize_(const Napi::CallbackInfo& info) {
  if (stmt_) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }
  return info.This();
}
