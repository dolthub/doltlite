#include "database.h"
#include "statement.h"

Napi::FunctionReference Database::constructor;

Napi::Function Database::Init(Napi::Env env) {
  Napi::Function func = DefineClass(env, "Database", {
    InstanceMethod<&Database::Prepare>("prepare"),
    InstanceMethod<&Database::Exec>("exec"),
    InstanceMethod<&Database::Close>("close"),
    InstanceAccessor<&Database::OpenGetter>("open"),
    InstanceAccessor<&Database::MemoryGetter>("memory"),
  });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  return func;
}

Database::Database(const Napi::CallbackInfo& info)
  : Napi::ObjectWrap<Database>(info), db_(nullptr), memory_(false) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Database(path) requires a string path").ThrowAsJavaScriptException();
    return;
  }

  std::string path = info[0].As<Napi::String>().Utf8Value();
  memory_ = (path == ":memory:");

  int rc = sqlite3_open(path.c_str(), &db_);
  if (rc != SQLITE_OK) {
    std::string msg = std::string("failed to open database: ") + sqlite3_errmsg(db_);
    sqlite3_close(db_);
    db_ = nullptr;
    Napi::Error::New(env, msg).ThrowAsJavaScriptException();
    return;
  }
}

Database::~Database() {
  if (db_) {
    sqlite3_close(db_);
    db_ = nullptr;
  }
}

void Database::Throw(const Napi::Env& env, const char* msg) {
  Napi::Error::New(env, msg).ThrowAsJavaScriptException();
}

Napi::Value Database::Prepare(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    Throw(env, "prepare(sql) requires a string");
    return env.Undefined();
  }
  if (!db_) {
    Throw(env, "database is closed");
    return env.Undefined();
  }
  // Hand off to Statement constructor, which prepares the SQL and
  // stashes the sqlite3* + the prepared sqlite3_stmt*.
  Napi::Object stmt = Statement::constructor.New({info.This(), info[0]});
  return stmt;
}

Napi::Value Database::Exec(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    Throw(env, "exec(sql) requires a string");
    return env.Undefined();
  }
  if (!db_) {
    Throw(env, "database is closed");
    return env.Undefined();
  }
  std::string sql = info[0].As<Napi::String>().Utf8Value();
  char* err = nullptr;
  int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err);
  if (rc != SQLITE_OK) {
    std::string msg = err ? err : "exec failed";
    sqlite3_free(err);
    Throw(env, msg.c_str());
    return env.Undefined();
  }
  return info.This();
}

Napi::Value Database::Close(const Napi::CallbackInfo& info) {
  if (db_) {
    sqlite3_close(db_);
    db_ = nullptr;
  }
  return info.This();
}

Napi::Value Database::OpenGetter(const Napi::CallbackInfo& info) {
  return Napi::Boolean::New(info.Env(), db_ != nullptr);
}

Napi::Value Database::MemoryGetter(const Napi::CallbackInfo& info) {
  return Napi::Boolean::New(info.Env(), memory_);
}
