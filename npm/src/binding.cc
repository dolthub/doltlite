#include <napi.h>
#include "database.h"
#include "statement.h"
#include "sqlite3.h"

static Napi::Value Version(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), sqlite3_libversion());
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  exports.Set("Database", Database::Init(env));
  exports.Set("Statement", Statement::Init(env));
  exports.Set("version", Napi::Function::New(env, Version));
  return exports;
}

NODE_API_MODULE(doltlite, Init)
