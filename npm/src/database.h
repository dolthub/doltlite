#ifndef DOLTLITE_NPM_DATABASE_H
#define DOLTLITE_NPM_DATABASE_H

#include <napi.h>
#include "sqlite3.h"

class Database : public Napi::ObjectWrap<Database> {
 public:
  static Napi::Function Init(Napi::Env env);
  static Napi::FunctionReference constructor;

  Database(const Napi::CallbackInfo& info);
  ~Database();

  sqlite3* Handle() const { return db_; }

 private:
  Napi::Value Prepare(const Napi::CallbackInfo& info);
  Napi::Value Exec(const Napi::CallbackInfo& info);
  Napi::Value Close(const Napi::CallbackInfo& info);
  Napi::Value OpenGetter(const Napi::CallbackInfo& info);
  Napi::Value MemoryGetter(const Napi::CallbackInfo& info);

  void Throw(const Napi::Env& env, const char* msg);

  sqlite3* db_;
  bool memory_;
};

#endif
