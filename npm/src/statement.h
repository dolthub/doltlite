#ifndef DOLTLITE_NPM_STATEMENT_H
#define DOLTLITE_NPM_STATEMENT_H

#include <napi.h>
#include "sqlite3.h"

class Statement : public Napi::ObjectWrap<Statement> {
 public:
  static Napi::Function Init(Napi::Env env);
  static Napi::FunctionReference constructor;

  Statement(const Napi::CallbackInfo& info);
  ~Statement();

 private:
  Napi::Value Run(const Napi::CallbackInfo& info);
  Napi::Value Get(const Napi::CallbackInfo& info);
  Napi::Value All(const Napi::CallbackInfo& info);
  Napi::Value Finalize_(const Napi::CallbackInfo& info);

  bool BindArgs(const Napi::CallbackInfo& info, Napi::Env env);
  Napi::Value RowToObject(Napi::Env env);

  sqlite3_stmt* stmt_;
  sqlite3* db_;  // weak reference, owned by Database
};

#endif
