import SQLiteESMFactory from "../dist/doltlite.mjs";
import * as SQLite from "wa-sqlite";

const module = await SQLiteESMFactory();
module._sqlite3_initialize();

const sqlite3 = SQLite.Factory(module);
const db = await sqlite3.open_v2("remote-prod-test.doltlite");

await sqlite3.exec(
  db,
  "SELECT dolt_clone('https://dolthub.com/dolthub/remote-prod-test')",
  (row, columns) => console.log(columns, row),
);
await sqlite3.exec(db, "SELECT * FROM dolt_log", (row, columns) =>
  console.log(columns, row),
);

await sqlite3.close(db);
