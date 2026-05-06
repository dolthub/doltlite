declare class Statement {
  run(...params: any[]): { changes: number; lastInsertRowid: number };
  get(...params: any[]): Record<string, any> | undefined;
  all(...params: any[]): Array<Record<string, any>>;
  finalize(): this;
}

declare class Database {
  constructor(path: string);
  readonly open: boolean;
  readonly memory: boolean;
  prepare(sql: string): Statement;
  exec(sql: string): this;
  close(): this;
}

declare const version: () => string;

export = Database;
export { Database, Statement, version };
