PRAGMA foreign_keys=ON;

CREATE TABLE deep(
  id INTEGER PRIMARY KEY,
  grp TEXT COLLATE NOCASE NOT NULL,
  score REAL NOT NULL,
  payload BLOB NOT NULL,
  label TEXT GENERATED ALWAYS AS (grp || ':' || id) STORED,
  CHECK(score >= 0)
) STRICT;
CREATE INDEX deep_grp_score ON deep(grp COLLATE NOCASE DESC, score, id);
CREATE INDEX deep_score_partial ON deep(score DESC) WHERE id % 2 = 0;
WITH RECURSIVE seq(x) AS (
  VALUES(1)
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < 20000
)
INSERT INTO deep(id, grp, score, payload)
SELECT x, printf('group-%03d', x % 101), x / 10.0,
       CAST(printf('payload-%08d-abcdefghijklmnopqrstuvwxyz', x) AS BLOB)
FROM seq;

CREATE TABLE keyed(
  a TEXT COLLATE NOCASE,
  b BLOB,
  v REAL,
  PRIMARY KEY(a DESC, b)
) WITHOUT ROWID;
INSERT INTO keyed VALUES
  ('Alpha', x'00ff', 1.5),
  ('beta', x'1020', -2.25),
  ('gamma', x'ff00', 3.75);

CREATE TABLE seq(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  note TEXT NOT NULL
);
INSERT INTO seq(note) VALUES('first'), ('second'), ('third');
DELETE FROM seq WHERE id = 2;

CREATE TABLE generated_values(
  id INTEGER PRIMARY KEY,
  base INT NOT NULL,
  stored INT GENERATED ALWAYS AS (base * 2) STORED,
  virtual INT GENERATED ALWAYS AS (base + 1) VIRTUAL
);
INSERT INTO generated_values(id, base) VALUES(1, 5), (2, 9);

CREATE TABLE parent(
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE
);
CREATE TABLE child(
  id INTEGER PRIMARY KEY,
  parent_code TEXT REFERENCES parent(code),
  note TEXT DEFAULT 'child'
);
INSERT INTO parent VALUES(1, 'p-one'), (2, 'p-two');
INSERT INTO child VALUES(1, 'p-one', 'valid');
CREATE TABLE branch_data(id INTEGER PRIMARY KEY, note TEXT);
INSERT INTO branch_data VALUES(1, 'base');

CREATE TABLE audit(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  deep_id INTEGER NOT NULL,
  old_payload BLOB NOT NULL,
  new_payload BLOB NOT NULL
);
CREATE TRIGGER deep_audit AFTER UPDATE OF payload ON deep
BEGIN
  INSERT INTO audit(deep_id, old_payload, new_payload)
  VALUES(old.id, old.payload, new.payload);
END;
CREATE VIEW deep_even AS
SELECT id, label, score FROM deep WHERE id % 2 = 0;

CREATE VIRTUAL TABLE docs USING fts5(title, body);
INSERT INTO docs VALUES
  ('format', 'content addressed prolly tree storage'),
  ('history', 'branch commit merge diff rebase');

SELECT dolt_commit('-A', '-m', 'v12 comprehensive base',
                   '--author', 'Format Writer <format@example.com>');
SELECT dolt_tag('v12-base', '-m', 'annotated format baseline',
                '--author', 'Format Tagger <tagger@example.com>');
SELECT dolt_remote('add', 'origin',
                   'file:///tmp/doltlite-format-corpus-v12-origin.db');
SELECT dolt_push('origin', 'main');
SELECT dolt_fetch('origin', 'main');

SELECT dolt_checkout('-b', 'feature');
INSERT INTO branch_data VALUES(2, 'feature');
CREATE TABLE feature_only(id TEXT PRIMARY KEY, note TEXT) WITHOUT ROWID;
INSERT INTO feature_only VALUES('feature', 'branch catalog');
SELECT dolt_commit('-A', '-m', 'v12 feature',
                   '--author', 'Feature Writer <feature@example.com>');

SELECT dolt_checkout('main');
INSERT INTO branch_data VALUES(3, 'main');
UPDATE deep SET payload = x'deadbeef' WHERE id IN (30, 40);
CREATE TABLE main_only(id INTEGER PRIMARY KEY, note TEXT);
INSERT INTO main_only VALUES(1, 'main catalog');
SELECT dolt_commit('-A', '-m', 'v12 main',
                   '--author', 'Main Writer <main@example.com>');
SELECT dolt_merge('feature');
SELECT dolt_tag('v12-merge', '-m', 'annotated merge result',
                '--author', 'Merge Tagger <merge@example.com>');

SELECT dolt_checkout('-b', 'workspace');
INSERT INTO deep(id, grp, score, payload)
VALUES(20001, 'staged', 2000.1, x'01020304');
SELECT dolt_add('deep');
UPDATE deep SET grp = 'working' WHERE id = 20001;
INSERT INTO deep(id, grp, score, payload)
VALUES(20002, 'unstaged', 2000.2, x'05060708');
SELECT dolt_checkout('main');

SELECT dolt_checkout('-b', 'violations');
PRAGMA foreign_keys=OFF;
INSERT INTO child VALUES(99, 'missing-parent', 'persisted violation');
PRAGMA foreign_keys=ON;
SELECT dolt_commit('-A', '-m', 'v12 violating head',
                   '--author', 'Violation Writer <violation@example.com>');
SELECT dolt_verify_constraints('--all');
SELECT dolt_checkout('main');

SELECT dolt_checkout('-b', 'rebase_source');
INSERT INTO deep(id, grp, score, payload)
VALUES(20003, 'rebase-source', 2000.3, x'090a0b0c');
SELECT dolt_commit('-A', '-m', 'v12 rebase source',
                   '--author', 'Rebase Writer <rebase@example.com>');
SELECT dolt_checkout('main');
INSERT INTO deep(id, grp, score, payload)
VALUES(20004, 'rebase-onto', 2000.4, x'0d0e0f10');
SELECT dolt_commit('-A', '-m', 'v12 rebase onto',
                   '--author', 'Main Writer <main@example.com>');
SELECT dolt_checkout('rebase_source');
SELECT dolt_rebase('-i', 'main');
