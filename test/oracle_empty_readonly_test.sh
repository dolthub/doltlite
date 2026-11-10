#!/usr/bin/env bash
set -euo pipefail
DOLTLITE="${1:-build/doltlite}"
SQLITE3="${2:-build/sqlite3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$SCRIPT_DIR/assert_stock_reference.sh" "$SQLITE3" "$DOLTLITE"
python3 - "$DOLTLITE" "$SQLITE3" <<'PY'
import pathlib
import queue
import subprocess
import sys
import tempfile
import threading

candidate, stock = sys.argv[1:]
passed = failed = 0
for reader_name, executable in (("stock", stock), ("doltlite", candidate)):
    for mode in ("readonly", "uri", "shared"):
        for journal in ("delete", "wal"):
            name = f"{reader_name}/{mode}/{journal}"
            with tempfile.TemporaryDirectory() as tmp:
                path = pathlib.Path(tmp) / "empty.db"
                path.touch()
                filename = str(path)
                if mode != "readonly":
                    filename = f"file:{path.as_posix()}?mode=ro"
                    if mode == "shared":
                        filename += "&cache=shared"
                reader = subprocess.Popen(
                    [executable, "-batch", "-readonly", filename],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True,
                )
                lines = queue.Queue()

                def collect():
                    for line in reader.stdout:
                        lines.put(line.rstrip("\r\n"))
                    lines.put(None)

                thread = threading.Thread(target=collect, daemon=True)
                thread.start()
                try:
                    reader.stdin.write("SELECT count(*) FROM sqlite_schema;\n")
                    reader.stdin.flush()
                    initial = lines.get(timeout=20)
                    if initial != "0":
                        raise AssertionError(f"empty schema: {initial!r}")
                    subprocess.run(
                        [stock, str(path), f"PRAGMA journal_mode={journal};"
                         "CREATE TABLE s(x); INSERT INTO s VALUES(42);"],
                        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True, timeout=20,
                    )
                    reader.stdin.write("SELECT x FROM s;\n")
                    reader.stdin.flush()
                    observed = lines.get(timeout=20)
                    if observed != "42":
                        raise AssertionError(f"committed row: {observed!r}")
                    reader.stdin.close()
                    if reader.wait(timeout=20) != 0:
                        raise AssertionError("reader exited unsuccessfully")
                    thread.join(timeout=20)
                    passed += 1
                except Exception as error:
                    print(f"FAIL: {name}: {error}")
                    failed += 1
                finally:
                    if reader.poll() is None:
                        reader.kill()
                        reader.wait()
                    thread.join(timeout=20)
                    if not reader.stdin.closed:
                        reader.stdin.close()
                    reader.stdout.close()
print(f"Results: {passed} passed, {failed} failed")
print("__SUITE_COMPLETE__")
sys.exit(bool(failed))
PY
