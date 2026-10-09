import http.client
import http.server
import os
import subprocess
import sys
import tempfile
import threading
import urllib.parse


def main():
    engine, url, server_db, scratch = sys.argv[1:]
    engine = os.path.abspath(os.environ.get("DOLTLITE_SYSTEM", engine))
    upstream = urllib.parse.urlsplit(url)
    state = {"sql": None, "reads": 0, "errors": [], "once": False}
    failures = []

    def sql(db, statement, allow_error=False):
        result = subprocess.run(
            [engine, "-bail", db, statement], text=True,
            capture_output=True, timeout=60,
        )
        if result.returncode and not allow_error:
            raise RuntimeError(result.stdout + result.stderr)
        return result.returncode, (result.stdout + result.stderr).strip()

    def check(name, condition):
        print("  %s: %s" % ("PASS" if condition else "FAIL", name), flush=True)
        if not condition:
            failures.append(name)

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def forward(self):
            conn = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=60)
            try:
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                conn.request(self.command, self.path, body)
                response = conn.getresponse()
                data = response.read()
                if self.command == "GET" and self.path.endswith("/refs") and state["sql"]:
                    state["reads"] += 1
                    peer_sql = state["sql"].replace("@READ@", str(state["reads"]))
                    sql(server_db, "PRAGMA busy_timeout=5000; " + peer_sql)
                    if state["once"]:
                        state["sql"] = None
                self.send_response(response.status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except Exception as error:
                state["errors"].append(str(error))
                self.send_error(500)
            finally:
                conn.close()

        do_GET = forward
        do_PUT = forward
        do_POST = forward

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    proxy_url = "http://127.0.0.1:%d%s" % (proxy.server_port, upstream.path)

    def push(db, statement, peer_sql, once=False):
        state.update(sql=peer_sql, reads=0, once=once)
        try:
            result = sql(db, statement, allow_error=True)
            reads = state["reads"]
            check("peer writes complete before ref install", not state["errors"] and reads > 0)
            return result, reads
        finally:
            state["sql"] = None

    try:
        with tempfile.TemporaryDirectory(dir=scratch) as tmp:
            client = os.path.join(tmp, "client.db")
            sql(server_db, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT); "
                "INSERT INTO t(v) VALUES('base'); SELECT dolt_commit('-Am','base');")
            sql(client, "SELECT dolt_clone('%s'); SELECT dolt_checkout('-b','feature'); "
                "INSERT INTO t(v) VALUES('feature'); SELECT dolt_commit('-am','feature');" % proxy_url)
            for step in range(3):
                if step:
                    sql(client, "INSERT INTO t(v) VALUES('client'); SELECT dolt_commit('-am','next');")
                peer = "UPDATE t SET v='peer' WHERE id=1; "
                peer += "INSERT INTO t(v) VALUES('writer'),('writer');"
                result, reads = push(client, "SELECT dolt_push('origin','feature');", peer)
                check("busy-main feature push succeeds on first snapshot", result == (0, "0") and reads == 1)
                if result != (0, "0"):
                    print(result, flush=True)
                    return 1
                check("peer main rows survive feature push", sql(server_db,
                    "SELECT count(*) FROM t WHERE v='writer';")[1] == str(2 * (step + 1)))
                check("peer sequence survives feature push", sql(server_db,
                    "SELECT seq FROM sqlite_sequence WHERE name='t';")[1] == sql(server_db,
                    "SELECT max(id) FROM t;")[1])
                local_tip = sql(client, "SELECT dolt_hashof('feature');")[1]
                check("feature tip is installed", sql(server_db,
                    "SELECT dolt_hashof('feature');")[1] == local_tip)

            result, reads = push(client, "SELECT dolt_push('origin','feature');",
                                 "UPDATE t SET v='noop peer' WHERE id=1;")
            check("busy-main no-op push succeeds on first snapshot", result == (0, "0") and reads == 1)
            check("no-op push preserves peer working rows", sql(server_db,
                "SELECT v FROM t WHERE id=1;")[1] == "noop peer")

            sql(server_db, "SELECT dolt_commit('-am','writer committed');")
            result, reads = push(client, "SELECT dolt_push('origin','feature');",
                "UPDATE t SET v='new main' WHERE id=1; SELECT dolt_commit('-am','new main'); "
                "SELECT dolt_branch('peer_branch'); SELECT dolt_tag('peer_tag'); "
                "SELECT dolt_remote('add','peer_remote','file:///peer.db'); "
                "SELECT dolt_default_branch('peer_branch');")
            check("peer commit does not invalidate another branch push", result == (0, "0") and reads == 1)
            check("peer branch survives", sql(server_db,
                "SELECT count(*) FROM dolt_branches WHERE name='peer_branch';")[1] == "1")
            check("peer tag survives", sql(server_db,
                "SELECT count(*) FROM dolt_tags WHERE tag_name='peer_tag';")[1] == "1")
            check("peer remote survives", sql(server_db,
                "SELECT count(*) FROM dolt_remotes WHERE name='peer_remote';")[1] == "1")
            check("peer commit survives", sql(server_db,
                "SELECT message FROM dolt_log LIMIT 1;")[1] == "new main")
            check("peer default-branch choice survives", sql(server_db,
                "SELECT dolt_default_branch();")[1] == "peer_branch")

            sql(client, "SELECT dolt_tag('client_tag');")
            result, reads = push(client, "SELECT dolt_push('origin','client_tag');",
                                 "UPDATE t SET v='tag peer @READ@' WHERE id=1;")
            check("tag push preserves unrelated peer write", result == (0, "0") and reads == 2)
            if result != (0, "0"):
                print(result, flush=True)
            check("pushed tag is present", sql(server_db,
                "SELECT count(*) FROM dolt_tags WHERE tag_name='client_tag';")[1] == "1")
            check("tag push preserves the latest peer write", sql(server_db,
                "SELECT v FROM t WHERE id=1;")[1] == "tag peer 2")

            result, reads = push(client, "SELECT dolt_push('origin',':feature');",
                                 "UPDATE t SET v='delete peer' WHERE id=1;")
            check("delete push succeeds despite unrelated peer write", result == (0, "0") and reads == 1)
            check("delete push preserves peer write", sql(server_db,
                "SELECT v FROM t WHERE id=1;")[1] == "delete peer")
            check("only the target branch is deleted", sql(server_db,
                "SELECT count(*) FROM dolt_branches WHERE name='feature';")[1] == "0")

            for force in (False, True):
                branch = "race_force" if force else "race"
                sql(client, "SELECT dolt_checkout('-b','%s'); SELECT dolt_push('origin','%s'); "
                    "INSERT INTO t(v) VALUES('local race'); SELECT dolt_commit('-am','local race');" % (branch, branch))
                peer = "SELECT dolt_checkout('%s'); INSERT INTO t(v) VALUES('peer race'); "
                peer = peer % branch + "SELECT dolt_commit('-am','peer race');"
                command = "SELECT dolt_push('origin','%s'%s);" % (branch, ",'--force'" if force else "")
                result, reads = push(client, command, peer, once=True)
                check("same-branch advance refuses push even with force", result[0] != 0)
                check("same-branch peer commit survives", sql(server_db + "/" + branch,
                    "SELECT message FROM dolt_log LIMIT 1;")[1] == "peer race")

            sql(client, "SELECT dolt_checkout('-b','dirty'); SELECT dolt_push('origin','dirty'); "
                "INSERT INTO t(v) VALUES('local dirty'); SELECT dolt_commit('-am','local dirty');")
            for force in (False, True):
                command = "SELECT dolt_push('origin','dirty'%s);" % (",'--force'" if force else "")
                result, reads = push(client, command,
                    "SELECT dolt_checkout('dirty'); UPDATE t SET v='dirty peer' WHERE id=1;", once=True)
                check("dirty target refuses push even with force", result[0] != 0)
                check("dirty target peer rows survive", sql(server_db + "/dirty",
                    "SELECT v FROM t WHERE id=1;")[1] == "dirty peer")

            check("server integrity after conditional installs", sql(server_db,
                "PRAGMA integrity_check;")[1] == "ok")
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join()
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
