require "doltlite"
require "rbconfig"
require "tmpdir"

MESSAGE = "installed gem commit"
ROWS = [[1, "café".b, nil], [2, "second".b, "\x00\xff".b]]

def check(label, actual, expected)
  raise "#{label}: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected
end

def with_database(path)
  db = Doltlite.open(path)
  yield db
ensure
  db&.close
end

def verify_database(db, version, commit)
  check("engine and version", db.query("SELECT doltlite_engine(), dolt_version()"), [["prolly", "v#{version}"]])
  rows = db.query("SELECT id, value, bytes FROM records ORDER BY id")
  check("rows", rows.map { |id, value, bytes| [id, value.b, bytes] }, ROWS)
  check("commit history", db.query("SELECT commit_hash FROM dolt_log WHERE message = ?", MESSAGE), [[commit]])
end

version, platform, mode, path, commit = ARGV
spec = Gem.loaded_specs.fetch("doltlite")
check("installed gem version", spec.version.to_s, version)
check("Ruby wrapper version", Doltlite::VERSION, version)
check("installed gem platform", spec.platform.to_s, platform)
bundled = Doltlite::Library.bundled_path
unless bundled && File.realpath(bundled).start_with?(File.realpath(spec.full_gem_path) + File::SEPARATOR)
  raise "libdoltlite is missing from the installed gem"
end

if mode == "reopen"
  raise "database is missing: #{path}" unless File.file?(path)
  with_database(path) { |db| verify_database(db, version, commit) }
  puts "Fresh Ruby process reopened the database and verified the commit"
else
  Dir.mktmpdir do |directory|
    database = File.join(directory, "smoke.db")
    hash = with_database(database) do |db|
      db.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT, bytes BLOB)")
      db.execute("INSERT INTO records VALUES (?, ?, NULL)", 1, "café")
      db.execute("INSERT INTO records VALUES (?, ?, x'00ff')", 2, "second")
      db.execute("BEGIN")
      db.execute("UPDATE records SET value = 'rolled back'")
      db.execute("ROLLBACK")
      result = db.dolt_commit(message: MESSAGE)
      raise "dolt_commit returned no hash" unless result.is_a?(String) && !result.empty?
      verify_database(db, version, result)
      result
    end
    system(RbConfig.ruby, __FILE__, version, platform, "reopen", database, hash, exception: true)
  end
end
