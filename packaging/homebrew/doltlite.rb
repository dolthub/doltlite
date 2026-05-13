# Homebrew formula for doltlite. Held in this repo as the source-of-truth
# copy until the project clears homebrew-core's notability threshold for
# new third-party submissions (≥30 forks, ≥30 watchers, ≥75 stars).
class Doltlite < Formula
  desc "SQLite fork with Git-style version control via prolly trees"
  homepage "https://github.com/dolthub/doltlite"
  url "https://github.com/dolthub/doltlite/releases/download/v0.10.6/doltlite-autoconf-0.10.6.tar.gz"
  sha256 "816ecedc369dd61fd06a0759985d5fbfbdf9fadcd1096cc1c35cc7f09447e7c3"
  license "Apache-2.0"
  head "https://github.com/dolthub/doltlite.git", branch: "master"

  uses_from_macos "zlib"

  def install
    mkdir "build" do
      system "../configure"
      system "make", "doltlite", "doltlite-remotesrv", "doltlite-lib"
      bin.install "doltlite", "doltlite-remotesrv"
      # Ship sqlite3.h verbatim so existing C programs that
      # `#include "sqlite3.h"` (including examples/quickstart.c) just
      # work after install. doltlite.h is the same content under the
      # project's preferred name. doltlite_remotesrv.h declares the
      # in-process remote-server API (doltliteServeAsync et al.).
      include.install "sqlite3.h"
      include.install "sqlite3.h" => "doltlite.h"
      include.install "#{buildpath}/src/doltlite_remotesrv.h"
      lib.install "libdoltlite.a"
      lib.install OS.mac? ? "libdoltlite.dylib" : "libdoltlite.so"
    end
  end

  test do
    assert_match version.to_s,
                 shell_output("#{bin}/doltlite :memory: 'SELECT dolt_version();'")
    (testpath/"hello.c").write <<~EOS
      #include <stdio.h>
      #include "sqlite3.h"
      int main(void) {
        sqlite3 *db;
        if (sqlite3_open(":memory:", &db) != SQLITE_OK) return 1;
        sqlite3_close(db);
        printf("ok\\n");
        return 0;
      }
    EOS
    system ENV.cc, "hello.c", "-I#{include}", "-L#{lib}", "-ldoltlite",
                   "-o", "hello"
    assert_equal "ok", shell_output("./hello").chomp
  end
end
