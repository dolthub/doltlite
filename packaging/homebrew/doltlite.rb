# Homebrew formula for doltlite. Intended for submission to
# Homebrew/homebrew-core — once accepted, `brew install doltlite` works
# everywhere without a tap. Subsequent version bumps are opened
# automatically by Homebrew's `BrewTestBot` because of the livecheck
# block below (same pattern dolt uses).
class Doltlite < Formula
  desc "SQLite fork with Git-style version control via prolly trees"
  homepage "https://github.com/dolthub/doltlite"
  url "https://github.com/dolthub/doltlite/releases/download/v0.10.4/doltlite-autoconf-0.10.4.tar.gz"
  sha256 "8a3244473e5a84664dda3e45ccd949691dd108a5efe05da0966948ac61fca7d8"
  license "Apache-2.0"
  head "https://github.com/dolthub/doltlite.git", branch: "master"

  livecheck do
    url :stable
    strategy :github_latest
  end

  depends_on "zlib"

  def install
    mkdir "build" do
      system "../configure"
      system "make", "doltlite", "doltlite-remotesrv", "doltlite-lib"
      bin.install "doltlite", "doltlite-remotesrv"
      include.install "sqlite3.h" => "doltlite.h"
      lib.install "libdoltlite.a"
      lib.install OS.mac? ? "libdoltlite.dylib" : "libdoltlite.so"
    end
  end

  test do
    output = shell_output("#{bin}/doltlite :memory: 'SELECT dolt_version();'")
    assert_match(/v?\d+\.\d+\.\d+/, output)
  end
end
