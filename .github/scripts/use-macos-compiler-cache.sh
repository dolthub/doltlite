export PATH="$(brew --prefix ccache)/libexec:$PATH"
export CC="$(command -v cc)"
export CXX="$(command -v c++)"
[[ "$CC" == */ccache/libexec/cc ]]
