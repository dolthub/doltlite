# openssl_test_cert <key-out> <cert-out> <subject> [openssl req options...]
#
# Writes a one-day self-signed certificate. On MSYS the subject must not be
# path-converted ("/CN=x" would become "C:/msys64/CN=x" and openssl fails), so
# conversion is switched off and the file paths are converted by hand.
openssl_test_cert() {
  local key="$1" cert="$2" subj="$3"
  shift 3
  [ "$key" = /dev/null ] && key="$cert.key"
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      MSYS2_ARG_CONV_EXCL='*' openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "$(cygpath -w "$key")" -out "$(cygpath -w "$cert")" \
        -subj "$subj" "$@"
      ;;
    *)
      openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "$key" -out "$cert" -subj "$subj" "$@"
      ;;
  esac
}

# tls_unavailable <reason>: CI sets DOLTLITE_REQUIRE_TLS=1 so a runner that
# cannot build a certificate fails instead of skipping its TLS coverage.
tls_unavailable() {
  if [ "${DOLTLITE_REQUIRE_TLS:-0}" = 1 ]; then
    echo "FAIL: $1 (DOLTLITE_REQUIRE_TLS=1)"
    exit 1
  fi
  echo "SKIP: $1"
  exit 0
}
