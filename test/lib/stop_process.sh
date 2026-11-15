# stop_process PID [SIGNAL] ends a background process a test started.
#
# Under MSYS2 a signal sent to a native Windows program is carried out by the
# runtime walking the Windows process table and signalling every process
# whose recorded parent PID is the target's. Windows reuses PIDs, so that
# can be the test's own shell, which then dies mid-run. SIGTERM is also
# delivered by injecting ExitProcess into the target, which can leave a
# threaded server hung and the caller's wait blocked. taskkill ends exactly
# the one process.
stop_process() {
  local pid="$1" sig="${2:-TERM}" winpid
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      winpid="$(cat "/proc/$pid/winpid" 2>/dev/null)" || return 1
      [ -n "$winpid" ] || return 1
      taskkill //F //PID "$winpid" >/dev/null 2>&1
      ;;
    *)
      kill "-$sig" "$pid"
      ;;
  esac
}
