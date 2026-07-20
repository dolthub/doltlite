#ifndef DOLTLITE_NET_H
#define DOLTLITE_NET_H

/* Thin cross-platform socket shim shared by doltlite_tls.c and
 * doltlite_remotesrv.c. Socket handles are kept as int to match
 * mbedtls_net_context, which stores its fd as int on Windows too. */

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

typedef WSAPOLLFD doltlite_pollfd;
#define doltliteCloseSocket closesocket
#define doltlitePoll(fds, n, ms) WSAPoll((fds), (n), (ms))
#define doltliteShutdownSocket(fd) shutdown((fd), SD_BOTH)

static inline int doltliteSocketSetTimeout(int fd, int timeoutMs) {
  DWORD timeout = (DWORD)timeoutMs;
  int rc1 = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                       (const char *)&timeout, sizeof(timeout));
  int rc2 = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                       (const char *)&timeout, sizeof(timeout));
  return rc1 == 0 && rc2 == 0 ? 0 : 1;
}

/* WSAStartup is refcounted and never torn down here; process exit reclaims
 * it. Idempotent enough for a CLI and the test server. */
static inline int doltliteNetInit(void) {
  static int done = 0;
  WSADATA wsa;
  if (done) return 0;
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return 1;
  done = 1;
  return 0;
}

#else

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/time.h>
#include <unistd.h>

typedef struct pollfd doltlite_pollfd;
#define doltliteCloseSocket close
#define doltlitePoll(fds, n, ms) poll((fds), (n), (ms))
#define doltliteShutdownSocket(fd) shutdown((fd), SHUT_RDWR)

static inline int doltliteSocketSetTimeout(int fd, int timeoutMs) {
  struct timeval timeout;
  int rc1, rc2;
  timeout.tv_sec = timeoutMs / 1000;
  timeout.tv_usec = (timeoutMs % 1000) * 1000;
  rc1 = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  rc2 = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  return rc1 == 0 && rc2 == 0 ? 0 : 1;
}

static inline int doltliteNetInit(void) { return 0; }

#endif

#endif
