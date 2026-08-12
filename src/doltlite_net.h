#ifndef DOLTLITE_NET_H
#define DOLTLITE_NET_H

#include <errno.h> /* amalgamator: keep */
#include <string.h> /* amalgamator: keep */

/* Thin cross-platform socket shim shared by doltlite_tls.c and
 * doltlite_remotesrv.c. Socket handles are kept as int to match
 * mbedtls_net_context, which stores its fd as int on Windows too. */

#ifdef _WIN32

#include <winsock2.h> /* amalgamator: keep */
#include <ws2tcpip.h> /* amalgamator: keep */
#include <windows.h> /* amalgamator: keep */

typedef WSAPOLLFD doltlite_pollfd;
#define doltliteCloseSocket closesocket
#define doltlitePoll(fds, n, ms) WSAPoll((fds), (n), (ms))
#define doltliteShutdownSocket(fd) shutdown((fd), SD_BOTH)

static inline long long doltliteMonotonicMs(void) {
  return (long long)GetTickCount64();
}

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

#include <sys/socket.h> /* amalgamator: keep */
#include <netinet/in.h> /* amalgamator: keep */
#include <arpa/inet.h> /* amalgamator: keep */
#include <netdb.h> /* amalgamator: keep */
#include <fcntl.h> /* amalgamator: keep */
#include <poll.h> /* amalgamator: keep */
#include <sys/time.h> /* amalgamator: keep */
#include <time.h> /* amalgamator: keep */
#include <unistd.h> /* amalgamator: keep */

typedef struct pollfd doltlite_pollfd;
#define doltliteCloseSocket close
#define doltlitePoll(fds, n, ms) poll((fds), (n), (ms))
#define doltliteShutdownSocket(fd) shutdown((fd), SHUT_RDWR)

static inline long long doltliteMonotonicMs(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

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

static inline int doltliteSocketSetBlocking(int fd, int blocking) {
#ifdef _WIN32
  u_long mode = blocking ? 0 : 1;
  return ioctlsocket(fd, FIONBIO, &mode) == 0 ? 0 : 1;
#else
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return 1;
  if (blocking) {
    flags &= ~O_NONBLOCK;
  } else {
    flags |= O_NONBLOCK;
  }
  return fcntl(fd, F_SETFL, flags) == 0 ? 0 : 1;
#endif
}

static inline int doltliteSocketConnectPending(void) {
#ifdef _WIN32
  int err = WSAGetLastError();
  return err == WSAEWOULDBLOCK || err == WSAEINPROGRESS
      || err == WSAEALREADY;
#else
  return errno == EINPROGRESS || errno == EWOULDBLOCK
      || errno == EALREADY || errno == EINTR;
#endif
}

static inline int doltliteTcpConnect(
  const char *host,
  const char *port,
  int timeoutMs
) {
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  struct addrinfo *ai;
  long long deadline = timeoutMs > 0
                     ? doltliteMonotonicMs() + timeoutMs : 0;
  int fd = -1;

  if (doltliteNetInit() != 0) return -1;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(host, port, &hints, &res) != 0) return -1;

  for (ai = res; ai != NULL; ai = ai->ai_next) {
    int connected = 0;
    int rc;
    fd = (int)socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;

    if (timeoutMs <= 0) {
      connected = connect(fd, ai->ai_addr, (int)ai->ai_addrlen) == 0;
    } else if (doltliteSocketSetBlocking(fd, 0) == 0) {
      rc = connect(fd, ai->ai_addr, (int)ai->ai_addrlen);
      if (rc == 0) {
        connected = 1;
      } else if (doltliteSocketConnectPending()) {
        doltlite_pollfd pfd;
        long long remaining = deadline - doltliteMonotonicMs();
        int waitMs = remaining > 0x7fffffff
                   ? 0x7fffffff : (int)remaining;
        int soError = 0;
#ifdef _WIN32
        int nSoError = (int)sizeof(soError);
#else
        socklen_t nSoError = (socklen_t)sizeof(soError);
#endif
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;
        if (remaining > 0 && doltlitePoll(&pfd, 1, waitMs) > 0
         && getsockopt(fd, SOL_SOCKET, SO_ERROR,
#ifdef _WIN32
                       (char *)&soError,
#else
                       &soError,
#endif
                       &nSoError) == 0
         && soError == 0) {
          connected = 1;
        }
      }
      if (doltliteSocketSetBlocking(fd, 1) != 0) connected = 0;
    }

    if (connected) break;
    doltliteCloseSocket(fd);
    fd = -1;
    if (deadline && doltliteMonotonicMs() >= deadline) break;
  }

  freeaddrinfo(res);
  return fd;
}

#endif
