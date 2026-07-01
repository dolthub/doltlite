#ifndef DOLTLITE_TLS_H
#define DOLTLITE_TLS_H

/*
** A minimal blocking connection used by the HTTP remote client: either a plain
** TCP socket or a TLS-over-TCP session (mbedTLS). For TLS, the server
** certificate chain is verified against trusted CA roots and the hostname is
** checked; a failed verification makes doltliteConnOpen return NULL.
**
** Trusted roots are taken from $DOLTLITE_CA_FILE if set, otherwise from the
** first readable well-known system CA bundle.
*/

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteConn DoltliteConn;

/* Open a connection to host:port. useTls != 0 selects TLS. Returns NULL on
** connect failure or (for TLS) certificate/hostname verification failure. */
DoltliteConn *doltliteConnOpen(const char *host, int port, int useTls);

/* Write all nbuf bytes. Returns 0 on success, non-zero on error. */
int doltliteConnWriteAll(DoltliteConn *conn, const void *buf, int nbuf);

/* Read up to nbuf bytes. Returns the count read (>0), 0 at end of stream, or
** a negative value on error. */
int doltliteConnRead(DoltliteConn *conn, void *buf, int nbuf);

void doltliteConnClose(DoltliteConn *conn);

#ifdef __cplusplus
}
#endif

#endif /* DOLTLITE_TLS_H */
