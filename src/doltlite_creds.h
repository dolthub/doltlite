#ifndef DOLTLITE_CREDS_H
#define DOLTLITE_CREDS_H

#include <stddef.h>

#define DOLTLITE_CREDS_SESSION "doltlite.creds"

#define DOLTLITE_SEED_LEN   32
#define DOLTLITE_PUBKEY_LEN 32
#define DOLTLITE_SIG_LEN    64
#define DOLTLITE_KID_RAW_LEN 28

#if DOLTLITE_ENABLE_REMOTES && \
    (!defined(SQLITE_AMALGAMATION) || defined(DOLTLITE_AMALGAMATION_AUTH))
#define DOLTLITE_HAVE_AUTH 1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteCreds DoltliteCreds;

/* sqlite3_malloc results; sqlite3_free or doltliteCredsFree/FreeList. */

void doltliteSha512_224(const unsigned char *in, size_t inlen,
                        unsigned char out[DOLTLITE_KID_RAW_LEN]);

char *doltliteBase64UrlEncode(const unsigned char *in, size_t inlen);
int doltliteBase64UrlDecode(const char *in, unsigned char **out, size_t *outlen);

int doltliteCredsGenerate(DoltliteCreds **out);
int doltliteCredsFromSeed(const unsigned char seed[DOLTLITE_SEED_LEN],
                          DoltliteCreds **out);
void doltliteCredsFree(DoltliteCreds *cred);

char *doltliteCredsKid(const DoltliteCreds *cred);
char *doltliteCredsPubKeyB32(const DoltliteCreds *cred);

void doltliteCredsSign(const DoltliteCreds *cred, const unsigned char *msg,
                       size_t msglen, unsigned char sig[DOLTLITE_SIG_LEN]);

int doltliteCredsBearerToken(const DoltliteCreds *cred, const char *audience,
                             char **jwtOut);
int doltliteCredsBearerTokenAt(const DoltliteCreds *cred, const char *audience,
                               long iat, char **jwtOut);

char *doltliteCredsToJwk(const DoltliteCreds *cred);
char *doltliteCredsToPublicJwk(const DoltliteCreds *cred);
int doltliteCredsFromJwk(const char *json, DoltliteCreds **out);

char *doltliteCredsDir(void);
int doltliteCredsSave(const DoltliteCreds *cred, const char *dir);
int doltliteCredsSavePublic(const DoltliteCreds *cred, const char *dir);
int doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out);
int doltliteCredsLoadDefault(const char *dir, DoltliteCreds **out);

int doltliteCredsList(const char *dir, char ***out, int *n);
void doltliteCredsFreeList(char **list, int n);

int doltliteCredsExists(const char *dir, const char *kid);
int doltliteCredsRemove(const char *dir, const char *kid);

#define DOLTLITE_CREDS_ERROR     (-1)
#define DOLTLITE_CREDS_NONE      0
#define DOLTLITE_CREDS_SELECTED  1
#define DOLTLITE_CREDS_ANONYMOUS 2

int doltliteCredsConfigSet(const char*, const char*, const char*, int);
char *doltliteCredsRemoteKey(const char*);
int doltliteCredsLoadNamed(const char*, const char*, DoltliteCreds**);
int doltliteCredsImport(const char*, DoltliteCreds**);
int doltliteCredsResolve(const char*, const char*, const char*,
                         DoltliteCreds**, char**, char**);

int doltliteCredsLoadPubKey(const char *dir, const char *kid,
                            unsigned char pub[DOLTLITE_PUBKEY_LEN]);
int doltliteCredsValidateAuthDir(const char *dir);

int doltliteCredsVerifyBearer(const char *authValue, const char *expectedAudience,
                              const char *authKeysDir, long now, char **kidOut);

#ifdef __cplusplus
}
#endif

#endif
