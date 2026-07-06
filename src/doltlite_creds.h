#ifndef DOLTLITE_CREDS_H
#define DOLTLITE_CREDS_H

#include <stddef.h>

#define DOLTLITE_SEED_LEN   32
#define DOLTLITE_PUBKEY_LEN 32
#define DOLTLITE_SIG_LEN    64
#define DOLTLITE_KID_RAW_LEN 28

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteCreds DoltliteCreds;

void doltliteSha512_224(const unsigned char *in, size_t inlen,
                        unsigned char out[DOLTLITE_KID_RAW_LEN]);

char *doltliteBase32Encode(const unsigned char *in, size_t inlen);

char *doltliteBase64UrlEncode(const unsigned char *in, size_t inlen);
int doltliteBase64UrlDecode(const char *in, unsigned char **out, size_t *outlen);

int doltliteCredsGenerate(DoltliteCreds **out);
int doltliteCredsFromSeed(const unsigned char seed[DOLTLITE_SEED_LEN],
                          DoltliteCreds **out);
void doltliteCredsFree(DoltliteCreds *cred);

const unsigned char *doltliteCredsPubKey(const DoltliteCreds *cred);
const unsigned char *doltliteCredsSeed(const DoltliteCreds *cred);

char *doltliteCredsKid(const DoltliteCreds *cred);
char *doltliteCredsPubKeyB32(const DoltliteCreds *cred);

void doltliteCredsSign(const DoltliteCreds *cred, const unsigned char *msg,
                       size_t msglen, unsigned char sig[DOLTLITE_SIG_LEN]);

int doltliteCredsBearerToken(const DoltliteCreds *cred, const char *audience,
                             char **jwtOut);
int doltliteCredsBearerTokenAt(const DoltliteCreds *cred, const char *audience,
                               long iat, char **jwtOut);

char *doltliteCredsToJwk(const DoltliteCreds *cred);
int doltliteCredsFromJwk(const char *json, DoltliteCreds **out);

char *doltliteCredsDir(void);
int doltliteCredsSave(const DoltliteCreds *cred, const char *dir);
int doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out);
int doltliteCredsLoadDefault(const char *dir, DoltliteCreds **out);

int doltliteCredsList(const char *dir, char ***out, int *n);
void doltliteCredsFreeList(char **list, int n);

int doltliteCredsRemove(const char *dir, const char *kid);

int doltliteCredsLoadPubKey(const char *dir, const char *kid,
                            unsigned char pub[DOLTLITE_PUBKEY_LEN]);

int doltliteCredsVerifyBearer(const char *authValue, const char *expectedAudience,
                              const char *authKeysDir, long now, char **kidOut);

#ifdef __cplusplus
}
#endif

#endif
