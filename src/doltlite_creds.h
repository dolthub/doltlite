#ifndef DOLTLITE_CREDS_H
#define DOLTLITE_CREDS_H

/*
** doltlite remote credentials.
**
** A doltlite credential is an ed25519 keypair, wire-compatible with `dolt
** login`. The public key is registered with a DoltHub account; the private key
** signs a short-lived bearer JWT on each remote request (JWT assembly lives in
** a later step; this file provides the crypto/keying/storage foundation).
**
** The credential id ("kid") is base32(sha512_224(pubkey)) using dolt's alphabet.
** SHA-512/224 is a distinct FIPS 180-4 variant (its own IVs), matched here so
** that the id doltlite computes equals the one DoltHub derives from the
** approved public key.
**
** Credentials are stored one-per-file as OKP/Ed25519 JWKs under a creds
** directory (default ~/.doltlite/creds, override with $DOLTLITE_CREDS_DIR).
*/

#include <stddef.h>

#define DOLTLITE_SEED_LEN   32  /* ed25519 seed (the RFC 8037 private key "d") */
#define DOLTLITE_PUBKEY_LEN 32
#define DOLTLITE_SIG_LEN    64
#define DOLTLITE_KID_RAW_LEN 28 /* sha512/224 digest length */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteCreds DoltliteCreds;

/* --- Low-level primitives (exposed for reuse and known-answer tests) --- */

/* SHA-512/224 of in[0..inlen). Writes 28 bytes to out. */
void doltliteSha512_224(const unsigned char *in, size_t inlen,
                        unsigned char out[DOLTLITE_KID_RAW_LEN]);

/* base32 with dolt's alphabet "0123456789abcdefghijklmnopqrstuv", no padding.
** Returns a malloc'd NUL-terminated string the caller frees, or NULL on OOM. */
char *doltliteBase32Encode(const unsigned char *in, size_t inlen);

/* base64url matching Go's base64.URLEncoding (alphabet A-Za-z0-9-_, '=' pad). */
char *doltliteBase64UrlEncode(const unsigned char *in, size_t inlen);
/* Decodes base64url. On success sets *out (malloc'd) and *outlen, returns 0.
** Returns non-zero on malformed input. */
int doltliteBase64UrlDecode(const char *in, unsigned char **out, size_t *outlen);

/* --- Credential lifecycle --- */

/* Generate a credential from a fresh random seed. Returns 0 on success. */
int doltliteCredsGenerate(DoltliteCreds **out);
/* Build a credential from a caller-supplied 32-byte seed (deterministic). */
int doltliteCredsFromSeed(const unsigned char seed[DOLTLITE_SEED_LEN],
                          DoltliteCreds **out);
void doltliteCredsFree(DoltliteCreds *cred);

/* Borrowed pointers into cred; valid until doltliteCredsFree. */
const unsigned char *doltliteCredsPubKey(const DoltliteCreds *cred); /* 32 bytes */
const unsigned char *doltliteCredsSeed(const DoltliteCreds *cred);   /* 32 bytes */

/* kid = base32(sha512_224(pubkey)). Malloc'd string; caller frees. */
char *doltliteCredsKid(const DoltliteCreds *cred);
/* base32(pubkey), for the approval URL fragment. Malloc'd; caller frees. */
char *doltliteCredsPubKeyB32(const DoltliteCreds *cred);

/* Detached ed25519 (RFC 8032) signature over msg. Writes 64 bytes to sig. */
void doltliteCredsSign(const DoltliteCreds *cred, const unsigned char *msg,
                       size_t msglen, unsigned char sig[DOLTLITE_SIG_LEN]);

/* --- JWK persistence --- */

/* Serialize to an OKP/Ed25519 JWK JSON object (stores the 32-byte seed as
** "d", the pubkey as "x"). Malloc'd string; caller frees. */
char *doltliteCredsToJwk(const DoltliteCreds *cred);
/* Parse a JWK produced by doltliteCredsToJwk. Returns 0 on success. */
int doltliteCredsFromJwk(const char *json, DoltliteCreds **out);

/* --- File store (~/.doltlite/creds/<kid>.jwk, mode 0600) --- */

/* Resolve the creds directory: $DOLTLITE_CREDS_DIR if set, else
** $HOME/.doltlite/creds. Malloc'd string; caller frees. NULL if HOME unset. */
char *doltliteCredsDir(void);
/* Write cred to <dir>/<kid>.jwk (creating dir). dir NULL => default. */
int doltliteCredsSave(const DoltliteCreds *cred, const char *dir);
/* Load the credential with the given kid from dir (NULL => default). */
int doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out);

#ifdef __cplusplus
}
#endif

#endif /* DOLTLITE_CREDS_H */
