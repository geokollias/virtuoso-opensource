#ifndef __HTTP_SSL_H
#define __HTTP_SSL_H
#ifdef _SSL
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/asn1.h>
#include <openssl/bn.h>
#include <openssl/dsa.h>
#include <openssl/sha.h>
#include <openssl/rsa.h>
#include <openssl/crypto.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/pkcs12.h>
#include <openssl/safestack.h>
#include <openssl/bio.h>
#include <openssl/asn1.h>
#include <openssl/md5.h>
#else
#ifndef VOSBLD
#define MD5_CTX virt__MD5_CTX
#endif
#include "util/md5.h"
#endif
#include "Dk.h"

VIRT_API_BEGIN

#ifdef _SSL
void * tcpses_get_sslctx (session_t * ses);
void tcpses_set_sslctx (session_t * ses, void * ssl_ctx);
extern int ssl_ctx_set_cipher_list (SSL_CTX *ctx, char *cipher_list);
extern int ssl_ctx_set_protocol_options (SSL_CTX *ctx, char *protocols);
#endif

#if defined (_SSL) && !defined (NO_THREAD)
extern int ssl_server_set_certificate (SSL_CTX* ssl_ctx, char * cert_name, char * key_name, char * extra);
#endif

int ssl_client_use_pkcs12 (SSL * ssl, char *pkcs12file, char *passwd, char *ca);


VIRT_API_END

#endif /* __HTTP_SSL_H */