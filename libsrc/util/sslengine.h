/*
 *  Copyright (C) 2011-2014 OpenLink Software.
 *  All Rights Reserved.
 *
 *  The copyright above and this notice must be preserved in all
 *  copies of this source code.  The copyright above does not
 *  evidence any actual or intended publication of this source code.
 *
 *  This is unpublished proprietary trade secret of OpenLink Software.
 *  This source code may not be copied, disclosed, distributed, demonstrated
 *  or licensed except as authorized by OpenLink Software.
 */

#ifndef _SSLENGINE_H
#define _SSLENGINE_H

#include <openssl/ssl.h>

BEGIN_CPLUSPLUS

int ssl_engine_startup (void);
int ssl_engine_configure (const char *settings);
EVP_PKEY *ssl_load_privkey (const char *keyname, const void *keypass);
X509 *ssl_load_x509 (const char *filename);

END_CPLUSPLUS

#endif
