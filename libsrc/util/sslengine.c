/*
 *  sslengine.c
 *
 *  $Id$
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2014 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

/* Implements glue code to support OpenSSL Engines in the client/server model.
 * The functions here primarily deal with configuring engines and the
 * loading of private key material through an engine.
 */

/* #define WITH_SSL_ENGINES */
/* #define TRACE */

#include "libutil.h"
#include "util/sslengine.h"
#include <openssl/err.h>

#ifdef WITH_SSL_ENGINES
# include <openssl/engine.h>
# if OPENSSL_VERSION_NUMBER < 0x10000000L
#  error OpenSSL too old to use engines
# endif

/******************************************************************************/

/* An internal replacement for OpenSSL's ENGINE_openssl.
 * This engine exist to have a coherent scheme for loading private keys.
 * Engines use a UI_METHOD to get the password while retrieving private keys,
 * whereas PEM_read_bio_PrivateKey uses a callback function.
 * Methods in engines are not engaged by Virtuoso by default, unless the
 * user configures the engine in the virtuoso.ini file as 'default'.
 */

static EVP_PKEY *
openssl_load_privkey (ENGINE *eng, const char *key_id,
    UI_METHOD *ui_method, void *callback_data);

static int
bind_helper (ENGINE *e)
{
  if (!ENGINE_set_id (e, "openssl") ||
      !ENGINE_set_name (e, "Software engine support") ||
#ifndef OPENSSL_NO_RSA
      !ENGINE_set_RSA (e, RSA_get_default_method ()) ||
#endif
#ifndef OPENSSL_NO_DSA
      !ENGINE_set_DSA (e, DSA_get_default_method ()) ||
#endif
#ifndef OPENSSL_NO_ECDH
      !ENGINE_set_ECDH (e, ECDH_OpenSSL ()) ||
#endif
#ifndef OPENSSL_NO_ECDSA
      !ENGINE_set_ECDSA (e, ECDSA_OpenSSL ()) ||
#endif
#ifndef OPENSSL_NO_DH
      !ENGINE_set_DH (e, DH_get_default_method ()) ||
#endif
      !ENGINE_set_RAND (e, RAND_SSLeay ()) ||
      !ENGINE_set_load_privkey_function (e, openssl_load_privkey))
    {
      return 0;
    }

  return 1;
}


static ENGINE *
engine_openssl (void)
{
  ENGINE *e = ENGINE_new ();
  if (!e)
    return NULL;
  if (!bind_helper (e))
    {
      ENGINE_free (e);
      return NULL;
    }
  return e;
}


static void
ENGINE_load_openssl_2 (void)
{
  ENGINE *e = engine_openssl ();
  if (!e) return;
  ENGINE_add (e);
  ENGINE_free (e);
  ERR_clear_error ();
}

struct ui_cb_data
  {
    const char *key_id;
    UI_METHOD *ui_method;
    void *callback_data;
  };

static int
file_pwd_cb (char *buf, int num, int w, void *ctx)
{
  struct ui_cb_data *cb = (struct ui_cb_data *) ctx;
  UI *ui;
  char *prompt;
  int ok = -1;

  buf[0] = 0;
  if (cb->ui_method && num > 1)
    {
      ui = UI_new_method (cb->ui_method);
      prompt = UI_construct_prompt (ui, "pass phrase", cb->key_id);
      ok = UI_add_input_string (ui, prompt, UI_INPUT_FLAG_DEFAULT_PWD, buf, 0, num - 1);
      UI_add_user_data (ui, cb->callback_data);
      UI_ctrl (ui, UI_CTRL_PRINT_ERRORS, 1, 0, 0);
      if (ok >= 0)
	{
	  do
	    {
	      ok = UI_process (ui);
	    } while (ok < 0 && UI_ctrl (ui, UI_CTRL_IS_REDOABLE, 0, 0, 0));
	}
      UI_free (ui);
      OPENSSL_free (prompt);
    }

  return ok >= 0 ? (int) strlen (buf) : 0;
}

static EVP_PKEY *
openssl_load_privkey (ENGINE * eng, const char *key_id,
    UI_METHOD * ui_method, void *callback_data)
{
  struct ui_cb_data cbdata;
  EVP_PKEY *pkey = NULL;
  BIO *bio_in;

  cbdata.key_id = key_id;
  cbdata.ui_method = ui_method;
  cbdata.callback_data = callback_data;

  if ((bio_in = BIO_new_file (key_id, "r")) != NULL)
    {
      pkey = PEM_read_bio_PrivateKey (bio_in, NULL, file_pwd_cb, &cbdata);
      BIO_free (bio_in);
    }

  return pkey;
}

/******************************************************************************/

/* A pointer to this structure is passed together with an UI_METHOD *
 * to an OpenSSL engine.
 *
 * The UI_METHOD implementation is here to be compatible with existing
 * engines that use OpenSSL UI in order to retrieve the password for
 * the private key.
 */

struct veng_ctx_s
  {
    unsigned int magic;		/* identifies Virtuoso magic context */
    const void *keypass;	/* password for the private key */
  };

#define VIRTUOSO_CTX_MAGIC	0x8395abf3	/* arbitrary */


/* An OpenSSL UI_METHOD that doesn't do I/O but returns the password from
 * the application context
 */

static int
ui_read (UI *ui, UI_STRING *uis)
{
  struct veng_ctx_s *ctx;

  if (UI_get_string_type (uis) == UIT_PROMPT &&
      (UI_get_input_flags (uis) & UI_INPUT_FLAG_DEFAULT_PWD) &&
      (ctx = (struct veng_ctx_s *) UI_get0_user_data (ui)) != NULL &&
      ctx->magic == VIRTUOSO_CTX_MAGIC)
    {
      UI_set_result (ui, uis, (const char *) ctx->keypass);
      return 1;
    }

  return 0;
}


static int
ui_write (UI *ui, UI_STRING *uis)
{
  return 1;
}


static int
ui_open_close (UI *ui)
{
  return 1;
}


static UI_METHOD *
UI_get_server_method (void)
{
  UI_METHOD *ui_method = UI_create_method ("server");
  UI_method_set_reader (ui_method, ui_read);
  UI_method_set_writer (ui_method, ui_write);
  UI_method_set_opener (ui_method, ui_open_close);
  UI_method_set_closer (ui_method, ui_open_close);

  return ui_method;
}

/******************************************************************************/

/* Load & configure an engine for future use.
 * Settings contains a space-separated list of name[=value] pairs,
 * the first indicating the name of the engine to configure.
 * eg:
    ssl_engine_configure (
      "capi"
      " key_type=1 csp_name=\"Microsoft Enhanced Cryptographic Provider v1.0\"
      " lookup_method=1 store_name=\"MY\" store_flags=0");
 */

int
ssl_engine_configure (const char *settings)
{
  char **av, *s, *name, *value, *engine;
  int ac, i, failed;
  int dynamic = 0;
  ENGINE *e;

  build_argv_from_string (settings, &ac, &av);
  if (ac == 0)
    return -1;

  /* 1st parameter is the engine to load */
  engine = av[0];

  /* If the engine isn't built-in, see if we can load it */
  if ((e = ENGINE_by_id (engine)) == NULL &&
      (e = ENGINE_by_id ("dynamic")) != NULL)
    {
      ERR_clear_error ();
      dynamic = 1;
      if (!ENGINE_ctrl_cmd_string (e, "SO_PATH", engine, 0) ||
	  !ENGINE_ctrl_cmd_string (e, "LOAD", NULL, 0))
	{
	  ENGINE_free (e);
	  e = NULL;
	}
    }

#ifdef TRACE
  if (e)
    printf ("Loaded: %s\n", engine);
#endif

  /* Need to init before we can configure the engine */
  if (e && !ENGINE_init (e))
    {
      ENGINE_free (e);
      e = NULL;
    }

#ifdef TRACE
  if (e)
    printf ("Initialized: %s\n", engine);
#endif

  /* Send key/value pairs to the engine */
  if (e)
    {
      failed = 0;
      for (i = 1; i < ac; i++)
	{
	  name = av[i];
	  if ((s = strchr (name, '=')) == NULL)
	    value = NULL;
	  else
	    {
	      *s++ = '\0';
	      value = s;
	    }

#ifdef TRACE
	  printf ("CONFIGURE [%s]=[%s]\n", name, value);
#endif
	  /* Special value "default" : register the engine as default for all
	   * ciphers and methods it supports
	   */
	  if (!strcmp (name, "default"))
	    {
	      if (!ENGINE_set_default(e, ENGINE_METHOD_ALL))
		{
#ifdef TRACE
		  printf ("Failed to set engine '%s' as default\n", engine);
#endif
		  failed = 1;
		}
	    }
	  else if (!ENGINE_ctrl_cmd_string (e, name, value, 0))
	    {
#ifdef TRACE
	      printf ("engine '%s' failed to handle '%s' setting\n", engine, name);
#endif
	      failed = 1;
	    }
	}
      if (failed)
	{
	  ENGINE_free (e);
	  e = NULL;
	}
    }

  if (e && dynamic)
    ENGINE_add (e);

#ifdef TRACE
  if (e == NULL)
    printf ("Failed to initialize engine '%s'\n", engine);
#endif

  free_argv (av);

  return e == NULL ? -1 : 0;
}


int
ssl_engine_startup (void)
{
#ifdef _WIN32
  /* XXX XXX IMPORTANT XXX XXX
   *
   * OpenSSLs new nifty Applink/Uplink scheme will break for OpenLink when
   * we're trying to use SSL in a client DLL. OpenSSL's ms/uplink.c attempts
   * to locate the magic OPENSSL_AppLink symbol in the main executable,
   * which, in case of the ODBC client, will most likely not export that.
   * As a result, any application using ODBC may die when trying to make a
   * secure Virtuoso connection through the OpenLink ODBC layer (!)
   *
   * OpenSSL wants to cater for incompatibilities between mixed-mode DLLs
   * (-MD/-MT etc), but that won't work for OpenLink.
   * We cannot statically link in the OpenSSL runtime, because we're loading
   * DLLs that use OpenSLL shared libraries.
   * So we must link to shared OpenSSL, but we cannot guarantee the
   * existence of OPENSSL_AppLink in the main executable, which is a
   * requirement for OpenSSL.
   *
   * Hopefully, this all changes in a future release of OpenSSL.
   * For now, you need to build OpenSSL without the -DOPENSSL_USE_APPLINK
   * compiler flag, so this feature is disabled.
   * Configure won't do this for you; you'll have to patch util/pl/VC-32.pl.
   * Sorry to all VOS users, who will now have to do a custom build of OpenSSL.
   * In addition, you'll need to compile all OpenLink code with -MD. Period.
   *
   * The additional version check is to make sure that we don't
   * accidentally use a different OpenSSL runtime which *may* have
   * changed their internal structures for engines. Paranoid approach.
   *
   * XXX XXX
   */
  const char *cflags = SSLeay_version (SSLEAY_CFLAGS);
  if (strstr (cflags, "-DOPENSSL_USE_APPLINK") != NULL ||
      strstr (cflags, "/MD") == NULL ||
      SSLeay () != SSLEAY_VERSION_NUMBER)
    {
      OpenSSLDie (__FILE__, __LINE__, "shared library requirements not met");
    }
#endif

  CRYPTO_malloc_init ();
  ERR_load_crypto_strings();
  OpenSSL_add_all_algorithms();

  ENGINE_load_builtin_engines ();
  ENGINE_load_openssl_2 ();

  ssl_engine_configure ("openssl");

  return 0;
}

/******************************************************************************/

EVP_PKEY *
ssl_load_privkey (const char *keyname, const void *keypass)
{
  char *copy = NULL;
  char *scheme, *s;
  ENGINE *e;
  EVP_PKEY *pkey = NULL;
  UI_METHOD *ui_method = NULL;

  if (!keyname || (copy = strdup (keyname)) == NULL)
    return NULL;

  /* don't get confused by windows filenames */
  if ((s = strchr (copy, ':')) != NULL && s[1] && s[1] != '\\')
    {
      *s++ = '\0';
      scheme = copy;
      keyname = s;
    }
  else
    {
      scheme = "openssl";
      keyname = copy;
    }

  if ((e = ENGINE_by_id (scheme)) != NULL)
    {
      struct veng_ctx_s ctx;
      ctx.magic = VIRTUOSO_CTX_MAGIC;
      ctx.keypass = keypass;

      if (keypass)
	{
	  ui_method = UI_get_server_method ();
	  pkey = ENGINE_load_private_key (e, keyname, ui_method, &ctx);
	  UI_destroy_method (ui_method);
	}
      else
	{
	  /* lets OpenSSL prompt for the password if required */
	  ui_method = (UI_METHOD *) UI_get_default_method ();
	  pkey = ENGINE_load_private_key (e, keyname, ui_method, &ctx);
	}

      ENGINE_free (e);
    }

#ifdef TRACE
  if (pkey == NULL)
    printf ("Failed to load private key '%s' with '%s'\n", keyname, scheme);
  else
    printf ("Loaded private key '%s' with '%s'\n", keyname, scheme);
#endif

  if (copy)
    free (copy);

  return pkey;
}

#else /* WITH_SSL_ENGINES */

/* partial functions when SSL engine support is not compiled in */

int
ssl_engine_startup (void)
{
  CRYPTO_malloc_init ();
  ERR_load_crypto_strings();
  OpenSSL_add_all_algorithms();

  return 0;
}


int
ssl_engine_configure (const char *settings)
{
  return 0;
}


EVP_PKEY *
ssl_load_privkey (const char *keyname, const void *keypass)
{
  EVP_PKEY *pkey = NULL;
  BIO *bio_in;
  char *s;

  if ((bio_in = BIO_new_file (keyname, "r")) != NULL)
    {
      pkey = PEM_read_bio_PrivateKey (bio_in, NULL, NULL, NULL);
      BIO_free (bio_in);
    }

  return pkey;
}

#endif /* WITH_SSL_ENGINES */

/******************************************************************************/

X509 *
ssl_load_x509 (const char *filename)
{
  X509 *x509 = NULL;
  BIO *bio_in;

  if ((bio_in = BIO_new_file (filename, "r")) != NULL)
    {
      x509 = PEM_read_bio_X509 (bio_in, NULL, NULL, NULL);

      /* attempt binary certificates too, it's a native format on Windows */
      if (x509 == NULL &&
	(ERR_GET_REASON (ERR_peek_last_error ()) == PEM_R_NO_START_LINE))
	{
	  ERR_clear_error ();
	  BIO_seek (bio_in, 0);
	  x509 = d2i_X509_bio (bio_in, NULL);
	}

      BIO_free (bio_in);
    }

  return x509;
}
