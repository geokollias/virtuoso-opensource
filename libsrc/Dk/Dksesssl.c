#include "Dk.h"
#include "Dksestcp.h"
#include "Dksestcpint.h"
#include "Dksesssl.h"

#ifdef _SSL

/* SSL support */
static int
sslses_read (session_t * ses, char *buffer, int n_bytes)
{
  int n_in;
  if (ses->ses_class == SESCLASS_UNIX)
    {
      SESSTAT_CLR (ses, SST_OK);
      SESSTAT_SET (ses, SST_BROKEN_CONNECTION);
      return 0;
    }
  ses->ses_status = 0;
  SESSTAT_SET (ses, SST_OK);
  n_in = SSL_read ((SSL *) (ses->ses_device->dev_connection->ssl), buffer, n_bytes);
  if (n_in <= 0)
    {
      int error_code = SSL_get_error ((SSL *) (ses->ses_device->dev_connection->ssl), n_in);
      if (SSL_ERROR_WANT_READ == error_code || SSL_ERROR_WANT_WRITE == error_code)
	{
	  SESSTAT_CLR (ses, SST_OK);
	  SESSTAT_SET (ses, SST_BLOCK_ON_READ);
	}
      else
	{
	  SESSTAT_CLR (ses, SST_OK);
	  SESSTAT_SET (ses, SST_BROKEN_CONNECTION);
	}
    }
  ses->ses_bytes_read = n_in;
  return (n_in);
}


static int
sslses_write (session_t * ses, char *buffer, int n_bytes)
{
  int n_out;
  if (ses->ses_class == SESCLASS_UNIX)
    {
      SESSTAT_W_CLR (ses, SST_OK);
      SESSTAT_W_SET (ses, SST_BROKEN_CONNECTION);
      return 0;
    }
  SESSTAT_W_SET (ses, SST_OK);
  SESSTAT_W_CLR (ses, SST_BLOCK_ON_WRITE);
  n_out = SSL_write ((SSL *) (ses->ses_device->dev_connection->ssl), buffer, n_bytes);
  if (n_out <= 0)
    {
      int error_code = SSL_get_error ((SSL *) (ses->ses_device->dev_connection->ssl), n_out);
      if (SSL_ERROR_WANT_READ == error_code || SSL_ERROR_WANT_WRITE == error_code)
	{
	  SESSTAT_W_CLR (ses, SST_OK);
	  SESSTAT_W_SET (ses, SST_BLOCK_ON_WRITE);
	}
      else
	{
	  SESSTAT_W_CLR (ses, SST_OK);
	  SESSTAT_W_SET (ses, SST_BROKEN_CONNECTION);
	}
    }
  ses->ses_bytes_written = n_out;
  return (n_out);
}


static int
ssldev_free (device_t * dev)
{
  dbg_printf_1 (("tcpdev_free."));

  if ((dev == NULL) || (dev->dev_check != TCP_CHECKVALUE))
    {
      dbg_printf_2 (("SER_ILLSESP"));
      return (SER_ILLSESP);
    }

  SSL_free ((SSL *) dev->dev_connection->ssl);
  free ((char *) dev->dev_address);
  free ((char *) dev->dev_connection);
  free ((char *) dev->dev_funs);
  free ((char *) dev->dev_accepted_address);

  /* Set the check-field anything but TCP_CHECKVALUE */
  dev->dev_check = TCP_CHECKVALUE - 9;

  free ((char *) dev);
  dbg_printf_2 (("SER_SUCC."));
  return (SER_SUCC);
}


caddr_t
tcpses_get_ssl (session_t * ses)
{
  return ((caddr_t) (ses->ses_device->dev_connection->ssl));
}


void *
tcpses_get_sslctx (session_t * ses)
{
  if (ses && ses->ses_device && ses->ses_device->dev_connection)
    return ((ses->ses_device->dev_connection->ssl_ctx));
  return NULL;
}


void
tcpses_set_sslctx (session_t * ses, void *ssl_ctx)
{
  if (ses->ses_class == SESCLASS_UNIX)
    return;
  if (ses && ses->ses_device && ses->ses_device->dev_connection)
    ses->ses_device->dev_connection->ssl_ctx = ssl_ctx;
  return;
}


void
sslses_to_tcpses (session_t * ses)
{
  if (ses->ses_class == SESCLASS_UNIX)
    return;
  if (ses->ses_device->dev_connection->ssl)
    SSL_free ((SSL *) (ses->ses_device->dev_connection->ssl));
  ses->ses_device->dev_funs->dfp_read = tcpses_read;
  ses->ses_device->dev_funs->dfp_write = tcpses_write;
  ses->ses_device->dev_funs->dfp_free = tcpdev_free;
  ses->ses_device->dev_connection->ssl = NULL;
}


void
tcpses_to_sslses (session_t * ses, void *s_ssl)
{
  if (ses->ses_class == SESCLASS_UNIX)
    return;
  ses->ses_device->dev_funs->dfp_read = sslses_read;
  ses->ses_device->dev_funs->dfp_write = sslses_write;
  ses->ses_device->dev_funs->dfp_free = ssldev_free;
  ses->ses_device->dev_connection->ssl = (SSL *) s_ssl;
}


/* END SSL support*/
#endif
