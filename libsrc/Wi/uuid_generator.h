#ifndef __UUID_GENERATOR_H
#define __UUID_GENERATOR_H

#if 0
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif


#ifdef _SSL
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#define MD5Init MD5_Init
#define MD5Update MD5_Update
#define MD5Final MD5_Final
#else
#include "../util/md5.h"
#endif /* _SSL */
#endif

#include "Dk.h"

VIRT_API_BEGIN
/* UUIDs generator */
#define UUIDS_PER_TICK 1024
/*  64 bit data type */
#ifdef WIN32
#define unsigned64_t unsigned __int64
#elif SIZEOF_LONG_LONG == 8
#define unsigned64_t unsigned long long
#elif SIZEOF_LONG == 8
#define unsigned64_t unsigned long
#endif
#define UUID_STATE "uuid_state"
typedef unsigned64_t uuid_time_t;

typedef struct
{
  char nodeID[6];
} uuid_node_t;

#undef uuid_t
typedef struct _uuid_t
{
  uint32 time_low;
  uint16 time_mid;
  uint16 time_hi_and_version;
  unsigned char clock_seq_hi_and_reserved;
  unsigned char clock_seq_low;
  unsigned char node[6];
} detailed_uuid_t;
#define uuid_t detailed_uuid_t

extern void uuid_set (uuid_t * u);
extern void uuid_str (char *p, int len);

VIRT_API_END
#endif /* __UUID_GENERATOR_H */
