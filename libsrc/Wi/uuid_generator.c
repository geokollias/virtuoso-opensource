#include "uuid_generator.h"

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#if defined (__APPLE__) && defined(SPOTLIGHT)
#include <CoreFoundation/CoreFoundation.h>
#include <CoreFoundation/CFPlugInCOM.h>
#include <CoreServices/CoreServices.h>
#undef FAILED
#define _boolean
#endif

#include <stdio.h>
#include "sqlnode.h"
#include "sqlparext.h"
#include "security.h"
#include "sqlbif.h"

#ifdef __MINGW32__
#define P_tmpdir _P_tmpdir
#endif

#define UUID_T_DEFINED
#include "libutil.h"		/* needed by bif_cfg_* functions */
#include "statuslog.h"
#include "util/md5.h"
#include <sys/stat.h>

#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include "zlib.h"
#include "zutil.h"
#include "srvmultibyte.h"

#define FS_MAX_STRING	(10L * 1024L * 1024L)	/* allow files up to 10 MB */

#ifdef WIN32
#include <windows.h>
#define HAVE_DIRECT_H
#endif

#ifdef HAVE_DIRECT_H
#include <direct.h>
#include <io.h>
#define mkdir(p,m)	_mkdir (p)
#define FS_DIR_MODE	0777
#define PATH_MAX	 MAX_PATH
#define get_cwd(p,l)	_get_cwd (p,l)
#else
#include <dirent.h>
#define FS_DIR_MODE	 (S_IRWXU | S_IRWXG | S_IRWXO)
#endif

#include "datesupp.h"
#include "langfunc.h"
/* data type for UUID generator persistent state */

typedef struct
{
  uuid_node_t node;		/* saved node ID */
  unsigned short cs;		/* saved clock sequence */
} uuid_state;

static void get_system_time (uuid_time_t * uuid_time);
static void get_random_info (unsigned char seed[16]);

/* format_uuid_v1 -- make a UUID from the timestamp, clockseq, and node ID */
static void
format_uuid_v1 (uuid_t * uuid, unsigned short clock_seq, uuid_time_t timestamp, uuid_node_t node)
{
  uuid->time_low = (unsigned long) (timestamp & 0xFFFFFFFF);
  uuid->time_mid = (unsigned short) ((timestamp >> 32) & 0xFFFF);
  uuid->time_hi_and_version = (unsigned short) ((timestamp >> 48) & 0x0FFF);
  uuid->time_hi_and_version |= (1 << 12);
  uuid->clock_seq_low = clock_seq & 0xFF;
  uuid->clock_seq_hi_and_reserved = (clock_seq & 0x3F00) >> 8;
  uuid->clock_seq_hi_and_reserved |= 0x80;
  memcpy (&uuid->node, &node, sizeof uuid->node);
}

static void
get_current_time (uuid_time_t * timestamp)
{
  uuid_time_t time_now;
  static uuid_time_t time_last;
  static unsigned short uuids_this_tick;
  static int inited = 0;

  if (!inited)
    {
      get_system_time (&time_last);
      uuids_this_tick = 0;
      inited = 1;
    }
  while (1)
    {
      get_system_time (&time_now);

      /* if clock reading changed since last UUID generated... */
      if (time_last != time_now)
	{
	  /* reset count of uuids gen'd with this clock reading */
	  time_last = time_now;
	  uuids_this_tick = 0;
	  break;
	};
      if (uuids_this_tick < UUIDS_PER_TICK)
	{
	  uuids_this_tick++;
	  break;
	};			/* going too fast for our clock; spin */
    };				/* add the count of uuids to low order bits of the clock reading */

  *timestamp = time_now + uuids_this_tick;
}

static unsigned short
true_random (void)
{
  uuid_time_t time_now;

  get_system_time (&time_now);
  time_now = time_now / UUIDS_PER_TICK;
  srand ((unsigned int) (((time_now >> 32) ^ time_now) & 0xffffffff));
  return rand ();
}

static void
get_pseudo_node_identifier (uuid_node_t * node)
{
  unsigned char seed[16];
  get_random_info (seed);
  seed[0] |= 0x80;
  memcpy (node, seed, sizeof (*node));
}

/* system dependent call to get the current system time.
   Returned as 100ns ticks since Oct 15, 1582, but resolution may be
   less than 100ns.  */
#ifdef WIN32
static void
get_system_time (uuid_time_t * uuid_time)
{
  ULARGE_INTEGER time;
  GetSystemTimeAsFileTime ((FILETIME *) & time);
  /* NT keeps time in FILETIME format which is 100ns ticks since
     Jan 1, 1601.  UUIDs use time in 100ns ticks since Oct 15, 1582.
     The difference is 17 Days in Oct + 30 (Nov) + 31 (Dec)
     + 18 years and 5 leap days.        */
  time.QuadPart += (unsigned __int64) (1000 * 1000 * 10)	/* seconds */
      * (unsigned __int64) (60 * 60 * 24)	/* days */
      * (unsigned __int64) (17 + 30 + 31 + 365 * 18 + 5);	/* # of days */
  *uuid_time = time.QuadPart;
}

static void
get_random_info (unsigned char seed[16])
{
  MD5_CTX c;
  struct
  {
    MEMORYSTATUS m;
    SYSTEM_INFO s;
    FILETIME t;
    LARGE_INTEGER pc;
    DWORD tc;
    DWORD l;
    char hostname[MAX_COMPUTERNAME_LENGTH + 1];
  } r;

  memset (&c, 0, sizeof (MD5_CTX));
  MD5Init (&c);			/* memory usage stats */
  GlobalMemoryStatus (&r.m);	/* random system stats */
  GetSystemInfo (&r.s);		/* 100ns resolution (nominally) time of day */
  GetSystemTimeAsFileTime (&r.t);	/* high resolution performance counter */
  QueryPerformanceCounter (&r.pc);	/* milliseconds since last boot */
  r.tc = GetTickCount ();
  r.l = MAX_COMPUTERNAME_LENGTH + 1;

  GetComputerName (r.hostname, &r.l);
  MD5Update (&c, (unsigned char *) &r, sizeof (r));
  MD5Final (seed, &c);
}

#else /* UNIX */
static void
get_system_time (uuid_time_t * uuid_time)
{
  struct timeval tp;
  gettimeofday (&tp, (struct timezone *) 0);
  /* Offset between UUID formatted times and Unix formatted times.
     UUID UTC base time is October 15, 1582.
     Unix base time is January 1, 1970.   */
  *uuid_time = ((uuid_time_t) (tp.tv_sec) * 10000000) + ((uuid_time_t) (tp.tv_usec) * 10) + 0x01B21DD213814000LL;
}

static void
get_random_info (unsigned char seed[16])
{
  MD5_CTX c;

  struct
  {
    pid_t pid;
    struct timeval t;
    char hostname[257];
  } r;

  memset (&c, 0, sizeof (MD5_CTX));
  MD5Init (&c);
  r.pid = getpid ();
  gettimeofday (&r.t, (struct timezone *) 0);
  gethostname (r.hostname, 256);
  MD5Update (&c, (unsigned char *) &r, sizeof (r));
  MD5Final (seed, &c);
}
#endif /* end system specific routines */


static uuid_state *ustate = NULL;

void
uuid_set (uuid_t * u)
{
  uuid_time_t timestamp;
  char p[200];

  if (!ustate)
    {
#if 1
      caddr_t saved_state;
      unsigned char node[sizeof (uuid_node_t)];
      ustate = (uuid_state *) dk_alloc (sizeof (uuid_state));
      IN_TXN;
      saved_state = registry_get (UUID_STATE);
      if (!saved_state)
	{
	  ustate->cs = true_random ();
	  get_pseudo_node_identifier (&ustate->node);
#ifdef VALGRIND
	  memset (node, 0, sizeof (node));
#endif
	  memcpy (node, &ustate->node, sizeof (uuid_node_t));
	  snprintf (p, sizeof (p), "%d %02X%02X%02X%02X%02X%02X", ustate->cs,
	      (unsigned) (node[0]), (unsigned) (node[1]), (unsigned) (node[2]),
	      (unsigned) (node[3]), (unsigned) (node[4]), (unsigned) (node[5]));
	  registry_set (UUID_STATE, p);
	}
      else
	{
	  int cs;
	  unsigned n1[sizeof (uuid_node_t)];
	  sscanf (saved_state, "%d %02X%02X%02X%02X%02X%02X", &cs, &n1[0], &n1[1], &n1[2], &n1[3], &n1[4], &n1[5]);
	  node[0] = n1[0];
	  node[1] = n1[1];
	  node[2] = n1[2];
	  node[3] = n1[3];
	  node[4] = n1[4];
	  node[5] = n1[5];
	  ustate->cs = cs;
	  memcpy (&ustate->node, node, sizeof (uuid_node_t));
	  dk_free_box (saved_state);
	}
      LEAVE_TXN;
#else
      ustate = dk_alloc (sizeof (uuid_state));
      ustate->cs = true_random ();
      get_pseudo_node_identifier (&ustate->node);
#endif
    }

  get_current_time (&timestamp);
  format_uuid_v1 (u, ustate->cs, timestamp, ustate->node);

  return;
}

void
uuid_str (char *p, int len)
{
  uuid_t u;

  memset (&u, 0, sizeof (uuid_t));
  uuid_set (&u);

  snprintf (p, len, "%08lX-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
      (unsigned long) u.time_low, u.time_mid, u.time_hi_and_version,
      u.clock_seq_hi_and_reserved, u.clock_seq_low, u.node[0], u.node[1], u.node[2], u.node[3], u.node[4], u.node[5]);
}
