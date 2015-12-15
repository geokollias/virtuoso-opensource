/*
 *  logmsg.h
 *
 *  $Id: logmsg.h,v 1.11 2013/01/02 11:35:23 source Exp $
 *
 *  Alternate logging module
 *
 *  Copyright (C) 1993-2013 OpenLink Software.
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

#ifndef _LOGMSG_H
#define _LOGMSG_H

#ifdef HAVE_SYSLOG
# include <syslog.h>
#else
# define LOG_EMERG	0	/* system is unusable */
# define LOG_ALERT	1	/* action must be taken immediately */
# define LOG_CRIT	2	/* critical conditions */
# define LOG_ERR	3	/* error conditions */
# define LOG_WARNING	4	/* warning conditions */
# define LOG_NOTICE	5	/* normal but signification condition */
# define LOG_INFO	6	/* informational */
# define LOG_DEBUG	7	/* debug-level messages */


BEGIN_CPLUSPLUS

int openlog (char *ident, int options, int facility);
int syslog (int level, char *format, ...);

END_CPLUSPLUS

#endif /* HAVE_syslog */

#define MAX_LOG_LEVEL	LOG_DEBUG

typedef struct _log LOG;

typedef void (*log_emit_func) (LOG *log, int level, char *msg);
typedef void (*log_close_func) (LOG *log);

struct _log
{
  struct _log *next;
  struct _log *prev;
  int mask[MAX_LOG_LEVEL + 1];
  int style;
  int month;
  int day;
  int year;
  log_emit_func emitter;
  log_close_func closer;
  void *user_data;
};

/* log styles */
#define L_STYLE_GROUP	0x0001	/* group by date	*/
#define L_STYLE_TIME	0x0002	/* include time/date 	*/
#define L_STYLE_LEVEL	0x0004	/* include level	*/
#define L_STYLE_PROG	0x0008	/* include program name */
#define L_STYLE_LINE	0x0010	/* include file/line	*/
#define L_STYLE_ALL	(L_STYLE_GROUP|L_STYLE_TIME|L_STYLE_LEVEL|L_STYLE_PROG|L_STYLE_LINE)

/* log masks */
#define L_MASK_ALL	-1	/* all catagories	*/

/* log levels */
#define L_EMERG   LOG_EMERG,__FILE__,__LINE__	/* system is unusabled	*/
#define L_ALERT	  LOG_ALERT,__FILE__,__LINE__	/* action must be taken	*/
#define L_CRIT	  LOG_CRIT,__FILE__,__LINE__	/* critical condition	*/
#define L_ERR	  LOG_ERR,__FILE__,__LINE__	/* error condition	*/
#define L_WARNING LOG_WARNING,__FILE__,__LINE__	/* warning condition	*/
#define L_NOTICE  LOG_NOTICE,__FILE__,__LINE__	/* normal but signif	*/
#define L_INFO    LOG_INFO,__FILE__,__LINE__	/* informational	*/
#define L_DEBUG   LOG_DEBUG,__FILE__,__LINE__	/* debug-level message	*/

/* used to parse a symbolic mask list */
typedef struct
{
  char *name;
  int bit;
} LOGMASK_ALIST;

/*
  Prototypes:
*/


BEGIN_CPLUSPLUS

/*
 *  Sorry for this one, but Oracle 7 has its own mandatory olm.o wich
 *  contains log (argh)
 */
#define log	logit

int   logmsg_ap (int level, char *file, int line, int mask, char *format, va_list ap);
int   logmsg (int level, char *file, int line, int mask, char *format, ...);
int   log_error (const char *format, ...);
int   log_warning (const char *format, ...);
int   log_info (const char *format, ...);
int   log_debug (const char *format, ...);
int   log (int level, const char *file, int line, const char *format, ...);
int   log_set_limit (int rate);
int   log_reset_limits (void);
int   log_set_mask (LOG * logptr, int level, int mask);
int   log_set_level (LOG * logptr, int level);
LOG * log_open_syslog (char *ident, int logopt, int facility, int level,
		      int mask, int style);
LOG * log_open_fp (FILE * fp, int level, int mask, int style);
LOG * log_open_fp2 (FILE * fp, int level, int mask, int style);
LOG * log_open_callback (log_emit_func emitter, log_close_func closer,
			int level, int mask, int style);
LOG * log_open_file (char *filename, int level, int mask, int style);
int   log_close (LOG * log);
void  log_close_all (void);
int   log_parse_mask (char *mask_str, LOGMASK_ALIST * alist, int size,
		      int *maskp);
void  log_flush_all (void);

END_CPLUSPLUS

#endif
