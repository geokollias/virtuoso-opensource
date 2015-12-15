/*
 *  ncfg.h
 *
 *  $Id: ncfg.h,v 1.3 2002/12/03 09:44:38 source Exp $
 *
 *  New Configuration File Management
 *
 *  (C)Copyright 1997 OpenLink Software.
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

#ifndef _NCFG_H
#define _NCFG_H

/* message digest using md5 */
typedef unsigned char digest_t[16];

/* configuration file entry */
typedef struct TCFGENTRY
  {
    char *section;
    char *id;
    char *value;
    char *comment;
    u_short flags;
  } TCFGENTRY, *PCFGENTRY;

/* values for flags */
#define CFE_MUST_FREE_SECTION	0x8000
#define CFE_MUST_FREE_ID	0x4000
#define CFE_MUST_FREE_VALUE	0x2000
#define CFE_MUST_FREE_COMMENT	0x1000

/* configuration file */
typedef struct TCFGDATA
  {
    char *fileName;		/* Current file name */

    int dirty;			/* Did we make modifications? */

    char *image;		/* In-memory copy of the file */
    size_t size;		/* Size of this copy (excl. \0) */
    time_t mtime;		/* Modification time */
    digest_t digest;		/* MD5 Sum of the contents */

    u_int numEntries;
    u_int maxEntries;
    PCFGENTRY entries;

    /* Compatibility */
    u_int cursor;
    char *section;
    char *id;
    char *value;
    char *comment;
    u_short flags;

    OPL_MUTEX_DECLARE (mtx);

  } TCONFIG, *PCONFIG;

#define CFG_VALID		0x8000
#define CFG_EOF			0x4000

#define CFG_ERROR		0x0000
#define CFG_SECTION		0x0001
#define CFG_DEFINE		0x0002
#define CFG_CONTINUE		0x0003

#define CFG_TYPEMASK		0x000F
#define CFG_TYPE(X)		((X) & CFG_TYPEMASK)
#define cfg_valid(X)		((X) != NULL && ((X)->flags & CFG_VALID))
#define cfg_eof(X)		((X)->flags & CFG_EOF)
#define cfg_section(X)		(CFG_TYPE((X)->flags) == CFG_SECTION)
#define cfg_define(X)		(CFG_TYPE((X)->flags) == CFG_DEFINE)
#define cfg_continue(X)		(CFG_TYPE((X)->flags) == CFG_CONTINUE)

/*
 *  Change function calls to be inside OpenLink namespace instead of 
 *  risking name clashes with other libraries (like with Solid)
 */
#define cfg_init	OPL_Cfg_init
#define cfg_init2	OPL_Cfg_init2
#define cfg_done 	OPL_Cfg_done
#define cfg_freeimage	OPL_Cfg_freeimage
#define cfg_refresh	OPL_Cfg_refresh
#define cfg_storeentry	OPL_Cfg_storeentry
#define cfg_rewind 	OPL_Cfg_rewind
#define cfg_nextentry 	OPL_Cfg_nextentry
#define cfg_find 	OPL_Cfg_find
#define cfg_write	OPL_Cfg_write
#define cfg_commit	OPL_Cfg_commit
#define cfg_getstring 	OPL_Cfg_getstring
#define cfg_getlong	OPL_Cfg_getlong
#define cfg_getshort	OPL_Cfg_getshort

BEGIN_CPLUSPLUS

int cfg_init (PCONFIG *ppconf, char *filename);
int cfg_init2 (PCONFIG *ppconf, char *filename, int doCreate);
int cfg_done (PCONFIG pconfig);
int cfg_freeimage (PCONFIG pconfig);
int cfg_refresh (PCONFIG pconfig);
int cfg_storeentry (PCONFIG pconfig, char *section, char *id, char *value, char *comment, int dynamic);
int cfg_rewind (PCONFIG pconfig);
int cfg_nextentry (PCONFIG pconfig);
int cfg_find (PCONFIG pconfig, char *section, char *id);
int cfg_merge (PCONFIG pconfig, PCONFIG src);
int cfg_write (PCONFIG pconfig, char *section, char *id, char *value);
int cfg_commit (PCONFIG pconfig);
int cfg_getstring (PCONFIG pconfig, char *section, char *id, char **valptr);
int cfg_getlong (PCONFIG pconfig, char *section, char *id, int32  *valptr);
int cfg_getshort (PCONFIG pconfig, char *section, char *id, short *valptr);

END_CPLUSPLUS

#endif /* _NCFG_H */
