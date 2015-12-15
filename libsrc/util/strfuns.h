/*
 *  strfuns.h
 *
 *  $Id: strfuns.h,v 1.24 2013/01/02 11:35:23 source Exp $
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

#ifndef _STRFUNS_H
#define _STRFUNS_H

#ifndef VMS
struct tm;	/* VAX C complains on this type of declarations */
#endif

#ifdef __THROW
#define __THROW_AS_IN_SYS __THROW
#else
#define __THROW_AS_IN_SYS
#endif

BEGIN_CPLUSPLUS

char *	cslentry (const char *list, int entry);
int	cslnumentries (const char *list);
int	csllookup (const char *list, const char *expr);
int	make_env (const char *id, const char *value);
#ifndef WIN32
int	stricmp (const char *, const char *);
int	strnicmp (const char *, const char *, size_t);
char *	strlwr (char *);
char *	strupr (char *);
#endif
char *	rtrim (char *);
const char *ltrim (const char *);
char *	strindex (const char *str, const char *find);
void	strinsert (char *where, const char *what);
char *	strexpect (char *keyword, char *source);
char *	strexpect_cs (char *keyword, char *source);
int	build_argv_from_string (const char *s, int *pargc, char ***pargv);
void	free_argv (char **argv);
char *	fntodos (char *filename);
char *	fnundos (char *filename);
char *	fnqualify (char *filename);
char *	fnsearch (const char *filename, const char *path);
char *	quotelist (char* szIn);
int	StrCopyIn (char **poutStr, char *inStr, ssize_t size);
int	StrCopyInUQ (char **poutStr, char *inStr, ssize_t size);
int	strcpy_out (const char *inStr, char *outStr, size_t size, size_t *result);
char *	strquote (char *s, ssize_t size, int quoteChr);
char *	strunquote (char *s, ssize_t size, int quoteChr);

#if !defined (HAVE_STRDUP) && !defined (MALLOC_DEBUG)
#undef strdup
char *	strdup (const char *);
#endif

#ifndef HAVE_STPCPY
#undef stpcpy
char *	stpcpy (char *, const char *);
#endif

#ifndef HAVE_STRFTIME
size_t	strftime (char *, size_t, const char *, const struct tm *);
#endif

#ifndef HAVE_MEMMOVE
void *	memmove (void *dest, const void *src, size_t count);
#endif

char *	opl_strerror (int err);


#ifndef HAVE_WCSLEN
size_t wcslen (wchar_t *wcs);
#endif

#ifndef HAVE_WCSCPY
wchar_t *wcscpy (wchar_t *dest, const wchar_t *src);
#endif

#ifndef HAVE_WCSDUP
wchar_t *wcsdup (wchar_t *s);
#endif

#ifndef HAVE_WCSCMP
int wcscmp (wchar_t *s1, wchar_t *s2);
#endif

#ifndef HAVE_TOWLOWER
wint_t towlower (wint_t wc);
#endif

#ifndef HAVE_TOWUPPER
wint_t towupper (wint_t wc);
#endif

#ifndef HAVE_WCSCASECMP
int wcscasecmp (wchar_t *s1, wchar_t *s2);
#endif

#ifndef HAVE_WCSTOL
long int wcstol (const wchar_t *ptr, wchar_t **endptr, int base) __THROW_AS_IN_SYS;
#endif

#ifndef HAVE_WCSTOD
double wcstod (const wchar_t *ptr, wchar_t **endptr) __THROW_AS_IN_SYS;
#endif

#ifndef HAVE_WCSNCPY
wchar_t *wcsncpy (wchar_t *dest, const wchar_t *src, size_t n) __THROW_AS_IN_SYS;
#endif

#ifndef HAVE_WCSCHR
wchar_t *wcschr (const wchar_t *wcs, const wchar_t wc) __THROW_AS_IN_SYS;
#endif

#ifndef HAVE_WCSCAT
wchar_t *wcscat (wchar_t *dest, const wchar_t *src) __THROW_AS_IN_SYS;
#endif

#ifndef HAVE_WCSNCAT
wchar_t *wcsncat (wchar_t *dest, const wchar_t *src, size_t n) __THROW_AS_IN_SYS;
#endif

END_CPLUSPLUS

#endif
