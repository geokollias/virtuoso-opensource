/*
 *  cfg2.c
 *
 *  $Id: cfg2.c,v 1.4 1995/03/20 17:40:00 openlink Exp $
 *
 *  Configuration Management
 *
 *  (C)Copyright 1993, 1994 OpenLink Software.
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

#include "libutil.h"


int
cfg_getstring (PCONFIG pconfig, const char *section, const char *id, char **valptr)
{
  if (cfg_find (pconfig, section, id))
    return -1;

  *valptr = pconfig->value;
  return 0;
}


int
cfg_getlong (PCONFIG pconfig, const char *section, const char *id, int32 *valptr)
{
  int32 value;
  int negative;
  char *np;

  if (cfg_getstring (pconfig, section, id, &np))
    return -1;

  while (isspace (*np))
    np++;
  negative = 0;
  value = 0;
  if (*np == '-')
    {
      negative = 1;
      np++;
    }
  else if (*np == '+')
    np++;
  if (np[0] == '0' && toupper (np[1]) == 'X')
    {
      np += 2;
      while (*np && isxdigit (*np))
	{
	  value *= 16;
	  if (isdigit (*np))
	    value += *np++ - '0';
	  else
	    value += toupper (*np++) - 'A' + 10;
	}
    }
  else
    while (*np && isdigit (*np))
      value = 10 * value + *np++ - '0';

  *valptr = negative ? -value : value;

  return 0;
}


int
cfg_getshort (PCONFIG pconfig, const char *section, const char *id, short *valptr)
{
  int32 value;

  if (cfg_getlong (pconfig, section, id, &value))
    return -1;

  *valptr = (short) value;
  return 0;
}
