/*
 *  Dkhash64.h
 *
 *  $Id$
 *
 *  int64 hashtable for 32 bit platforms
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
 *
 */

#ifndef _DKHASH64_H
#define _DKHASH64_H

#define dk_hash_64_t 			dk_hash_t

#define hash_table_allocate_64(sz)  	hash_table_allocate (sz)

#define sethash_64(k, ht, v) \
  sethash ((void*)(ptrlong)k, ht, (void*)v)


#define gethash_64(res, k, ht) \
  res = (int64)gethash ((void*)(ptrlong)k, ht)



#define remhash_64(k, ht) \
  remhash ((void*)(ptrlong)k, ht)

#define remhash_64_f(k, ht, flag)			\
  flag = remhash ((void*)(ptrlong)k, ht)

#define hash_table_free_64(ht) \
  hash_table_free (ht)

#define dk_hash_64_iterator_t dk_hash_iterator_t
#define dk_hash_64_iterator dk_hash_iterator
#define dk_hash_64_hit_next(ht,kptr,vptr) dk_hit_next ((ht), (caddr_t *)((int64 *)(kptr)), (caddr_t *)((int64 *)(vptr)))






#endif
