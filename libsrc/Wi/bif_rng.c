/*
 *  bif_rng.c
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
 *
 */

#include "sqlnode.h"
#include "sqlbif.h"
#include "arith.h"
#include "xmltree.h"
#include "rdf_core.h"
#include "http.h"		/* For DKS_ESC_XXX constants */
#include "date.h"		/* For DT_TYPE_DATE and the like */
#include "security.h"		/* For sec_check_dba() */
#include "mhash.h"






dk_hash_t id_to_ir;



ir_range_t *
irng_alloc (id_range_t * ir, uint64 start)
{
  int head = (ptrlong) & (((ir_range_t *) 0)->irng_seq);
  ir_range_t *irng = (ir_range_t *) dk_alloc_box_zero (head + sizeof (short) * ir->ir_n_slices + ir->ir_n_slices / 8, DV_BIN);
  irng->irng_start = start;
  irng->irng_n_slices = ir->ir_n_slices;
  return irng;
}


void
irng_hist_add (ir_range_t ** hist, ir_range_t * new_irng)
{
  int inx;
  DO_BOX (ir_range_t *, irng, inx, hist)
  {
    if (!irng)
      {
	hist[inx] = new_irng;
	return;
      }
    if (irng->irng_start == new_irng->irng_start)
      {
	dk_free_box ((caddr_t) new_irng);
	return;
      }
  }
  END_DO_BOX;
  dk_free_box ((caddr_t) hist[0]);
  memmove (&hist[0], &hist[1], box_length (hist) - sizeof (caddr_t));
  hist[BOX_ELEMENTS (hist) - 1] = new_irng;
}


int64
ir_new_id (caddr_t * inst, int64 ir_id, int64 * id_ret)
{
  caddr_t err = NULL;
  ushort *ctr;
  uint64 res;
  uint32 max_seq;
  int slice_given = IR_ID_HAS_SLICE (ir_id), n_hist, i;
  int slid = IR_ID_SLICE (ir_id);
  uint64 start;
  ir_range_t **hist;
  QNCAST (QI, qi, inst);
  uint32 nth_way;
  id_range_t *ir = (id_range_t *) gethash ((void *) IR_ID_ID (ir_id), &id_to_ir);
  lock_trx_t *lt = qi->qi_trx;
  if (!ir)
    sqlr_new_error ("42000", "NORNG", "No range with id %Ld", ir_id);
  nth_way = (lt->lt_main_trx_no ? lt->lt_main_trx_no : lt->lt_trx_no) % ir->ir_n_way_txn;
  *id_ret = ((int64) nth_way) << 32 | IR_ID_ID (ir_id);
  max_seq = (1L << ir->ir_slice_bits) - 1;
  mutex_enter (&ir->ir_mtx);
  hist = ir->ir_ranges[nth_way];
  if (!hist)
    {
      hist = (ir_range_t **) dk_alloc_box_zero (sizeof (caddr_t) * ir->ir_hist_depth, DV_ARRAY_OF_POINTER);
      ir->ir_ranges[nth_way] = hist;
    }
  n_hist = BOX_ELEMENTS (hist);
  for (i = 0; i < n_hist; i++)
    {
      ir_range_t *irng = hist[i];
      if (!irng)
	{
	get_new:
	  mutex_leave (&ir->ir_mtx);
	  if (CL_RUN_LOCAL == cl_run_local_only)
	    {
	      start = sequence_next_inc (ir->ir_seq, OUTSIDE_MAP, ir->ir_chunk);
	      log_sequence_sync (qi->qi_trx, ir->ir_seq, start + ir->ir_chunk);
	    }
	  mutex_enter (&ir->ir_mtx);
	  irng = irng_alloc (ir, start);
	  irng_hist_add (hist, irng);
	}
      if (slice_given)
	{
	  if (BIT_IS_SET (&irng->irng_seq, slid))
	    continue;
	  ctr = (ushort *) (((db_buf_t) & irng->irng_seq) + (irng->irng_n_slices / 8) + 2 * slid);
	  if ((long) ctr - (long) irng > box_length (irng) - 2)
	    {
	      mutex_leave (&ir->ir_mtx);
	      sqlr_new_error ("42000", "SLIRN", "slid out of range in getting range");
	    }
	  res = (*ctr)++;
	  if (max_seq == res)
	    BIT_SET (&irng->irng_seq, slid);
	  res += irng->irng_start + (max_seq + 1) * slid;
	  mutex_leave (&ir->ir_mtx);
	  return res;
	}
      else
	{
	  slid = irng->irng_first_free_slice;
	  while (BIT_IS_SET (&irng->irng_seq, slid))
	    {
	      slid++;
	      if (slid == irng->irng_n_slices)
		goto next_irng;
	    }
	  irng->irng_first_free_slice = slid;
	  ctr = (ushort *) (((db_buf_t) & irng->irng_seq) + (irng->irng_n_slices / 8) + 2 * slid);
	  res = (*ctr)++;
	  if (max_seq == res)
	    BIT_SET (&irng->irng_seq, slid);
	  res += irng->irng_start + (slid * (1 + max_seq));
	  mutex_leave (&ir->ir_mtx);
	  return res;
	}
    next_irng:;
    }
  goto get_new;
  mutex_leave (&ir->ir_mtx);
  GPF_T1 ("ir not supposed to come here");
  return 0;
}


void
ir_set_data (int64 id, caddr_t data)
{
  id_range_t *ir = (id_range_t *) gethash ((void *) id, &id_to_ir);
  if (!ir)
    {
      NEW_VARZ (id_range_t, ir2);
      return;
      ir = ir2;
      sethash ((void *) id, &id_to_ir, (void *) ir);
    }
  dk_free_tree (ir->ir_ranges);
  ir->ir_ranges = box_copy_tree (data);
}


void
ir_replay (int64 id, int64 n)
{
  ir_range_t *irng;
  unsigned short *ctr;
  unsigned short nctr;
  slice_id_t slid;
  uint64 start;
  int i;
  uint32 max_seq;
  int nth_way;
  ir_range_t **hist;
  id_range_t *ir = (id_range_t *) gethash ((void *) IR_ID_ID (id), &id_to_ir);
  if (!ir)
    return;
  max_seq = (1L << ir->ir_slice_bits) - 1;
  nth_way = IR_ID_WAY (id);
  if (nth_way >= BOX_ELEMENTS (ir->ir_ranges))
    return;
  start = ir->ir_start + ((id / ir->ir_start) / ir->ir_chunk) * ir->ir_chunk;
  hist = ir->ir_ranges[nth_way];
  if (!hist)
    ir->ir_ranges[nth_way] = hist = (ir_range_t **) dk_alloc_box_zero (ir->ir_hist_depth * sizeof (caddr_t), DV_ARRAY_OF_POINTER);
  for (i = 0; i < BOX_ELEMENTS (hist); i++)
    {
      if (!hist[i])
	break;
      else
	irng = hist[i];
      if (irng->irng_start != start)
	continue;
      slid = (id - irng->irng_start) / (max_seq + 1);
      ctr = (ushort *) (((db_buf_t) & irng->irng_seq) + (irng->irng_n_slices / 8) + 2 * slid);
      nctr = id - start - slid * (max_seq + 1);
      if (*ctr < nctr)
	*ctr = nctr;
      if (max_seq == nctr)
	{
	  BIT_SET (&irng->irng_seq, slid);
	}
      return;
    }
  irng = irng_alloc (ir, start);
  irng_hist_add (hist, irng);
  slid = (id - irng->irng_start) / (max_seq + 1);
  ctr = (ushort *) (((db_buf_t) & irng->irng_seq) + (irng->irng_n_slices / 8) + 2 * slid);
  nctr = id - start - slid * (max_seq + 1);
  if (*ctr < nctr)
    *ctr = nctr;
  if (max_seq == nctr)
    {
      BIT_SET (&irng->irng_seq, slid);
    }
}


caddr_t
bif_ir_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  caddr_t ir_name = bif_string_arg (qst, args, 0, "ir_def");
  uint64 id = bif_long_arg (qst, args, 1, "ir_def");
  caddr_t ir_cluster = bif_string_arg (qst, args, 2, "ir_def");
  int ir_mode = bif_long_arg (qst, args, 3, "ir_def");
  unsigned int ir_n_way_txn = bif_long_arg (qst, args, 4, "ir_def");
  int ir_allocator_host = bif_long_arg (qst, args, 5, "ir_def");
  caddr_t ir_main_seq = bif_string_arg (qst, args, 6, "ir_def");
  unsigned int ir_slice_bits = bif_long_arg (qst, args, 7, "ir_def");
  uint64 ir_chunk = bif_long_arg (qst, args, 8, "ir_def");
  uint64 ir_max = bif_long_arg (qst, args, 9, "ir_def");
  unsigned int ir_hist_depth = bif_long_arg (qst, args, 10, "ir_def");
  int ir_n_slices = bif_long_arg (qst, args, 11, "ir_def");
  uint64 ir_start = bif_long_arg (qst, args, 12, "ir_def");
  caddr_t *ir_options = bif_array_of_pointer_arg (qst, args, 13, "ir_def");
  id_range_t *ir = (id_range_t *) gethash ((void *) id, &id_to_ir);
  if (!ir)
    {
      NEW_VARZ (id_range_t, ir2);
      sethash ((void *) id, &id_to_ir, (void *) ir2);
      ir = ir2;
      dk_mutex_init (&ir->ir_mtx, MUTEX_TYPE_SHORT);
    }
  ir->ir_name = box_copy (ir_name);
  ir->ir_id = id;
  ir->ir_clm = clm_all;
  ir->ir_mode = ir_mode;
  ir->ir_allocator_host = ir_allocator_host;
  ir->ir_slice_bits = ir_slice_bits <= 16 ? ir_slice_bits : 16;
  ir->ir_chunk = ir_chunk;
  ir->ir_start = ir_start;
  ir->ir_seq = box_copy (ir_main_seq);
  ir->ir_max = ir_max ? ir_max : 0xffffffffffffffff;
  ir->ir_hist_depth = ir_hist_depth <= 10 ? ir_hist_depth : 10;
  ir->ir_n_way_txn = ir_n_way_txn <= 19 ? ir_n_way_txn : 19;
  ir->ir_n_slices = ir_n_slices;
  if (!ir->ir_ranges || BOX_ELEMENTS (ir->ir_ranges) < ir->ir_n_way_txn)
    {
      ir_range_t ***rngs = (ir_range_t ***) dk_alloc_box_zero (sizeof (caddr_t) * ir->ir_n_way_txn, DV_ARRAY_OF_POINTER);
      if (ir->ir_ranges)
	memcpy (rngs, ir->ir_ranges, MIN (box_length (ir->ir_ranges), box_length (rngs)));
      ir->ir_ranges = rngs;
    }
  return NULL;
}


caddr_t
bif_ir_next (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  uint64 id = bif_long_arg (qst, args, 0, "ir_next");
  QNCAST (QI, qi, qst);
  int64 log = 0;
  uint64 res = ir_new_id (qst, id, &log);
  if (REPL_NO_LOG != qi->qi_trx->lt_replicate)
    {
      caddr_t idb = box_num (log);
      log_sequence (qi->qi_trx, idb, res + 1);
      dk_free_box (idb);
    }
  return box_num (res);
}

id_hash_t *iri_pattern_to_ip;
dk_hash_t pref_hash_to_ip;
iri_pattern_t *iri_range_to_ip;

#define is_digit(c) ((c) >= '0'&& (c) <= '9')

uint64
ip_multiply (iri_pattern_t * ip, int nth)
{
  int n_fields = box_length (ip->ip_fields) / sizeof (ip_field_t), i;
  uint64 r = 1;
  for (i = nth + 1; i < n_fields; i++)
    {
      ip_field_t *irif = &ip->ip_fields[i];
      if (!irif->irif_string && irif->irif_high)
	r *= irif->irif_high - irif->irif_low;
    }
  return r;
}



caddr_t
bif_iri_pattern_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  caddr_t pattern = bif_string_arg (qst, args, 0, "iri_pattern_def");
  iri_id_t start = bif_iri_id_arg (qst, args, 1, "iri_pattern_def");
  caddr_t *rng = bif_array_of_pointer_arg (qst, args, 2, "iri_pattern_def");
  ptrlong cset = bif_long_arg (qst, args, 3, "iri_pattern_def");
  ptrlong int_range = bif_long_arg (qst, args, 4, "iri_pattern_def");
  ptrlong exc_range = bif_long_arg (qst, args, 5, "iri_pattern_def");
  int fill = 0;
  if (!start)
    {
      id_hash_remove (iri_pattern_to_ip, (caddr_t) & pattern);
      return NULL;
    }
  {
    uint64 h = 1;
    int len, inx, first_digit = -1, nth_n = 0, prev_start = 0;
    ip_field_t *part;
    dk_set_t parts = NULL;
    caddr_t *r1;
    NEW_VARZ (iri_pattern_t, ip);
    ip->ip_pattern = box_copy (pattern);
    id_hash_set (iri_pattern_to_ip, (caddr_t) & ip->ip_pattern, (caddr_t) & ip);
    ip->ip_start = start;
    if (exc_range)
      ip->ip_exc_range = (id_range_t *) gethash ((void *) exc_range, &id_to_ir);
    if (int_range)
      ip->ip_int_range = (id_range_t *) gethash ((void *) int_range, &id_to_ir);
    ip->ip_cset = cset;
    len = strlen (pattern);
    for (inx = 0; inx < len; inx++)
      {
	if (-1 == first_digit && is_digit (pattern[inx]))
	  first_digit = inx;
	if ('%' == pattern[inx] && '%' == pattern[inx + 1])
	  {
	    if (-1 == first_digit)
	      first_digit = inx;
	    part = (ip_field_t *) dk_alloc (sizeof (ip_field_t));
	    memzero (part, sizeof (ip_field_t));
	    part->irif_string = box_n_chars ((db_buf_t) & pattern[prev_start], inx - prev_start);
	    dk_set_push (&parts, (void *) part);
	    if (BOX_ELEMENTS (rng) <= nth_n)
	      sqlr_new_error ("42000", "IRIPF", "Not enough ranges for iri pattern");
	    r1 = ((caddr_t **) rng)[nth_n];
	    if (BOX_ELEMENTS (r1) < 2 || DV_ARRAY_OF_POINTER != DV_TYPE_OF (r1))
	      sqlr_new_error ("42000", "IRINR", "Not a valid numeric range in iri pattern");
	    part = (ip_field_t *) dk_alloc (sizeof (ip_field_t));
	    memzero (part, sizeof (ip_field_t));
	    part->irif_low = unbox_iri_int64 (r1[0]);
	    part->irif_high = unbox_iri_int64 (r1[1]);
	    dk_set_push (&parts, (void *) part);
	    nth_n++;
	    inx++;
	    prev_start = inx + 1;
	    continue;
	  }
      }
    if (prev_start != inx)
      {
	NEW_VARZ (ip_field_t, irif);
	irif->irif_string = box_n_chars ((db_buf_t) pattern, len - prev_start);
	dk_set_push (&parts, (void *) irif);
      }
    parts = dk_set_nreverse (parts);
    ip->ip_fields = (ip_field_t *) dk_alloc_box (sizeof (ip_field_t) * dk_set_length (parts), DV_BIN);
    DO_SET (ip_field_t *, irif, &parts)
    {
      ip->ip_fields[fill++] = *irif;
      dk_free (irif, sizeof (ip_field_t));
    }
    END_DO_SET ();
    h = 1;
    MHASH_VAR (h, ip->ip_fields[0].irif_string, first_digit);
    ip->ip_pref_hash = h;
    ip->ip_end = ip->ip_start + ip_multiply (ip, -1);
    return NULL;
  }
}

int
ip_r_cmp (const void *s1, const void *s2)
{
  QNCAST (iri_pattern_t, i1, s1);
  QNCAST (iri_pattern_t, i2, s2);
  return i1->ip_start < i2->ip_start ? -1 : 1;
}

caddr_t
bif_iri_pattern_changed (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  int n_patterns = iri_pattern_to_ip->ht_count, inx;
  iri_pattern_t *arr = dk_alloc_box (sizeof (iri_pattern_t) * n_patterns, DV_BIN);
  int fill = 0;
  DO_IDHASH (caddr_t, k, iri_pattern_t *, ip, iri_pattern_to_ip)
  {
    arr[fill++] = *ip;
  }
  END_DO_IDHASH;
  qsort (arr, fill, sizeof (iri_pattern_t), ip_r_cmp);
  clrhash (&pref_hash_to_ip);
  for (inx = 0; inx < fill; inx++)
    {
      dk_set_t list = (dk_set_t) gethash ((void *) arr[inx].ip_pref_hash, &pref_hash_to_ip);
      list = CONS (&arr[inx], list);
      sethash ((void *) arr[inx].ip_pref_hash, &pref_hash_to_ip, (void *) list);
    }
  iri_range_to_ip = arr;
  return NULL;
}

caddr_t
bif_str_bin_iri (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  caddr_t str = bif_string_arg (qst, args, 0, "str_bin_iri");
  return box_iri_id (INT64_REF_NA (str));
}


caddr_t
bif_str_num_fields (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  caddr_t *ret;
  char pattern[1000];
  caddr_t str = bif_string_arg (qst, args, 0, "str_num_fields");
  int min_digits = bif_long_arg (qst, args, 1, "str_num_fields");
  int n_fields = bif_long_arg (qst, args, 2, "str_num_fields");
  int len = box_length (str) - 1, pattern_fill = 0;
  uint64 numbers[10];
  int first_digit = -1, inx, in_digits = 0, num_fill = 0;
  for (inx = 0; inx < len; inx++)
    {
      if (is_digit (str[inx]))
	{
	  if (!in_digits)
	    {
	      in_digits = 1;
	      first_digit = inx;
	      num_fill++;
	      numbers[num_fill] = 0;
	    }
	  numbers[num_fill] = numbers[num_fill] * 10 + str[inx] - '0';
	}
      else
	{
	  if (in_digits)
	    {
	      if (inx - first_digit < min_digits || inx - first_digit > 15)
		{
		  memcpy (pattern + pattern_fill, str + first_digit, inx - first_digit);
		  pattern_fill += inx - first_digit;
		  in_digits = 0;
		  num_fill--;
		  continue;
		}
	      else
		{
		  pattern[pattern_fill++] = '%';
		  pattern[pattern_fill++] = '%';
		  pattern[pattern_fill++] = str[inx];
		  in_digits = 0;
		}
	    }
	  else
	    {
	      pattern[pattern_fill++] = str[inx];
	    }
	}
    }
  if (in_digits)
    {
      pattern[pattern_fill++] = '%';
      pattern[pattern_fill++] = '%';
    }
  ret = (caddr_t *) dk_alloc_box_zero (sizeof (caddr_t) * (n_fields + 1), DV_ARRAY_OF_POINTER);
  ret[0] = box_n_chars ((db_buf_t) pattern, pattern_fill);
  for (inx = 0; inx < n_fields; inx++)
    ret[inx + 1] = inx <= num_fill ? box_num (numbers[inx]) : dk_alloc_box (0, DV_DB_NULL);
  return (caddr_t) ret;
}

iri_id_t
iri_from_string (char *str, int len, iri_pattern_t ** ip_ret)
{
  /* if the string matches a numeric iri pattern, return the iri.  If matches but overflows, ret 0 but set ip_ret */
  iri_id_t id = 0;
  int overflow = 0;
  dk_set_t list;
  int inx, first_digit = -1;
  uint64 h = 1;
  for (inx = 0; inx < len; inx++)
    {
      if (is_digit (str[inx]))
	{
	  first_digit = inx;
	  break;
	}
    }
  if (-1 == first_digit)
    return 0;
  MHASH_VAR (h, str, first_digit);
  list = (dk_set_t) gethash ((void *) h, &pref_hash_to_ip);
  if (!list)
    return 0;
  DO_SET (iri_pattern_t *, ip, &list)
  {
    int nth, n_fields = box_length (ip->ip_fields) / sizeof (ip_field_t);
    int point = 0;
    for (nth = 0; nth < n_fields; nth++)
      {
	ip_field_t *irif = &ip->ip_fields[nth];
	if (irif->irif_string)
	  {
	    int s_len = box_length (irif->irif_string) - 1;
	    if (s_len + point > len)
	      goto no;
	    memcmp_8 (str + point, irif->irif_string, s_len, no);
	    point += s_len;
	    continue;
	  }
	else
	  {
	    int i;
	    int64 num = 0;
	    for (i = 0; point + i < len && is_digit (str[point + i]); i++)
	      {
		num = num * 10 + str[point + i] - '0';
	      }
	    if (i > 15)
	      goto no;
	    if (!irif->irif_high)
	      ;
	    else if (num < irif->irif_low || num > irif->irif_high)
	      overflow = 1;
	    else
	      id += (num - irif->irif_low) * ip_multiply (ip, nth);
	    point += i;
	  }
      }
    if (point == len)
      {
	*ip_ret = ip;
	return overflow ? 0 : id + ip->ip_start;
      }
  no:;
  }
  END_DO_SET ();
  return 0;
}


caddr_t
iri_to_string (iri_id_t iri, iri_pattern_t ** ip_ret)
{
  /* if the range is a numeric iri range, make the string from that */
  char res[1000];
  caddr_t box;
  int below;
  int at_or_above = 0;
  int guess, nth, n_fields, fill = 0;
  iri_pattern_t *ip;
  if (!iri_range_to_ip)
    return NULL;
  n_fields = below = box_length (iri_range_to_ip) / sizeof (iri_pattern_t);
  if (!n_fields)
    return NULL;
  guess = (below - at_or_above) / 2;
  for (;;)
    {
      ip = &iri_range_to_ip[guess];
      if (iri >= ip->ip_start && iri < ip->ip_end)
	goto found;
      if (below - at_or_above <= 1)
	return NULL;
      if (ip->ip_start < iri)
	at_or_above = guess + 1;
      else
	below = guess;
      guess = at_or_above + ((below - at_or_above) / 2);
    }
found:
  if (ip_ret)
    *ip_ret = ip;
  n_fields = box_length (ip->ip_fields) / sizeof (ip_field_t);
  iri -= ip->ip_start;
  fill = 0;
  for (nth = 0; nth < n_fields; nth++)
    {
      ip_field_t *irif = &ip->ip_fields[nth];
      if (irif->irif_string)
	{
	  int l = box_length (irif->irif_string) - 1;
	  memcpy (&res[fill], irif->irif_string, l);
	  fill += l;
	}
      else if (irif->irif_high)
	{
	  uint64 mpy = ip_multiply (ip, nth);
	  iri_id_t f = iri / mpy;
	  sprintf (&res[fill], "%Lu", f + irif->irif_low);
	  fill += strlen (&res[fill]);
	  iri = iri % mpy;
	}
    }
  box = box_dv_short_string (res);
  box_flags (box) = BF_IRI;
  return box;
}


caddr_t
bif_iri_ip_to_string (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  iri_id_t iri = bif_iri_id_arg (qst, args, 0, "iri_ip_to_string");
  iri_pattern_t *ip = NULL;
  caddr_t str = iri_to_string (iri, &ip);
  if (!str)
    return 0;
  return str;
}


void
bif_rng_init ()
{
  hash_table_init (&id_to_ir, 211);
  id_to_ir.ht_rehash_threshold = 2;
  bif_define ("ir_def", bif_ir_def);
  bif_define_typed ("ir_next", bif_ir_next, &bt_integer);
  bif_define ("iri_pattern_def", bif_iri_pattern_def);
  bif_define ("iri_pattern_changed", bif_iri_pattern_changed);
  bif_define_typed ("str_num_fields", bif_str_num_fields, &bt_any_box);
  bif_define_typed ("str_bin_iri", bif_str_bin_iri, &bt_iri_id);
  bif_define_typed ("iri_ip_to_string", bif_iri_ip_to_string, &bt_any);
  ddl_ensure_table ("DB.DBA.SYS_ID_RANGE",
      "create table DB.DBA.SYS_ID_RANGE (ir_name varchar primary key, ir_id bigint, ir_cluster varchar, ir_mode int, ir_n_way_txn int, ir_allocator_host int, ir_main_seq varchar, ir_slice_bits int, ir_chunk bigint, ir_max bigint, ir_hist_depth int, ir_n_slices int, ir_start bigint, ir_options any)\n"
      " alter index sys_id_range on sys_id_range  partition cluster REPLICATED\n");
  ddl_ensure_table ("do this always",
      "select ir_def (ir_name, ir_id, ir_cluster, ir_mode , ir_n_way_txn, ir_allocator_host, ir_main_seq, ir_slice_bits, ir_chunk, ir_max, "
      "ir_hist_depth, ir_n_slices, ir_start, ir_options) from sys_id_range");
  iri_pattern_to_ip = id_str_hash_create (183);
  hash_table_init (&pref_hash_to_ip, 33);
}
