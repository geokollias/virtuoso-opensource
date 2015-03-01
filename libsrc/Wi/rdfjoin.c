/*
 *  rdfjoin.c
 *
 *  $Id$
 *
 *  SQL query execution
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2013 OpenLink Software
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
#include "sqlfn.h"
#include "sqlcomp.h"
#include "sqlopcod.h"
#include "security.h"
#include "sqlbif.h"
#include "sqltype.h"
#include "libutil.h"
#include "aqueue.h"
#include "mhash.h"
#include "date.h"
#include "arith.h"


void
itc_set_cset_bits (it_cursor_t * itc, int row, int val)
{
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  int n_bytes = csm->csm_cset_bit_bytes;
  db_buf_t cset_bits = itc->itc_cset_bits;
  memset (cset_bits + n_bytes * row, val ? 0xff : 0, n_bytes);
}

void
itc_null_row (it_cursor_t * itc)
{
  int inx;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm;
  int nth_bit = csm->csm_bit;
  caddr_t *inst = itc->itc_out_state;
  v_out_map_t *om = itc->itc_v_out_map;
  int n_out = om ? box_length (om) / sizeof (v_out_map_t) : 0;
  for (inx = 0; inx < n_out; inx++)
    dc_append_null (QST_BOX (data_col_t *, inst, om[inx].om_ssl->ssl_index));
}


int
itc_cset_unmatched_by_s (it_cursor_t * itc)
{
  /* some ranges have no matchwhen looking in cset by s */
  int col_first = itc->itc_col_first_set;
  caddr_t *inst = itc->itc_out_state;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  iri_id_t *reqd_ps = csm->csm_reqd_ps;
  int n_reqd = BOX_ELEMENTS (reqd_ps), nth_p;
  int batch_sz = QST_INT (inst, ts->src_gen.src_batch_size);
  data_col_t *s_dc = ITC_P_VEC (itc, 0);
  int inx;
  cset_t *cset = itc->itc_insert_key->key_table->tb_cset;
  cset_p_t *csp[100];
  int last_unmatched = itc->itc_last_cset_unmatched_set;
  for (inx = 0; inx < n_reqd; inx++)
    {
      csp[inx] = gethash ((void *) reqd_ps[inx], &cset->cset_p);
      if (!csp[inx] || !csp[inx]->csetp_n_bloom)
	return 0;
    }
  for (inx = last_unmatched; inx < itc->itc_range_fill; inx++)
    {
      if (itc->itc_ranges[inx].r_first == itc->itc_ranges[inx].r_end)
	{
	  iri_id_t s = ((iri_id_t *) s_dc->dc_values)[itc->itc_param_order[inx + col_first]];
	  uint64 h = 1, mask;
	  MHASH_STEP_1 (h, s);
	  mask = BF_MASK (h);
	  for (nth_p = 0; nth_p < n_reqd; nth_p++)
	    {
	      uint64 *bf;
	      uint32 n_bf;
	      csetp_bloom (csp[inx], inst, &bf, &n_bf);
	      uint64 w = bf[BF_WORD (h, n_bf)];
	      if (mask != (mask & w))
		goto next_s;
	    }
	  itc_null_row (itc);
	  itc_set_cset_bits (itc, itc->itc_n_results, 0);
	  itc->itc_n_results++;
	  qn_result ((data_source_t *) ts, itc->itc_out_state, col_first + inx);
	  if (batch_sz == itc->itc_n_results)
	    {
	      itc->itc_last_cset_unmatched_set = inx + 1;
	      return 1;
	    }
	next_s:;
	}
    }
  itc->itc_last_cset_unmatched_set = 0;
  return 0;
}


void
itc_init_set_card (it_cursor_t * itc)
{
  /* make a bit array with 2 bits per set of input */
  table_source_t *ts = itc->itc_ks->ks_ts;
  int bytes;
  caddr_t *inst = itc->itc_out_state;
  db_buf_t bits;
  bytes = ALIGN_8 (2 * itc->itc_n_sets) / 8;
  qi_ensure_array (itc->itc_out_state, &inst[ts->ts_set_card_bits], bytes, 0);
  bits = QST_BOX (db_buf_t, inst, ts->ts_set_card_bits);
  memzero (bits, bytes);
}

void
itc_cset_record_set_card (it_cursor_t * itc)
{
  int *sets;
  int inx;
  caddr_t *inst = itc->itc_out_state;
  int out_fill;
  db_buf_t bits;
  table_source_t *ts;
  if (itc->itc_ks)
    return;
  ts = itc->itc_ks->ks_ts;
  if (!ts->ts_set_card_bits)
    return;
  bits = QST_BOX (db_buf_t, inst, ts->ts_set_card_bits);
  out_fill = QST_INT (inst, ts->src_gen.src_out_fill);
  sets = QST_BOX (int *, inst, ts->src_gen.src_sets);
  for (inx = 0; inx < out_fill; inx++)
    {
      int set = sets[inx];
      if (BIT_IS_SET (bits, set * 2))
	{
	  BIT_SET (bits, set * 2 + 1);
	}
      else
	BIT_SET (bits, set * 2);
    }
}

void
qi_ensure_array (caddr_t * inst, caddr_t * place, int n_bytes, int copy_content)
{
  QNCAST (QI, qi, inst);
  caddr_t box = *(caddr_t *) place;
  if (!box || box_length (box) < n_bytes)
    {
      caddr_t new_b = mp_alloc_box (qi->qi_mp, n_bytes + 1000, DV_BIN);
      if (copy_content)
	memcpy_16 (new_b, box, box_length (box));
      *place = new_b;
    }
}


void
itc_init_exc_bits (it_cursor_t * itc, buffer_desc_t * buf)
{
  /* set all the col bits to 1.  Will be set to 0 where col pred does not match and exception is possible */
  caddr_t *inst = itc->itc_out_state;
  caddr_t arr;
  data_col_t *s_dc = itc->itc_s_dc;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  int last, bytes, inx, bit_bytes = csm->csm_cset_bit_bytes;
  if (s_dc)
    s_dc->dc_n_values = 0;
  if (itc->itc_n_matches)
    last = itc->itc_matches[itc->itc_n_matches - 1];
  else
    last = itc->itc_ranges[itc->itc_range_fill - 1].r_end;
  if (COL_NO_ROW == last)
    last = itc_rows_in_seg (itc, buf);
  bytes = (last + 1) * bit_bytes;
  qi_ensure_array (itc->itc_out_state, &inst[csm->csm_exc_tmp_bits], last * csm->csm_cset_bit_bytes + 8, 0);
  arr = QST_BOX (caddr_t, inst, csm->csm_exc_tmp_bits);
  itc->itc_cset_bits = arr;
  if (itc->itc_n_matches)
    {
      if (1 == bit_bytes)
	{
	  for (inx = 0; inx < itc->itc_n_matches; inx++)
	    arr[itc->itc_matches[inx]] = 255;
	}
      else
	{
	  for (inx = 0; inx < itc->itc_n_matches; inx++)
	    memset (arr + inx * bit_bytes, 255, bit_bytes);
	}
    }
  else
    {
      row_range_t rng = itc->itc_ranges[itc->itc_set - itc->itc_col_first_set];
      if (COL_NO_ROW == rng.r_end)
	rng.r_end = itc_rows_in_seg (itc, buf);
      memset (arr + rng.r_first * bit_bytes, 255, (rng.r_end - rng.r_first) * bit_bytes);
    }
}


void
itc_ensure_array (it_cursor_t * itc, caddr_t * place, int n_bytes, int copy_content)
{
  caddr_t box = *(caddr_t *) place;
  if (!box || box_length (box) < n_bytes)
    {
      caddr_t new_b = itc_alloc_box (itc, n_bytes + 1000, DV_BIN);
      if (copy_content)
	memcpy_16 (new_b, box, box_length (box));
      itc_free_box (itc, box);
      *place = new_b;
    }
}


void
itc_seg_read_s (it_cursor_t * itc, buffer_desc_t * buf, int prev_matches)
{
  col_pos_t cpo;
  row_no_t *save_matches = itc->itc_matches;
  int save_n_matches = itc->itc_n_matches, target, row, nth_page, r;
  int end;
  row_no_t *matches = &itc->itc_matches[itc->itc_n_matches];
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  v_out_map_t *om = csm->csm_s_om;
  col_data_ref_t *cr = itc->itc_col_refs[csm->csm_s_col_pos];
  memzero (&cpo, sizeof (cpo));
  if (!cr->cr_is_valid)
    itc_fetch_col (itc, buf, &om->om_cl, 0, COL_NO_ROW);
  itc->itc_matches = matches;
  itc->itc_n_matches = prev_matches;
  end = itc->itc_matches[itc->itc_n_matches - 1] + 1;
  itc_ensure_array (itc, (caddr_t *) & itc->itc_s_dc_row, prev_matches * sizeof (row_no_t), 1);
  memcpy_16 (itc->itc_s_dc_row, matches, prev_matches * sizeof (row_no_t));
  cpo.cpo_clk_inx = 0;
  cpo.cpo_dc = itc->itc_s_dc;
  cpo.cpo_cl = &om->om_cl;
  cpo.cpo_ce_op = ce_op[(om->om_ce_op << 1) + (itc->itc_n_matches > 0)];
  DC_CHECK_LEN (cpo.cpo_dc, prev_matches - 1);
  target = matches[0];
  row = 0;
  itc->itc_match_in = 0;
  for (nth_page = 0; nth_page < cr->cr_n_pages; nth_page++)
    {
      page_map_t *pm = cr->cr_pages[nth_page].cp_map;
      int rows_on_page = 0;
      for (r = 0 == nth_page ? cr->cr_first_ce * 2 : 0; r < pm->pm_count; r += 2)
	{
	  int is_last = 0, end2, r2, ces_on_page, prev_fill;
	  if (row + pm->pm_entries[r + 1] <= target)
	    {
	      row += pm->pm_entries[r + 1];
	      continue;
	    }
	  if (row >= cpo.cpo_range->r_end)
	    goto done;
	  ces_on_page = nth_page == cr->cr_n_pages - 1 ? cr->cr_limit_ce : pm->pm_count;
	  cpo.cpo_pm = pm;
	  cpo.cpo_pm_pos = r;
	  for (r2 = r; r2 < ces_on_page; r2 += 2)
	    rows_on_page += pm->pm_entries[r2 + 1];
	  end2 = MIN (end, row + rows_on_page);
	  if (end2 >= end)
	    is_last = 1;
	  cpo.cpo_ce_row_no = row;
	  cpo.cpo_string = cr->cr_pages[nth_page].cp_string + pm->pm_entries[r];
	  cpo.cpo_bytes = pm->pm_filled_to - pm->pm_entries[r];
	  prev_fill = cpo.cpo_dc->dc_n_values;
	  target = cs_decode (&cpo, target, end2);
	  if (is_last || target >= end)
	    goto done;
	  break;
	}
      row += rows_on_page;
    }
done:;
  if (cpo.cpo_dc->dc_n_values > cpo.cpo_dc->dc_n_places)
    GPF_T1 ("filled dc past end");
  itc->itc_matches = save_matches;
  itc->itc_n_matches = save_n_matches;
}


iri_id_t
itc_seg_row_s (it_cursor_t * itc, buffer_desc_t * buf, row_no_t row, int prev_n_matches, int *last_place)
{
  /* given a row no from matches, give the s */
  caddr_t *inst = itc->itc_out_state;
  data_col_t *s_dc = itc->itc_s_dc;
  row_no_t *s_row = itc->itc_s_dc_row;
  int n_rows, inx;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  if (!s_dc)
    {
      s_dc = itc->itc_s_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_s->ssl_index);
      s_row = itc->itc_s_dc_row = QST_BOX (row_no_t *, inst, csm->csm_exc_s_row);;
    }
  if (!s_dc->dc_n_values)
    {
      itc_seg_read_s (itc, buf, prev_n_matches);
    }
  s_row = itc->itc_s_dc_row;
  n_rows = s_dc->dc_n_values;
  for (inx = *last_place; inx < n_rows; inx++)
    {
      if (row == s_row[inx])
	{
	  *last_place = inx;
	  return ((iri_id_t *) s_dc->dc_values)[inx];
	}
    }
  GPF_T1 ("looking for a cset s row that is not in matches");
  return 0;
}

void
itc_cset_except (it_cursor_t * itc, buffer_desc_t * buf, int prev_matches, int rows_in_seg, col_pos_t * cpo)
{
  /* itc_matches has the pre-filter matches starting at itc_n_matches.  If prev_matches is 0, then the prev is the itc_ranges for the set. *
   * matches is after filtering on col.  If a row was in prev and not in matches, put it back in matches if s can have exception. */
  caddr_t *inst = itc->itc_out_state;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  search_spec_t *sp = itc->itc_col_spec;
  row_no_t prev_row;
  int nth_bit = sp->sp_ordinal;
  cset_p_t *csetp = sp->sp_col->col_csetp;
  row_no_t *pre, *post;
  uint64 *bf;
  uint32 n_bf;
  int inx, n_post, fill = 0, nth_match = 0, point = 0;
  int n_matches = itc->itc_match_out;
  if (!csetp)
    return;
  csetp_bloom (csetp, inst, &bf, &n_bf);
  if (!bf)
    return;
  rows_in_seg = itc->itc_rows_in_seg;
  if (!itc->itc_n_matches)
    {
      int end = MIN (rows_in_seg, cpo->cpo_range->r_end);
      if (fill == end - cpo->cpo_range->r_first)
	return;
      post = itc->itc_matches;
      n_post = itc->itc_match_out;
      pre = &itc->itc_matches[n_post];
      for (prev_row = cpo->cpo_range->r_first; prev_row < end; prev_row++)
	{
	  if (nth_match >= n_matches || post[nth_match] != prev_row)
	    {
	      iri_id_t s = itc_seg_row_s (itc, buf, prev_row, prev_matches, &point);
	      uint64 mask, w, h = 1;
	      MHASH_STEP_1 (h, s);
	      mask = BF_MASK (h);
	      w = bf[BF_WORD (h, n_bf)];
	      if (mask == (w & mask))
		post[fill++] = prev_row;
	      else
		{
		  int off = prev_row * csm->csm_cset_bit_bytes;
		  BIT_CLR (itc->itc_cset_bits + off, nth_bit);
		  if (sp->sp_is_cset_opt)
		    pre[fill++] = prev_row;
		}
	    }
	  else
	    {
	      pre[fill++] = prev_row;
	      nth_match++;
	    }
	}
      memmove_16 (&itc->itc_matches, &itc->itc_matches[n_matches], fill * sizeof (row_no_t));
      itc->itc_match_out = fill;
      return;
    }
  pre = &itc->itc_matches[itc->itc_n_matches];
  post = itc->itc_matches;
  n_post = itc->itc_match_out;
  for (inx = 0; inx < prev_matches; inx++)
    {
      prev_row = pre[inx];
      if (nth_match >= n_matches || post[nth_match] != prev_row)
	{
	  iri_id_t s = itc_seg_row_s (itc, buf, prev_row, prev_matches, &point);
	  uint64 mask, w, h = 1;
	  MHASH_STEP_1 (h, s);
	  mask = BF_MASK (h);
	  w = bf[BF_WORD (h, n_bf)];
	  if (mask == (w & mask))
	    post[fill++] = prev_row;
	  else
	    {
	      int off = prev_row * csm->csm_cset_bit_bytes;
	      BIT_CLR (itc->itc_cset_bits + off, nth_bit);
	      if (sp->sp_is_cset_opt)
		pre[fill++] = prev_row;
	    }
	}
      else
	{
	  pre[fill++] = prev_row;
	  nth_match++;
	}
    }
  memmove_16 (post, pre, fill * sizeof (row_no_t));
  itc->itc_match_out = fill;
}

void
itc_cset_opt_nulls (it_cursor_t * itc)
{
  /* clear the cset bit for optional cset cols that have a null */
  data_col_t *dc;
  state_slot_t *bits_ssl;
  data_col_t *bits_dc;
  uint64 *bits, clr_mask;
  int this_bit, set;
  key_source_t *ks = itc->itc_ks;
  table_source_t *ts = ks->ks_ts;
  caddr_t *inst = itc->itc_out_state;
  v_out_map_t *om = ks->ks_v_out_map;
  int n_out = om ? box_length (om) / sizeof (v_out_map_t) : 0;
  int n_row_specs = itc->itc_n_row_specs, out_inx, sp_inx;
  for (out_inx = 0; out_inx < n_out; out_inx++)
    {
      oid_t col_id = om[out_inx].om_cl.cl_col_id;
      for (sp_inx = 0; sp_inx < n_row_specs; sp_inx++)
	{
	  oid_t sp_id = itc->itc_sp_stat[sp_inx].spst_sp->sp_cl.cl_col_id;
	  if (col_id == sp_id)
	    goto next_out;
	}
      if (OM_NO_CSET == om[out_inx].om_cset_bit)
	continue;
      this_bit = om[out_inx].om_cset_bit % 64;
      clr_mask = ~(1L << this_bit);
      bits_ssl = ts->ts_csm->csm_exc_bits_out[om[out_inx].om_cset_bit / 64];
      bits_dc = QST_BOX (data_col_t *, inst, bits_ssl->ssl_index);
      DC_CHECK_LEN (bits_dc, itc->itc_n_results - 1);
      bits = (uint64 *) bits_dc->dc_values;
      dc = QST_BOX (data_col_t *, inst, om[out_inx].om_ssl->ssl_index);
      if (dc->dc_any_null)
	{
	  if (DV_ANY == dc->dc_sqt.sqt_dtp)
	    {
	      for (set = 0; set < dc->dc_n_values; set++)
		{
		  db_buf_t dv = ((db_buf_t *) dc->dc_values)[set];
		  if (DV_DB_NULL == *dv)
		    bits[set] &= clr_mask;
		}
	    }
	  else if ((DCT_BOXES & dc->dc_type))
	    {
	      for (set = 0; set < dc->dc_n_values; set++)
		{
		  caddr_t box = ((caddr_t *) dc->dc_values)[set];
		  if (DV_DB_NULL == DV_TYPE_OF (box))
		    bits[set] &= clr_mask;
		}
	    }
	  else if (dc->dc_nulls)
	    {
	      for (set = 0; set < dc->dc_n_values; set++)
		{
		  if (BIT_IS_SET (dc->dc_nulls, set))
		    bits[set] &= clr_mask;
		}
	    }
	}

    next_out:;
    }
}


void
itc_cset_bit_final (it_cursor_t * itc, int prev_n_results)
{
  /*  itc_cset_bits has the matched mask at the place of the row.  Compact these so they correspond to matches */
  caddr_t *inst = itc->itc_out_state;
  db_buf_t bits = itc->itc_cset_bits;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  int n_bytes = csm->csm_cset_bit_bytes;
  int inx;
  state_slot_t *ssl = csm->csm_exc_bits_out[0];
  data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
  DC_CHECK_LEN (dc, itc->itc_n_results - 1);
  if (itc->itc_n_matches)
    {
      for (inx = 0; inx < itc->itc_n_matches; inx++)
	{
	  row_no_t row = itc->itc_matches[inx];
	  uint64 word = 0;
	  memcpy_16 (&word, &bits[row * n_bytes], n_bytes);
	  dc_append_int64 (dc, word);
	}
    }
  else
    {
      row_range_t rng = itc->itc_ranges[itc->itc_set - itc->itc_col_first_set];
      for (inx = rng.r_first; inx < rng.r_end; inx++)
	{
	  uint64 word = 0;
	  memcpy_16 (&word, &bits[inx * n_bytes], n_bytes);
	  dc_append_int64 (dc, word);
	}
    }
}

#define CSQ_PTR(p) ((query_t*)LOW_48 (p))
#define CSQ_FLAGS(p) HIGH_16 (p)

#define CSQ_MAKER (1L << 48)


int
csts_is_applicable (cset_ts_t * csts, cset_t * cset)
{
  int inx;
  DO_BOX (iri_id_t, p, inx, csts->csts_reqd_ps)
  {
    if (!gethash ((void *) p, &cset->cset_p))
      {
	if (!gethash ((void *) p, &cset->cset_except_p))
	  return 0;
      }
  }
  END_DO_BOX;
  return 1;
}


void
qr_cset_adjust_dcs (query_t * qr, caddr_t * inst)
{
  /* dc types may differ between cset variants */
  QNCAST (QI, qi, inst);
  data_source_t *qn;
  int inx;
  DO_BOX (state_slot_t *, ssl, inx, qr->qr_vec_ssls)
  {
    data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
    if (ssl->ssl_sets && !QST_BOX (int *, inst, ssl->ssl_sets))
       QST_BOX (int *, inst, ssl->ssl_sets) = (int *) mp_alloc_box (qi->qi_mp, sizeof (int) * dc_batch_sz, DV_BIN);
    if (!dc)
      continue;
    dc_reset (dc);
    dc->dc_sqt.sqt_dtp = ssl->ssl_sqt.sqt_dtp;
    dc->dc_sqt.sqt_col_dtp = ssl->ssl_sqt.sqt_col_dtp;
    dc_set_flags (dc, &ssl->ssl_sqt, ssl->ssl_dc_dtp);
    if (ssl->ssl_sqt.sqt_col_dtp)
      dc->dc_sqt.sqt_col_dtp = ssl->ssl_sqt.sqt_col_dtp;
  }
  END_DO_BOX;
  for (qn = qr->qr_head_node; qn; qn = qn_next (qn))
    {
      SRC_IN_STATE (qn, inst) = NULL;
      if (IS_TS (qn))
	{
	  QNCAST (table_source_t, ts, qn);
	  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
	  if (itc && DV_ITC == DV_TYPE_OF (itc))
	    itc_col_free (itc);
	}
    }
}


query_t *
ts_cset_query (table_source_t * ts, caddr_t * inst, cset_t * cset, int mode, caddr_t * o_mode)
{
  cset_ts_t *csts = ts->ts_csts;
  caddr_t err = NULL;
  du_thread_t *self;
  dk_set_t w;
  ptrlong key = cset->cset_id + mode;
  query_t *qr = QST_BOX (query_t *, inst, csts->csts_qr);
  if (qr && key == QST_INT (inst, csts->csts_qr_key))
    return qr;
  mutex_enter (&csts->csts_mtx);
  qr = (query_t *) gethash ((void *) key, &csts->csts_cset_plan);
  if (qr && !CSQ_FLAGS (qr))
    {
      mutex_leave (&csts->csts_mtx);
      qr_cset_adjust_dcs (qr, inst);
      QST_BOX (query_t *, inst, csts->csts_qr) = qr;
      QST_INT (inst, csts->csts_qr_key) = key;
      return qr;
    }
  self = THREAD_CURRENT_THREAD;
  if (!qr)
    {
      w = CONS (self, NULL);
      sethash ((void *) key, &csts->csts_cset_plan, (void *) ((ptrlong) w | CSQ_MAKER));
      mutex_leave (&csts->csts_mtx);
      qr = csg_query (cset, ts, mode, o_mode, &err);
      mutex_enter (&csts->csts_mtx);
      w = (dk_set_t) LOW_48 (gethash ((void *) key, &csts->csts_cset_plan));
      if (!err)
	sethash ((void *) key, &csts->csts_cset_plan, (void *) qr);
      else
	remhash ((void *) key, &csts->csts_cset_plan);
      mutex_leave (&csts->csts_mtx);
      DO_SET (du_thread_t *, thr, &w)
      {
	if (thr != self)
	  {
	    thr->thr_reset_code = box_copy_tree (err);
	    semaphore_leave (thr->thr_sem);
	  }
      }
      END_DO_SET ();
      dk_set_free (w);
      if (err)
	sqlr_resignal (err);
      qr_cset_adjust_dcs (qr, inst);
      QST_BOX (query_t *, inst, csts->csts_qr) = qr;
      QST_INT (inst, csts->csts_qr_key) = key;
      return qr;
    }
  w = (dk_set_t) LOW_48 (qr);
  w->next = CONS (self, w->next);
  self->thr_reset_code = NULL;
  mutex_leave (&csts->csts_mtx);
  semaphore_enter (self->thr_sem);
  if ((err = self->thr_reset_code))
    {
      self->thr_reset_code = NULL;
      sqlr_resignal (err);
    }
  return ts_cset_query (ts, inst, cset, mode, o_mode);
}


int
csts_resume_pending (query_t * subq, caddr_t * inst)
{
  int any = 0;
  dk_set_t nodes = subq->qr_bunion_reset_nodes ? subq->qr_bunion_reset_nodes : subq->qr_nodes;
  QNCAST (QI, qi, inst);
  int n_sets_save = qi->qi_n_sets;
  /* if the qr is a union term, continue the nodes that belong to the union term itself. qr_nodes is null and the nodes of the union term are added to the nodes of the containing qr */
cont_innermost_loop:
  DO_SET (data_source_t *, src, &nodes)
  {
    if (inst[src->src_in_state])
      {
	if (src->src_continue_reset)
	  dc_reset_array (inst, src, src->src_continue_reset, -1);
	qi->qi_n_sets = n_sets_save;
	SRC_RESUME (src, inst);
	src->src_input (src, inst, NULL);
	SRC_RETURN (src, inst);
	any = 1;
	goto cont_innermost_loop;
      }
  }
  END_DO_SET ();
  return any;
}

void
table_source_cset_scan_input (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int any;
  query_t *cset_qr;
  int start_cset = 0;
  cset_ts_t *csts = ts->ts_csts;
  int nth_cset = QST_INT (inst, csts->csts_nth_cset);
  int mode = QST_INT (inst, csts->csts_cset_mode);
  if (state)
    {
      SRC_IN_STATE (ts, inst) = inst;
      start_cset = 1;
      nth_cset = QST_INT (inst, csts->csts_nth_cset) = 0;
      mode = QST_INT (inst, csts->csts_cset_mode) = 0;
      QST_BOX (caddr_t, inst, csts->csts_qr) = NULL;
    }
next_cset:
  for (;;)
    {
      if (!rt_ranges[nth_cset].rtr_cset || !csts_is_applicable (csts, rt_ranges[nth_cset].rtr_cset))
	{
	  nth_cset = ++QST_INT (inst, csts->csts_nth_cset);
	  QST_BOX (caddr_t, inst, csts->csts_qr) = NULL;
	  if (nth_cset >= CSET_MAX)
	    {
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;
	    }
	  start_cset = 1;
	  continue;
	}
      cset_qr = ts_cset_query (ts, inst, rt_ranges[nth_cset].rtr_cset, mode, NULL);
      if (start_cset)
	{
	  qn_input (cset_qr->qr_head_node, inst, inst);
	  nth_cset = ++QST_INT (inst, csts->csts_nth_cset);
	  goto next_cset;
	}
      else
	{
	  any = csts_resume_pending (cset_qr, inst);
	  if (!any)
	    {
	      nth_cset = ++QST_INT (inst, csts->csts_nth_cset);
	      goto next_cset;
	    }
	}
    }
}


void
cset_align_input (cset_align_node_t * csa, caddr_t * inst, caddr_t * state)
{
  /* take the value from either cset or exception, depending on bit */
  QNCAST (QI, qi, inst);
  int n_sets = QST_INT (inst, csa->src_gen.src_prev->src_out_fill);
  table_source_t *model_ts = csa->csa_model_ts;
  int *sets;
  QST_INT (inst, model_ts->src_gen.src_out_fill) = n_sets;
  QN_CHECK_SETS (model_ts, inst, n_sets);
  sets = QST_BOX (int *, inst, model_ts->src_gen.src_sets);
  int_asc_fill (sets, n_sets, 0);
  qi->qi_n_sets = n_sets;
  qi->qi_set_mask = NULL;
  if (csa->csa_ssls)
    {
      int n = box_length (csa->csa_ssls) / sizeof (cset_align_t), inx, bit;
      for (inx = 0; inx < n; inx++)
	{
	  cset_align_t *csal = &csa->csa_ssls[inx];
	  state_slot_t *val;
	  if (!csal->csa_res)
	    continue;
	  if ((CSA_S == csal->csa_flag || CSA_G == csal->csa_flag)
	      && csal->csa_first && csal->csa_second && -1 != (bit = csa->csa_no_cset_bit))
	    {
	      int set;
	      state_slot_t *bits_ssl = csa->csa_exc_bits[bit / 64];
	      uint64 *bits = (uint64 *) QST_BOX (data_col_t *, inst, bits_ssl->ssl_index)->dc_values;
	      uint64 mask = 1L << (bit % 64);
	      data_col_t *res_dc = QST_BOX (data_col_t *, inst, csal->csa_res->ssl_index);
	      data_col_t *cset_s = QST_BOX (data_col_t *, inst, csal->csa_first->ssl_index);
	      data_col_t *rq_s = QST_BOX (data_col_t *, inst, csal->csa_second->ssl_index);
	      for (set = 0; set < n_sets; set++)
		{
		  int set_no = SSL_VEC == bits_ssl->ssl_type ? set : sslr_set_no (inst, bits_ssl, set);
		  if ((bits[set_no] & mask))
		    {
		      dc_assign (inst, csal->csa_res, set, csal->csa_first, set);
		    }
		  else
		    {
		      /* there is no cset match, s from 1st rq */
		      dc_assign (inst, csal->csa_res, set, csal->csa_second, set);
		    }
		}
	    }
	  else
	    {
	      val = csal->csa_second ? csal->csa_second : csal->csa_first;
	      vec_ssl_assign (inst, csal->csa_res, val);
	    }
	}
    }
  qn_send_output ((data_source_t *) model_ts, inst);
}


int
csts_next_cset (cset_ts_t * csts, caddr_t * inst, int nth_cset)
{
  db_buf_t bits = QST_BOX (db_buf_t, inst, csts->csts_lookup_csets->ssl_index);
  int inx;
  for (inx = nth_cset + 1; inx < CSET_MAX; inx++)
    if (BIT_IS_SET (bits, inx))
      return inx;
  return CSET_MAX;
}

int
csts_non_unq_s_lookup (table_source_t * ts, caddr_t * inst)
{
  return 0;
}


void
table_source_cset_lookup_input (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  state_slot_t *s_ssl;
  data_col_t *s_dc;
  iri_id_t s_const = 0;
  caddr_t *o_mode;
  db_buf_t cset_bits;
  cset_ts_t *csts = ts->ts_csts;
  int nth_cset, set;
  QNCAST (QI, qi, inst);
  key_source_t *ks = ts->ts_order_ks;
  int n_sets = ts->src_gen.src_prev ? QST_INT (inst, ts->src_gen.src_prev->src_out_fill) : qi->qi_n_sets;
  int any;
  if (state)
    {
      if (!ks->ks_is_qf_first)
	{
	  ks_vec_params (ks, NULL, inst);
	  if (ks->ks_last_vec_param)
	    n_sets = QST_BOX (data_col_t *, inst, ks->ks_last_vec_param->ssl_index)->dc_n_values;
	  else
	    n_sets = ts->src_gen.src_prev ? QST_INT (inst, ts->src_gen.src_prev->src_out_fill) : qi->qi_n_sets;
	}
    }
  if (csts->csts_o_scan_mode && (o_mode = (caddr_t *) qst_get (inst, csts->csts_o_scan_mode)))
    {
      /* scan variant with an o cond */
      if (state)
	{
	  cset_t *cset = cset_by_id (unbox (o_mode[0]));
	  query_t *qr = ts_cset_query (ts, inst, cset, CSQ_TABLE, o_mode);
	  SRC_IN_STATE (ts, inst) = inst;
	  qn_input (qr->qr_head_node, inst, inst);
	}
      any = csts_resume_pending (QST_BOX (query_t *, inst, csts->csts_qr), inst);
      if (!any)
	SRC_IN_STATE (ts, inst) = NULL;
      return;
    }
  s_ssl = ks->ks_spec.ksp_spec_array->sp_min_ssl;
  if (SSL_CONSTANT == s_ssl->ssl_type)
    s_const = unbox_iri_id (s_ssl->ssl_constant);
  else
    s_dc = QST_BOX (data_col_t *, inst, s_ssl->ssl_index);
  cset_bits = (db_buf_t) qst_get (inst, csts->csts_lookup_csets);
  if (!cset_bits)
    {
      cset_bits = dk_alloc_box (CSET_MAX / 8, DV_BIN);
      qst_set (inst, csts->csts_lookup_csets, (caddr_t) cset_bits);
    }
  if (state)
    {
      nth_cset = CSET_MAX;
      memzero (cset_bits, box_length (cset_bits));
      for (set = 0; set < n_sets; set++)
	{
	  iri_id_t s = s_const ? s_const : ((iri_id_t *) s_dc->dc_values)[set];
	  int rng = IRI_RANGE (s);
	  BIT_SET (cset_bits, rng);
	  if (rng < nth_cset)
	    nth_cset = rng;
	}
      QST_INT (inst, csts->csts_nth_cset) = nth_cset;
      QST_INT (inst, csts->csts_cset_mode) = CSQ_LOOKUP;
    }
  for (;;)
    {
      int mode = QST_INT (inst, csts->csts_cset_mode);
      nth_cset = QST_INT (inst, csts->csts_nth_cset);
      if ((CSQ_RUN & mode))
	{
	  query_t *qr = QST_BOX (query_t *, inst, csts->csts_qr);
	  int any = csts_resume_pending (qr, inst);
	  if (!any)
	    {
	      mode = QST_INT (inst, csts->csts_cset_mode) & ~CSQ_RUN;
	      if (CSQ_LOOKUP == mode)
		{
		  if (csts_non_unq_s_lookup (ts, inst))
		    {
		      QST_INT (inst, mode) = CSQ_G_MIX;
		      continue;
		    }
		  nth_cset = QST_INT (inst, csts->csts_nth_cset) = csts_next_cset (csts, inst, nth_cset);
		  if (CSET_MAX == nth_cset)
		    {
		      SRC_IN_STATE (ts, inst) = NULL;
		      return;
		    }
		  mode = QST_INT (inst, csts->csts_cset_mode) = CSQ_LOOKUP;
		}

	    }
	  continue;
	}
      if (CSQ_LOOKUP == mode)
	{
	  query_t *qr = ts_cset_query (ts, inst, cset_by_id (nth_cset), mode, NULL);
	  QST_INT (inst, csts->csts_cset_mode) |= CSQ_RUN;
	  SRC_IN_STATE (ts, inst) = inst;
	  qn_input (qr->qr_head_node, inst, inst);
	  continue;
	}
      if (CSQ_G_MIX == mode)
	{
	  query_t *qr = ts_cset_query (ts, inst, cset_by_id (nth_cset), mode, NULL);
	  QST_INT (inst, csts->csts_cset_mode) |= CSQ_RUN;
	  qn_input (qr->qr_head_node, inst, inst);
	  continue;
	}
    }
}


#define SSL_MV(inst, from, from_set, to)

void
ssl_mv_nn (data_col_t * to, data_col_t * from, int from_set)
{
  if ((DCT_BOXES & from->dc_type))
    {
      ((caddr_t *) to->dc_values)[to->dc_n_values++] = ((caddr_t *) from->dc_values)[from_set];
      ((caddr_t *) from->dc_values)[from_set] = NULL;
    }
  if (DV_SINGLE_FLOAT == from->dc_sqt.sqt_dtp)
    ((float *) to->dc_values)[to->dc_n_values++] = ((float *) from->dc_values)[from_set];
  else if (DV_DATETIME == from->dc_sqt.sqt_dtp)
    {
      memcpy_dt (to->dc_values + DT_LENGTH * to->dc_n_values, from->dc_values + from_set * DT_LENGTH);
      to->dc_n_values++;
    }
  else
    ((int64 *) to->dc_values)[to->dc_n_values++] = ((int64 *) from->dc_values)[from_set];
}


int
itc_cset_exc_param_nos (it_cursor_t * itc)
{
  int *nos = itc->itc_param_order;
  int inx, fill = 0;
  uint64 *bf;
  uint32 n_bf;
  if (itc->itc_ks->ks_is_cset_exc_scan)
    {
      nos[0] = 0;
      return 1;
    }
  table_source_t *ts = itc->itc_ks->ks_ts;
  iri_id_t p = unbox_iri_id (itc->itc_search_params[0]);
  cset_t *cset = ts->ts_csm->csm_cset;
  data_col_t *s_dc = ITC_P_VEC (itc, 1);
  iri_id_t *s_arr = (iri_id_t *) s_dc->dc_values;
  cset_p_t *csetp = (cset_p_t *) gethash ((void *) p, &cset->cset_p);
  caddr_t *inst = itc->itc_out_state;
  int n_from_cset = QST_INT (inst, ts->src_gen.src_out_fill);
  itc->itc_n_results = n_from_cset;	/*if bindings from cset table, do make a full batch, these are in the same batch with the exceptions, so this many are already in results */
  if (!csetp)
    return 0;
  csetp_bloom (csetp, inst, &bf, &n_bf);
  if (!bf)
    {
      itc->itc_n_sets = 0;
      return 1;
    }
  for (inx = 0; inx < s_dc->dc_n_values; inx++)
    {
      uint64 h, w;
      uint64 mask;
      iri_id_t s = s_arr[inx];
      MHASH_STEP_1 (h, s);
      mask = BF_MASK (h);
      w = bf[BF_WORD (h, n_bf)];
      if (mask == (w & mask))
	nos[fill++] = inx;
    }
  itc->itc_n_sets = fill;
  return 1;
}


void
itc_cset_s_param_nos (it_cursor_t * itc, int n_values)
{
  int *nos = itc->itc_param_order;
  int inx, fill = 0;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_t *cset = ts->ts_csm->csm_cset;
  data_col_t *s_dc = ITC_P_VEC (itc, 0);
  iri_id_t s_const = 0;
  iri_id_t *s_arr = NULL;
  if (s_dc)
    {
      s_arr = (iri_id_t *) s_dc->dc_values;
      n_values = s_dc->dc_n_values;
    }
  else
    {
      s_const = unbox_iri_id (itc->itc_search_params[0]);
    }
  for (inx = 0; inx < n_values; inx++)
    {
      iri_id_t s = s_const ? s_const : s_arr[inx];
      if (IRI_RANGE (s) == cset->cset_id)
	nos[fill++] = inx;
    }
  itc->itc_n_sets = fill;
  return;
}


/* csm mode in psog for cset exceptions */
#define PSOG_CSET_INIT 0
#define PSOG_CSET_VALUES 1
#define PSOG_OWN_VALUES 2
#define PSOG_OUTER_NULLS 3
#define PSOG_SCAN_EXC 4		/* after cset ts, get s values not in cset ts scan */
#define PSOG_PRE_EXC_SCAN 5

void
cset_psog_bits (table_source_t * ts, caddr_t * inst)
{
  /* for results from rq or nulls for oj misses, fill in the out bits by the out sets */
  cset_mode_t *csm = ts->ts_csm;
  if (csm->csm_exc_bits_in)
    {
      int n_res = QST_INT (inst, ts->src_gen.src_out_fill);
      int *out_sets = QST_BOX (int *, inst, ts->src_gen.src_sets);
      int nth_bit = csm->csm_bit % 64, set;
      int nth_ssl = csm->csm_bit / 64;
      state_slot_t *ssl1 = csm->csm_exc_bits_in[nth_ssl];
      int n_dcs = BOX_ELEMENTS (csm->csm_exc_bits_out);
      data_col_t *bit_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_bits_in[nth_ssl]->ssl_index);
      data_col_t *out_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_bits_out[0]->ssl_index);
      uint64 *bits = (uint64 *) bit_dc->dc_values;
      uint64 *out;
      int first_rq_set = QST_INT (inst, csm->csm_n_from_cset);
      int fill = first_rq_set;
      DC_CHECK_LEN (out_dc, n_res - 1);
      out = (uint64 *) out_dc->dc_values;
      if (SSL_VEC == ssl1->ssl_type)
	{
	  for (set = first_rq_set; set < n_res; set++)
	    {
	      out[fill++] = bits[out_sets[set]];
	    }
	}
      else
	{
	  for (set = first_rq_set; set < n_res; set++)
	    {
	      int out_set = out_sets[set];
	      int in_set = sslr_set_no (inst, csm->csm_exc_bits_in[0], out_set);
	      out[fill++] = bits[in_set];
	    }
	}
    }
}



void
sslr_n_consec_ref_2 (caddr_t * inst, state_slot_ref_t * sslr, int *sets, int set, int n_sets)
{
  if (SSL_VEC == sslr->ssl_type)
    int_asc_fill (sets, n_sets, set);
  else
    sslr_n_consec_ref (inst, sslr, sets, set, n_sets);
}


void
cset_psog_cset_values (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill), set, r;
  cset_mode_t *csm = ts->ts_csm;
  data_col_t *o_dc = csm->csm_rq_o ? QST_BOX (data_col_t *, inst, csm->csm_rq_o->ssl_index) : NULL;
  data_col_t *cset_dc = csm->csm_rq_o ? QST_BOX (data_col_t *, inst, csm->csm_cset_o->ssl_index) : NULL;
  int out_fill = 0;
  int nth_bit = csm->csm_bit % 64;
  int nth_ssl = csm->csm_bit / 64;
  state_slot_t *ssl1 = csm->csm_exc_bits_in[nth_ssl];
  int n_dcs = BOX_ELEMENTS (csm->csm_exc_bits_out);
  data_col_t *bit_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_bits_in[nth_ssl]->ssl_index);
  int64 this_mask = 1L << nth_bit;
  data_col_t *out_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_bits_out[0]->ssl_index);
  uint64 *bits = (uint64 *) bit_dc->dc_values;
  uint64 *out;
  key_source_t *ks;
  int cset_sets[ARTM_VEC_LEN];
  QST_INT (inst, ts->src_gen.src_out_fill) = 0;
  DC_CHECK_LEN (out_dc, n_sets - 1);
  out = (uint64 *) out_dc->dc_values;
  if (o_dc)
    {
      DC_CHECK_LEN (o_dc, n_sets - 1);
      if (DV_ANY == cset_dc->dc_sqt.sqt_dtp && DV_ANY != o_dc->dc_sqt.sqt_dtp)
	dc_heterogenous (o_dc);
      else
	dc_convert_empty (o_dc, dtp_canonical[cset_dc->dc_sqt.sqt_dtp]);
      dc_clr_nulls (o_dc);
    }
  if (SSL_REF == ssl1->ssl_type)
    {
      int sets[ARTM_VEC_LEN];
      for (set = 0; set < n_sets; set += ARTM_VEC_LEN)
	{
	  int last = MIN (n_sets, set + ARTM_VEC_LEN), r;
	  sslr_n_consec_ref (inst, (state_slot_ref_t *) csm->csm_exc_bits_in[0], sets, set, last - set);
	  if (o_dc)
	    sslr_n_consec_ref_2 (inst, (state_slot_ref_t *) csm->csm_cset_o, cset_sets, set, last - set);
	  for (r = 0; r < last - set; r++)
	    {
	      if (this_mask & bits[sets[r]])
		{
		  out[out_fill++] = bits[sets[r]] & ~this_mask;
		  if (o_dc)
		    ssl_mv_nn (o_dc, cset_dc, sets[cset_sets[r]]);
		  qn_result ((data_source_t *) ts, inst, set + r);
		}
	    }
	}
    }
  else
    {
      for (set = 0; set < n_sets; set += ARTM_VEC_LEN)
	{
	  int last = MIN (n_sets, set + ARTM_VEC_LEN), r;
	  if (o_dc)
	    sslr_n_consec_ref_2 (inst, (state_slot_ref_t *) csm->csm_cset_o, cset_sets, set, last - set);
	  for (r = 0; r < last - set; r++)
	    {
	      if (this_mask & bits[set + r])
		{
		  out[out_fill++] = bits[set + r] & ~this_mask;
		  if (o_dc)
		    ssl_mv_nn (o_dc, cset_dc, cset_sets[r]);
		  qn_result ((data_source_t *) ts, inst, set + r);
		}
	    }
	}
    }
  ks = ts->ts_order_ks;
  if (ks->ks_last_vec_param)
    ssl_consec_results (ks->ks_last_vec_param, inst, n_sets);
  QST_INT (inst, csm->csm_n_from_cset) = QST_INT (inst, ts->src_gen.src_out_fill);
  ts_at_end (ts, inst);
}


table_source_t *
rq_ts_cset_ts (table_source_t * ts)
{
  for (ts = (table_source_t *) ts->src_gen.src_prev; ts; ts = (table_source_t *) ts->src_gen.src_prev)
    {
      if (IS_TS (ts) && ts->ts_csm && ts->ts_csm->csm_cset_scan_state)
	return ts;
    }
  GPF_T1 ("first rq ts must be after cset ts");
  return NULL;
}

#define CSET_LOWER(c) ((uint64)c->cset_id << CSET_RNG_SHIFT)


void
psog_exc_scan_sets (table_source_t * ts, caddr_t * inst, int init_out)
{
  cset_mode_t *csm = ts->ts_csm;
  data_col_t *bits_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_bits_out[0]->ssl_index);
  int64 *bits = (int64 *) bits_dc->dc_values;
  int *sets;
  int fill = QST_INT (inst, ts->src_gen.src_out_fill), set;
  sets = QST_BOX (int *, inst, ts->src_gen.src_sets);
  for (set = init_out; set < fill; set++)
    {
      sets[set] = set - init_out;
      bits[set] = 1L << csm->csm_bit;
    }
}


void
psog_cset_scan_exceptions (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  search_spec_t *cset_s_spec;
  buffer_desc_t *buf;
  QNCAST (QI, qi, inst);
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  int init_out = QST_INT (inst, ts->src_gen.src_out_fill);
  int scan_state, from_next, batch_size;
  key_source_t *ks = ts->ts_order_ks;
  it_cursor_t *cset_itc;
  cset_mode_t *csm = ts->ts_csm;
  data_col_t *s_out_dc = QST_BOX (data_col_t *, inst, ks->ks_spec.ksp_spec_array->sp_next->sp_min_ssl->ssl_index);
  data_col_t *exc_s_dc = QST_BOX (data_col_t *, inst, csm->csm_exc_scan_exc_s->ssl_index);
  table_source_t *cset_ts = rq_ts_cset_ts (ts);
  cset_mode_t *cset_csm = cset_ts->ts_csm;
  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
  iri_id_t lower = 0, upper = 0;
  key_source_t *exc_ks = ts->ts_csm->csm_exc_scan_ks;
  data_col_t *s_lookup_dc;
  if (state && QST_INT (inst, ts->src_gen.src_out_fill))
    {
      /* send results so far, the scan of exc will be its own set of vectors */
      QST_INT (inst, csm->csm_mode) = PSOG_PRE_EXC_SCAN;
      SRC_IN_STATE (ts, inst) = inst;
      qn_ts_send_output ((data_source_t *) ts, inst, NULL);
      ks_vec_new_results (ks, inst, NULL);
    }
  if (!state && PSOG_PRE_EXC_SCAN == QST_INT (inst, csm->csm_mode))
    state = inst;
again:
  batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
  if (state)
    {
      QST_INT (inst, csm->csm_mode) = PSOG_SCAN_EXC;
      if (!itc)
	{
	  /* can be no itc if cset empty */
	  s_lookup_dc = NULL;
	  itc = itc_create (NULL, qi->qi_trx);
	  TS_ORDER_ITC (ts, state) = itc;
	  QST_BOX (data_col_t *, inst, csm->csm_exc_scan_exc_s->ssl_index)->dc_n_values = 0;
	}
      else
	{
	  s_lookup_dc = ITC_P_VEC (itc, 1);
	  DC_CHECK_LEN (exc_s_dc, s_lookup_dc->dc_n_values - 1);
	  memcpy_16 (exc_s_dc->dc_values, s_lookup_dc->dc_values, sizeof (iri_id_t) * s_lookup_dc->dc_n_values);
	  exc_s_dc->dc_n_values = s_lookup_dc->dc_n_values;
	}
      scan_state = QST_INT (inst, cset_csm->csm_cset_scan_state);
      cset_itc = TS_ORDER_ITC (cset_ts, inst);
      cset_s_spec = cset_itc->itc_key_spec.ksp_spec_array;
      if ((CSET_SCAN_FIRST & scan_state))
	{
	  /* the lower is the cset lower or the cset itc lowrer, if set */
	  lower = (!cset_s_spec || CMP_NONE == cset_s_spec->sp_min_op) ? CSET_LOWER (cset_csm->csm_cset)
	      : unbox_iri_id (cset_itc->itc_search_params[cset_s_spec->sp_min]);
	}
      else if (s_lookup_dc)
	lower = unbox_iri_id (qst_get (inst, csm->csm_exc_scan_upper));
      if ((CSET_SCAN_LAST & scan_state))
	{
	  /* the upper is the next cset lower or the cset itc upper, if set */
	  upper = (!cset_s_spec || CMP_NONE == cset_s_spec->sp_max_op) ? (1L << CSET_RNG_SHIFT) + CSET_LOWER (cset_csm->csm_cset)
	      : unbox_iri_id (cset_itc->itc_search_params[cset_s_spec->sp_max]);
	}
      else if (s_lookup_dc)
	upper = ((iri_id_t *) s_lookup_dc->dc_values)[s_lookup_dc->dc_n_values - 1];
      qst_set (inst, csm->csm_exc_scan_lower, box_iri_id (lower));
      qst_set (inst, csm->csm_exc_scan_upper, box_iri_id (upper));
      dc_reset (s_out_dc);	/* the lookup s is also an ouit dc of this, reset */
      itc_col_free (itc);
      ks_start_search (exc_ks, inst, inst, itc, &buf, ts, SM_READ);
      from_next = 0;
    }
  else
    {
      init_out = 0;
      dc_reset (s_out_dc);
      ITC_FAIL (itc)
      {
	buf = page_reenter_excl (itc);
	from_next = 1;
	itc_vec_next (itc, &buf);
      }
      END_FAIL (itc);
    }
  if (batch_size == itc->itc_n_results)
    {
      itc_register_and_leave (itc, buf);
    }
  else
    {
      if (from_next)
	itc_page_leave (itc, buf);
      SRC_IN_STATE (ts, inst) = NULL;
    }

  if (batch_size == itc->itc_n_results)
    {
      psog_exc_scan_sets (ts, inst, 0);
      qn_ts_send_output ((data_source_t *) ts, inst, NULL);
      itc_vec_new_results (itc);
      state = NULL;
      goto again;
    }
  else
    {
      psog_exc_scan_sets (ts, inst, 0);
      ts_at_end (ts, inst);
    }
}


void
ts_empty_cset_scan (table_source_t * ts, caddr_t * inst)
{
  table_source_t *exc_ts = (table_source_t *) qn_next ((data_source_t *) ts);
  cset_mode_t *exc_csm = exc_ts->ts_csm;
  QST_INT (inst, exc_csm->csm_cset_scan_state) = CSET_SCAN_FIRST | CSET_SCAN_LAST;
  qn_result ((data_source_t *) ts, inst, 0);
  QST_INT (inst, exc_ts->ts_cycle_pos[0]) = 2;
  psog_cset_scan_exceptions (ts, inst, inst);
}


void
psog_outer_nulls (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  /* set a null o for the sets with no output from this */
  int this_bit = -1, set;
  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
  data_col_t *o_dc = NULL;
  data_col_t *exc_bits_dc = NULL;
  state_slot_t *exc_ssl = NULL;
  uint64 *exc_bits = NULL;
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  db_buf_t set_card = QST_BOX (db_buf_t, inst, ts->ts_set_card_bits);
  cset_mode_t *csm = ts->ts_csm;
  int last_outer;
  int batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
  if (state)
    last_outer = QST_INT (inst, csm->csm_last_outer) = 0;
  else
    last_outer = QST_INT (inst, csm->csm_last_outer);
  if (-1 != csm->csm_bit)
    {
      this_bit = csm->csm_bit % 64;
      exc_ssl = csm->csm_exc_bits_in[csm->csm_bit / 64];
      exc_bits_dc = QST_BOX (data_col_t *, inst, exc_ssl->ssl_index);
      exc_bits = (uint64 *) exc_bits_dc->dc_values;
    }
  if (csm->csm_rq_o)
    o_dc = QST_BOX (data_col_t *, inst, csm->csm_rq_o->ssl_index);
  for (set = last_outer; set < n_sets; set++)
    {
      if (batch_size == QST_INT (inst, ts->src_gen.src_out_fill))
	{
	  QST_INT (inst, csm->csm_last_outer) = set;
	  SRC_IN_STATE (ts, inst) = inst;
	  cset_psog_bits (ts, inst);
	  qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	  itc_vec_new_results (itc);
	  batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
	}
      if (BIT_IS_SET (set_card, 2 * set))
	continue;
      if (exc_bits)
	{
	  int set2;
	  if (SSL_REF == exc_ssl->ssl_type)
	    set2 = sslr_set_no (inst, exc_ssl, set);
	  else
	    set2 = set;
	  if ((exc_bits[set2] & (1L << this_bit)))
	    continue;
	}
      if (o_dc)
	dc_append_null (o_dc);
      qn_result ((data_source_t *) ts, inst, set);
      if (batch_size == QST_INT (inst, ts->src_gen.src_out_fill))
	{
	  QST_INT (inst, csm->csm_last_outer) = set;
	  SRC_IN_STATE (ts, inst) = inst;
	  cset_psog_bits (ts, inst);
	  qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	  itc_vec_new_results (itc);
	  batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
	}
    }
  ts_at_end (ts, inst);
  if (QST_INT (inst, ts->src_gen.src_out_fill))
    cset_psog_bits (ts, inst);
}


void
cset_psog_input (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  cset_mode_t *csm = ts->ts_csm;
  int order_buf_preset = 0;
  query_instance_t *qi = (query_instance_t *) inst;
  int rc, start;
  int batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
  if (state)
    {
      QST_INT (inst, csm->csm_mode) = PSOG_OWN_VALUES;
      if (ts->ts_is_outer)
	{
	  int set_bytes = ALIGN_8 (n_sets) / 4;
	  qi_ensure_array (inst, &QST_BOX (caddr_t, inst, ts->ts_set_card_bits), set_bytes, 0);
	  memzero (QST_BOX (caddr_t, inst, ts->ts_set_card_bits), set_bytes);
	}
    }

  for (;;)
    {
      buffer_desc_t *order_buf = NULL;
      it_cursor_t *order_itc;
      if (!state)
	{
	  start = 0;
	  order_itc = TS_ORDER_ITC (ts, inst);
	  if (ts->ts_aq && ts_handle_aq (ts, inst, &order_buf, &order_buf_preset))
	    return;
	}
      else
	start = 1;
      order_itc = TS_ORDER_ITC (ts, state);
      if (start)
	{
	  SRC_IN_STATE (ts, inst) = inst;	/* for anytime break, must know if being run */
	  if (!order_itc)
	    {
	      order_itc = itc_create (NULL, qi->qi_trx);
	      TS_ORDER_ITC (ts, state) = order_itc;
	    }
	  order_itc->itc_random_search = RANDOM_SEARCH_OFF;
	  rc = ks_start_search (ts->ts_order_ks, inst, state,
	      order_itc, &order_buf, ts, ts->ts_is_unique ? SM_READ_EXACT : SM_READ);
	  if (!rc)
	    {
	      ts_at_end (ts, inst);
	      if (ts->ts_aq)
		ts_aq_handle_end (ts, inst);
	      ts_check_batch_sz (ts, inst, order_itc);
	      if (!ts->ts_is_outer && QST_INT (inst, ts->src_gen.src_out_fill))
		cset_psog_bits (ts, inst);
	      return;
	    }
	  itc_register_and_leave (order_itc, order_buf);
	}
      else
	{
	  if (order_buf && !order_buf_preset)
	    GPF_T;		/* TS loops back and order buf is set */
	  if (!order_buf_preset && !order_itc->itc_is_registered)
	    {
	      log_error ("cursor not continuable as it is unregistered");
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;
	    }
	  ITC_FAIL (order_itc)
	  {
	    int rc;
	    order_itc->itc_ltrx = qi->qi_trx;	/* next batch can be on a different aq thread than previous, so itc ltrx must match */
	    if (order_itc->itc_batch_size)
	      {
		order_itc->itc_n_results = 0;
		order_itc->itc_set_first = 0;
		QST_INT (inst, ts->src_gen.src_out_fill) = 0;
	      }
	    ks_set_dfg_queue (ts->ts_order_ks, inst, order_itc);
	    itc_vec_new_results (order_itc);	/* before reenter, could in principle be placeholders to unregister */
	    if (!order_buf_preset)
	      {
		order_itc->itc_ltrx = qi->qi_trx;	/* in sliced cluster a local can be continued under many different lt's dependeing on which aq thread gets the continue */
		order_buf = page_reenter_excl (order_itc);
	      }
	    itc_vec_next (order_itc, &order_buf);
	    order_itc->itc_rows_selected += order_itc->itc_n_results;
	    rc = order_itc->itc_n_results == order_itc->itc_batch_size ? DVC_MATCH : DVC_LESS;
	    if (DVC_MATCH == rc)
	      {
		itc_register_and_leave (order_itc, order_buf);
	      }
	    else
	      {
		itc_page_leave (order_itc, order_buf);
		ts_at_end (ts, inst);
		if (ts->ts_aq)
		  ts_aq_handle_end (ts, inst);
		ts_check_batch_sz (ts, inst, order_itc);
		if (!ts->ts_is_outer && QST_INT (inst, ts->src_gen.src_out_fill))
		  cset_psog_bits (ts, inst);
		return;
	      }
	  }
	  ITC_FAILED
	  {
	  }
	  END_FAIL (order_itc);
	}
#ifndef NDEBUG
      /* if in state itc must be registered */
      if (SRC_IN_STATE (ts, inst) != NULL && !order_itc->itc_is_registered)
	GPF_T;
#endif
      cset_psog_bits (ts, inst);
      qn_ts_send_output ((data_source_t *) ts, state, ts->ts_after_join_test);
      if (ts->ts_order_ks->ks_top_oby_col)
	ts_top_oby_limit (ts, inst, order_itc);
      state = NULL;
    }
}


/* csm mode in posg with a p that can be in a cset */
#define POSG_SCAN 1


caddr_t
cset_scan_mode (cset_p_t * csetp, caddr_t * inst, search_spec_t * sp, int set)
{
  QNCAST (QI, qi, inst);
  caddr_t *mode = (caddr_t *) dk_alloc_box (6 * sizeof (caddr_t), DV_ARRAY_OF_POINTER);
  qi->qi_set = set;
  mode[0] = box_num (csetp->csetp_cset->cset_id);
  mode[1] = box_iri_id (csetp->csetp_iri);
  mode[2] = box_num (sp->sp_min_op);
  mode[3] = CMP_NONE == sp->sp_min_op ? NULL : box_copy_tree (qst_get (inst, sp->sp_min_ssl));
  mode[4] = box_num (sp->sp_max_op);
  mode[5] = CMP_NONE == sp->sp_max_op ? NULL : box_copy_tree (qst_get (inst, sp->sp_max_ssl));
  return (caddr_t) mode;
}


#define NO_COND ((caddr_t)-1)

void
posg_special_o (table_source_t * ts, caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  key_source_t *ks = ts->ts_order_ks;

  iri_id_t p = unbox_iri_id (qst_get (inst, ks->ks_spec.ksp_spec_array->sp_min_ssl));
  int n_sets;
  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
  data_col_t *p_dc = ITC_P_VEC (itc, 0);
  dk_set_t csps;
  search_spec_t *o_spec = ks->ks_spec.ksp_spec_array->sp_next;
  cset_mode_t *csm = ts->ts_csm;
  int set = QST_INT (inst, csm->csm_posg_last_set);
  cset_p_t *csetp;
  if (itc)
    n_sets = itc->itc_n_sets;
  else
    n_sets = ts->src_gen.src_prev ? QST_INT (inst, ts->src_gen.src_prev->src_out_fill) : qi->qi_n_sets;

  if (-1 == set)
    set = QST_INT (inst, csm->csm_posg_last_set) = 0;
  for (;;)
    {
      ks_vec_new_results (ks, inst, NULL);
      csps = QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos);
      if ((dk_set_t) - 1 == csps)
	csps = QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) = NULL;
      if (!csps)
	{
	  p = p_dc ? ((iri_id_t *) p_dc->dc_values)[set] : unbox_iri_id (itc->itc_search_params[0]);
	  csps = (dk_set_t) gethash ((void *) p, &p_to_csetp_list);
	  if (!csps)
	    goto next_set;
	  QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) = csps->next;
	  csetp = (cset_p_t *) csps->data;
	}
      else
	{
	  csetp = (cset_p_t *) csps->data;
	  QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) = csps->next;
	}
      if (!csetp->csetp_index_o || csetp->csetp_non_index_o)
	{
	  caddr_t min_o, max_o;
	  qi->qi_set = set;
	  min_o = o_spec->sp_min_ssl ? qst_get (inst, o_spec->sp_min_ssl) : NO_COND;
	  max_o = o_spec->sp_max_ssl ? qst_get (inst, o_spec->sp_max_ssl) : NO_COND;
	  if (CMP_EQ == o_spec->sp_min_op && csetp->csetp_non_index_o && !id_hash_get (csetp->csetp_non_index_o, (caddr_t) & min_o))
	    continue;
	  qst_set (inst, csm->csm_o_scan_mode, cset_scan_mode (csetp, inst, o_spec, set));
	  qn_result ((data_source_t *) ts, inst, set);
	  SRC_IN_STATE (ts, inst) = (QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) || set < n_sets - 1) ? inst : NULL;
	  qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	}
      if (QST_BOX (dk_set_t, inst, ts->ts_csm->csm_posg_cset_pos))
	continue;
    next_set:
      set = ++QST_INT (inst, csm->csm_posg_last_set);
      if (set >= n_sets)
	{
	  SRC_IN_STATE (ts, inst) = NULL;
	  return;
	}
    }
}

#define SG_INIT 0
#define SG_CSET 1
#define SG_CHECK_G_MIX 2
#define SG_IN_G_MIX 3


void
table_source_g_mix (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int mode;
  cset_mode_t *csm = ts->ts_csm;
  if (!csm->csm_g_mix_qr)
    return;
  mode = QST_INT (inst, csm->csm_mode);
  if (SG_CHECK_G_MIX == mode)
    {

    }
}


void
table_source_sg_input (table_source_t * ts, caddr_t * inst, caddr_t * volatile state)
{
  /* for s cset table scan/lookup */
  it_cursor_t *order_itc = NULL;
  int need_leave = 0;
  buffer_desc_t *order_buf = NULL;
  int order_buf_preset = 0;
  query_instance_t *qi = (query_instance_t *) inst;
  cset_mode_t *csm = ts->ts_csm;
  int rc;
  order_itc = TS_ORDER_ITC (ts, inst);
  if (!order_itc)
    {
      order_itc = itc_create (NULL, qi->qi_trx);
      TS_ORDER_ITC (ts, state) = order_itc;
      order_itc->itc_random_search = RANDOM_SEARCH_OFF;
    }
  if (state)
    {
      QST_INT (inst, csm->csm_at_end) = 0;
      QST_INT (inst, csm->csm_mode) = SG_INIT;
      if (ts->ts_stream_flush_only)
	QST_INT (inst, ts->ts_stream_flush_only) = 0;
      SRC_IN_STATE (ts, inst) = inst;
    }
again:
  if (!state && ts_stream_flush_ck (ts, inst))
    return;
  if (!state)
    {
      if (ts->ts_aq && ts_handle_aq (ts, inst, &order_buf, &order_buf_preset))
	return;
    }

  for (;;)
    {
      int mode = QST_INT (inst, csm->csm_mode);
      int aq_state = ts->ts_aq_state ? QST_INT (inst, ts->ts_aq_state) : TS_AQ_NONE;
      switch (mode)
	{
	case SG_INIT:
	  rc = ks_start_search (ts->ts_order_ks, inst, state,
	      order_itc, &order_buf, ts, ts->ts_is_unique ? SM_READ_EXACT : SM_READ);
	  if (2 == rc)
	    {
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;
	    }
	  need_leave = rc;	/* if partial batch, order itc is not in order buf */
	  goto results;
	case SG_CHECK_G_MIX:
	case SG_IN_G_MIX:
	  table_source_g_mix (ts, inst, NULL);
	  if (QST_INT (inst, csm->csm_at_end))
	    {
	      ts_aq_handle_end (ts, inst);
	      ts_stream_flush_ck (ts, inst);
	      SRC_IN_STATE (ts, inst) = NULL;
	      ts_aq_final (ts, inst, order_itc);
	      return;
	    }
	  QST_INT (inst, csm->csm_mode) = SG_CSET;
	  /* no break */
	case SG_CSET:
	  if (!order_itc->itc_is_registered)
	    {
	      log_error ("cursor not continuable as it is unregistered");
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;
	    }
	  ITC_FAIL (order_itc)
	  {
	    int rc;
	    order_itc->itc_ltrx = qi->qi_trx;	/* next batch can be on a different aq thread than previous, so itc ltrx must match */
	    order_itc->itc_set_first = 0;
	    QST_INT (inst, ts->src_gen.src_out_fill) = 0;
	    ks_set_dfg_queue (ts->ts_order_ks, inst, order_itc);
	    itc_vec_new_results (order_itc);	/* before reenter, could in principle be placeholders to unregister */
	    order_buf = page_reenter_excl (order_itc);
	    rdbg_printf_if (enable_cr_trace, ("%p reenter L=%d pos=%d col_row=%d\n", order_itc, order_itc->itc_page,
		    order_itc->itc_map_pos, order_itc->itc_col_row));
	    itc_vec_next (order_itc, &order_buf);
	    order_itc->itc_rows_selected += order_itc->itc_n_results;
	    need_leave = 1;
	  results:
	    if (order_itc->itc_n_results == order_itc->itc_batch_size)
	      {
		itc_register_and_leave (order_itc, order_buf);
	      }
	    else
	      {
		if (need_leave)
		  itc_page_leave (order_itc, order_buf);
		QST_INT (inst, csm->csm_at_end) = 1;
		ts_check_batch_sz (ts, inst, order_itc);
	      }
	    QST_INT (inst, csm->csm_mode) = SG_CHECK_G_MIX;
	    if (order_itc->itc_n_results)
	      {
		qn_ts_send_output ((data_source_t *) ts, inst, ts->ts_after_join_test);
		state = NULL;
		goto again;
	      }
	    else if (csm->csm_cset_scan_state)
	      {
		ts_at_end (ts, inst);
		ts_empty_cset_scan (ts, inst);
		return;
	      }
	  }
	  ITC_FAILED
	  {
	  }
	  END_FAIL (order_itc);
	}
    }


}
