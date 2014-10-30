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
  for (inx = last_unmatched; inx < n_reqd; inx++)
    csp[inx] = gethash ((void *) reqd_ps[inx], &cset->cset_p);
  for (inx = 0; inx < itc->itc_range_fill; inx++)
    {
      if (itc->itc_ranges[inx].r_first == itc->itc_ranges[inx].r_end)
	{
	  iri_id_t s = ((iri_id_t *) s_dc->dc_values)[itc->itc_param_order[inx + col_first]];
	  uint64 h = 1, mask;
	  MHASH_STEP_1 (h, s);
	  mask = BF_MASK (h);
	  for (nth_p = 0; nth_p < n_reqd; nth_p++)
	    {
	      uint32 sz = csp[inx]->csetp_n_bloom;
	      uint64 w = csp[inx]->csetp_bloom[BF_WORD (h, sz)];
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
  int last, bytes;
  if (s_dc)
    s_dc->dc_n_values = 0;
  if (itc->itc_n_matches)
    last = itc->itc_matches[itc->itc_n_matches - 1];
  else
    last = itc->itc_ranges[itc->itc_range_fill - 1].r_end;
  if (COL_NO_ROW == last)
    last = itc_rows_in_seg (itc, buf);
  bytes = last * csm->csm_cset_bit_bytes;
  qi_ensure_array (itc->itc_out_state, &inst[csm->csm_exc_tmp_bits], last * csm->csm_cset_bit_bytes + 8, 0);
  arr = QST_BOX (caddr_t, inst, csm->csm_exc_tmp_bits);
  itc->itc_cset_bits = arr;
  int64_fill ((int64 *) arr, (int64) - 1, ALIGN_8 (bytes) / 8);
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
itc_cset_except (it_cursor_t * itc, buffer_desc_t * buf, int prev_matches, int rows_in_seg)
{
  /* itc_matches has the pre-filter matches starting at itc_n_matches.  If prev_matches is 0, then the prev is the itc_ranges for the set. *
   * matches is after filtering on col.  If a row was in prev and not in matches, put it back in matches if s can have exception. */
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_mode_t *csm = ts->ts_csm;
  search_spec_t *sp = itc->itc_col_spec;
  int nth_bit = sp->sp_ordinal;
  cset_p_t *csetp = sp->sp_col->col_csetp;
  row_no_t *pre, *post;
  int inx, n_post, fill = 0, nth_match = 0, point = 0;
  int n_matches = itc->itc_match_out;
  if (!csetp || !csetp->csetp_n_bloom)
    return;
  pre = &itc->itc_matches[itc->itc_n_matches];
  post = itc->itc_matches;
  n_post = itc->itc_match_out;
  for (inx = 0; inx < prev_matches; inx++)
    {
      row_no_t prev_row = pre[inx];
      if (nth_match >= n_matches || post[nth_match] != prev_row)
	{
	  iri_id_t s = itc_seg_row_s (itc, buf, prev_row, prev_matches, &point);
	  uint64 mask, w, h = 1;
	  MHASH_STEP_1 (h, s);
	  mask = BF_MASK (h);
	  w = csetp->csetp_bloom[BF_WORD (h, csetp->csetp_n_bloom)];
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
  data_source_t *qn;
  int inx;
  DO_BOX (state_slot_t *, ssl, inx, qr->qr_vec_ssls)
  {
    data_col_t *dc = QST_BOX (data_col_t *, inst, ssl->ssl_index);
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
      qr = csg_query (cset, ts, mode, &err);
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
      int n = box_length (csa->csa_ssls) / sizeof (cset_align_t), inx;
      for (inx = 0; inx < n; inx++)
	{
	  cset_align_t *csal = &csa->csa_ssls[inx];
	  state_slot_t *val = csal->csa_second ? csal->csa_second : csal->csa_first;
	  if (!csal->csa_res)
	    continue;
	  vec_ssl_assign (inst, csal->csa_res, val);
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
	  iri_id_t s = ((iri_id_t *) s_dc->dc_values)[set];
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
    return 1;
  if (!csetp->csetp_n_bloom)
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
      w = csetp->csetp_bloom[BF_WORD (h, csetp->csetp_n_bloom)];
      if (mask == (w & mask))
	nos[fill++] = inx;
    }
  itc->itc_n_sets = fill;
  return 1;
}


void
itc_cset_s_param_nos (it_cursor_t * itc)
{
  int *nos = itc->itc_param_order;
  int inx, fill = 0;
  table_source_t *ts = itc->itc_ks->ks_ts;
  cset_t *cset = ts->ts_csm->csm_cset;
  data_col_t *s_dc = ITC_P_VEC (itc, 0);
  iri_id_t *s_arr = (iri_id_t *) s_dc->dc_values;
  for (inx = 0; inx < s_dc->dc_n_values; inx++)
    {
      iri_id_t s = s_arr[inx];
      if (IRI_RANGE (s) == cset->cset_id)
	nos[fill++] = inx;
    }
  itc->itc_n_sets = fill;
  return 1;
}


/* csm mode in psog for cset exceptions */
#define PSOG_CSET_INIT 0
#define PSOG_CSET_VALUES 1
#define PSOG_OWN_VALUES 2
#define PSOG_OUTER_NULLS 3



void
cset_psog_cset_values (table_source_t * ts, caddr_t * inst)
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
  uint64 *out = (uint64 *) out_dc->dc_values;
  DC_CHECK_LEN (out_dc, n_sets - 1);
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
	  sslr_n_consec_ref (inst, (state_slot_ref_t *) csm->csm_exc_bits_in, sets, set, last);
	  for (r = 0; r < last - set; r++)
	    {
	      if (this_mask & bits[sets[r]])
		{
		  out[out_fill++] = bits[sets[r]] & ~this_mask;
		  if (o_dc)
		    ssl_mv_nn (o_dc, cset_dc, sets[r]);
		  qn_result ((data_source_t *) ts, inst, set + r);
		}
	    }
	}
    }
  else
    {
      for (r = 0; r < n_sets; r++)
	{
	  if (this_mask & bits[r])
	    {
	      out[out_fill++] = bits[r] & ~this_mask;
	      if (o_dc)
		ssl_mv_nn (o_dc, cset_dc, r);
	      qn_result ((data_source_t *) ts, inst, r);
	    }
	}
    }
#if 0				/* done in caller */
  if (batch_size == QST_INT (inst, ts->src_gen.src_out_fill))
    {
      SRC_IN_STATE (ts, inst) = inst;
      QST_INT (inst, csm->csm_mode) = PSOG_OWN_VALUES;
      qn_ts_send_output ((data_source_t *) ts, inst, NULL);
    }
#endif
}


void
psog_outer_nulls (table_source_t * ts, caddr_t * inst, it_cursor_t * itc)
{
  /* set a null o for the sets with no output from this */
  int this_bit = -1, set;
  data_col_t *o_dc = NULL;
  data_col_t *exc_bits_dc = NULL;
  state_slot_t *exc_ssl = NULL;
  uint64 *exc_bits = NULL;
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  db_buf_t set_card = QST_BOX (db_buf_t, inst, ts->ts_set_card_bits);
  cset_mode_t *csm = ts->ts_csm;
  int last_outer = QST_INT (inst, csm->csm_last_outer);
  int batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
  if (-1 != csm->csm_bit)
    {
      this_bit = csm->csm_bit % 64;
      exc_ssl = csm->csm_exc_bits_in[csm->csm_bit / 64];
      exc_bits_dc = QST_BOX (data_col_t *, inst, exc_ssl->ssl_index);
      exc_bits = (uint64 *) exc_bits_dc->dc_values;
    }
  if (csm->csm_rq_o)
    o_dc = QST_BOX (data_col_t *, inst, csm->csm_rq_o->ssl_index);
  for (set = 0; set < last_outer; set++)
    {
      if (batch_size == QST_INT (inst, ts->src_gen.src_out_fill))
	{
	  QST_INT (inst, csm->csm_last_outer) = set;
	  SRC_IN_STATE (ts, inst) = inst;
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
      if (batch_size == QST_INT (inst, ts->src_gen.src_batch_size))
	{
	  QST_INT (inst, csm->csm_last_outer) = set;
	  SRC_IN_STATE (ts, inst) = inst;
	  qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	  itc_vec_new_results (itc);
	  batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
	}
    }
  SRC_IN_STATE (ts, inst) = NULL;
  QST_INT (inst, csm->csm_mode) = 0;
  if (QST_INT (inst, ts->src_gen.src_out_fill))
    qn_ts_send_output ((data_source_t *) ts, inst, NULL);
}


void
cset_psog_input (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill), r;
  cset_mode_t *csm = ts->ts_csm;
  int order_buf_preset = 0;
  query_instance_t *qi = (query_instance_t *) inst;
  int rc, start;
  int batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
  if (state)
    QST_INT (inst, csm->csm_mode) = PSOG_CSET_INIT;
  if (state && -1 != csm->csm_bit)
    {
      /* first pass through values for this col where filled in from cset */
      int n_out;
      cset_psog_cset_values (ts, inst);
      n_out = QST_INT (inst, ts->src_gen.src_out_fill);
      if (n_out == batch_size)
	{
	  SRC_IN_STATE (ts, inst) = inst;
	  QST_INT (inst, csm->csm_mode) = PSOG_CSET_VALUES;
	  qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	}
    }
  if (PSOG_CSET_VALUES == QST_INT (inst, csm->csm_mode))
    {
      ks_vec_new_results (ts->ts_order_ks, inst, NULL);
      state = inst;
    }
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
  if (!state && PSOG_OUTER_NULLS == QST_INT (inst, csm->csm_mode))
    {
      psog_outer_nulls (ts, inst, TS_ORDER_ITC (ts, inst));
      return;
    }
  if (!state && ts_stream_flush_ck (ts, inst))
    return;
  for (;;)
    {
      buffer_desc_t *order_buf = NULL;
      it_cursor_t *order_itc;
      if (!state)
	{
	  start = 0;
	  state = SRC_IN_STATE (ts, inst);
	  if (!state)
	    return;
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
	  if (2 == rc)
	    {
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;		/* was a sdfg and ks start search did the work to completion */

	    }
	  if (!rc)
	    {
	      ts_at_end (ts, inst);
	      if (ts->ts_aq)
		ts_aq_handle_end (ts, inst);
	      ts_check_batch_sz (ts, inst, order_itc);
	      if (ts->ts_is_outer)
		psog_outer_nulls (ts, inst, order_itc);
	      if (order_itc->itc_batch_size && order_itc->itc_n_results)
		{
		  qn_ts_send_output ((data_source_t *) ts, inst, ts->ts_after_join_test);
		  ts_stream_flush_ck (ts, inst);
		  ts_aq_final (ts, inst, order_itc);
		  return;
		}
	      ts_aq_final (ts, inst, NULL);
	      return;
	    }
	  if (ts->ts_need_placeholder)
	    ts_set_placeholder (ts, inst, order_itc, &order_buf);
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
	    if (ts->ts_order_ks->ks_vec_source)
	      {
		ks_set_dfg_queue (ts->ts_order_ks, inst, order_itc);
		itc_vec_new_results (order_itc);	/* before reenter, could in principle be placeholders to unregister */
	      }
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
		if (ts->ts_need_placeholder)
		  ts_set_placeholder (ts, inst, order_itc, &order_buf);
		itc_register_and_leave (order_itc, order_buf);
	      }
	    else
	      {
		itc_page_leave (order_itc, order_buf);
		ts_at_end (ts, inst);
		if (ts->ts_aq)
		  ts_aq_handle_end (ts, inst);
		ts_check_batch_sz (ts, inst, order_itc);
		if (ts->ts_is_outer)
		  psog_outer_nulls (ts, inst, order_itc);
		if (order_itc->itc_n_results)
		  {
		    qn_ts_send_output ((data_source_t *) ts, inst, ts->ts_after_join_test);
		  }
		ts_stream_flush_ck (ts, inst);
		ts_aq_final (ts, inst, NULL);
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
  mode[3] = CMP_NONE == sp->sp_min_op ? NULL : qst_get (inst, sp->sp_min_ssl);
  mode[4] = box_num (sp->sp_max_op);
  mode[5] = CMP_NONE == sp->sp_max_op ? NULL : qst_get (inst, sp->sp_max_ssl);
  return (caddr_t) mode;
}


#define NO_COND ((caddr_t)-1)

void
posg_special_o (table_source_t * ts, caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  key_source_t *ks = ts->ts_order_ks;
  iri_id_t p = unbox_iri_id (qst_get (inst, ks->ks_spec.ksp_spec_array->sp_min_ssl));
  int set, n_sets, last_set;
  it_cursor_t *itc;
  dk_set_t csps;
  search_spec_t *o_spec = ks->ks_spec.ksp_spec_array->sp_next;
  cset_mode_t *csm = ts->ts_csm;
  csps = QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos);
  if (-1 == (ptrlong) csps)
    {
      csps = (dk_set_t) gethash ((void *) p, &p_to_csetp_list);
      QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) = csps;
      last_set = QST_INT (inst, csm->csm_posg_last_set) = 0;
    }
  else
    last_set = QST_INT (inst, csm->csm_posg_last_set);
  itc = TS_ORDER_ITC (ts, inst);
  if (itc)
    n_sets = itc->itc_n_sets;
  else
    n_sets = ts->src_gen.src_prev ? QST_INT (inst, ts->src_gen.src_prev->src_out_fill) : qi->qi_n_sets;

  while (csps)
    {
      cset_p_t *csp = (cset_p_t *) csps->data;
      if (!csp->csetp_index_o || csp->csetp_non_index_o)
	{
	  for (set = last_set; set < n_sets; set++)
	    {
	      caddr_t min_o, max_o;
	      qi->qi_set = set;
	      min_o = o_spec->sp_min_ssl ? qst_get (inst, o_spec->sp_min_ssl) : NO_COND;
	      max_o = o_spec->sp_max_ssl ? qst_get (inst, o_spec->sp_max_ssl) : NO_COND;
	      if (CMP_EQ == o_spec->sp_min_op && csp->csetp_non_index_o && !id_hash_get (csp->csetp_non_index_o, (caddr_t) & min_o))
		continue;
	      QST_INT (inst, csm->csm_posg_last_set) = set + 1;
	      qst_set (inst, csm->csm_o_scan_mode, cset_scan_mode (csp, inst, o_spec, set));
	      qn_result ((data_source_t *) ts, inst, set);
	      SRC_IN_STATE (ts, inst) = csps->next ? inst : NULL;
	      qn_ts_send_output ((data_source_t *) ts, inst, NULL);
	      ks_vec_new_results (ks, inst, NULL);
	    }
	}
      csps = csps->next;
      QST_BOX (dk_set_t, inst, csm->csm_posg_cset_pos) = csps;
    }
  SRC_IN_STATE (ts, inst) = NULL;
}
