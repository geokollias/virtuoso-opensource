/*
*  csetins.c
*
*  $Id$
*
*  Characteristic set insert
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
#include "mhash.h"


void itc_cset_upd_cols (it_cursor_t * itc, buffer_desc_t * buf, int *match_sets, int rows_in_seg, insert_node_t * ins);


void
dv_array_iter (dv_array_iter_t * dvai, db_buf_t dv)
{
  if (DV_DB_NULL == dv[0])
    dvai->dvai_n = -2;
  else if (DV_ARRAY_OF_POINTER == dv[0])
    {
      dtp_t ign;
      int n = dv_int (dv + 1, &ign);
      if (DV_SHORT_INT == dv[1])
	dv += 2;
      else
	dv += 5;
      dvai->dvai_dv = dv;
      dvai->dvai_pos = 0;
      dvai->dvai_n = n;
    }
  else
    {
      dvai->dvai_dv = dv;
      dvai->dvai_n = -1;
    }
}


db_buf_t
dvai_next (dv_array_iter_t * dvai)
{
  int len;
  db_buf_t dv;
  if (-2 == dvai->dvai_n)
    return NULL;
  if (!dvai->dvai_n)
    return NULL;
  if (-1 == dvai->dvai_n)
    {
      dvai->dvai_n = -2;
      return dvai->dvai_dv;
    }
  else if (dvai->dvai_pos == dvai->dvai_n)
    return NULL;
  dv = dvai->dvai_dv;
  DB_BUF_TLEN (len, dv[0], dv);
  dvai->dvai_dv += len;
  dvai->dvai_pos++;
  return dv;
}


void
itc_add_cset_quad (col_pos_t * cpo, row_delta_t * rd, int rdinx, insert_node_t * ins)
{
  it_cursor_t *itc = cpo->cpo_itc;

  caddr_t *inst = itc->itc_out_state;

  dbe_column_t *col = (dbe_column_t *) cpo->cpo_cmp_min;
  int inx_o = col->col_csetp->csetp_index_o;
  data_col_t *sdc = QST_BOX (data_col_t *, inst, ins->ins_cset_psog[0]->ssl_index);
  data_col_t *pdc = QST_BOX (data_col_t *, inst, ins->ins_cset_psog[1]->ssl_index);
  data_col_t *odc = QST_BOX (data_col_t *, inst, ins->ins_cset_psog[2]->ssl_index);
  data_col_t *gdc = QST_BOX (data_col_t *, inst, ins->ins_cset_psog[3]->ssl_index);
  dc_append_int64 (pdc, col->col_csetp->csetp_iri);
  dc_append_int64 (sdc, unbox_iri_id (rd->rd_values[1]));
  dc_append_int64 (gdc, unbox_iri_id (rd->rd_values[0]));
  if (DV_ANY == col->col_sqt.sqt_col_dtp)
    {
      db_buf_t dv = (db_buf_t) rd->rd_values[rdinx];
      int len;
      DB_BUF_TLEN (len, *dv, dv);
      dc_append_bytes (odc, dv, len, NULL, 0);
    }
  if (inx_o)
    {
      data_col_t *sdc = QST_BOX (data_col_t *, inst, ins->ins_cset_posg[0]->ssl_index);
      data_col_t *pdc = QST_BOX (data_col_t *, inst, ins->ins_cset_posg[1]->ssl_index);
      data_col_t *odc = QST_BOX (data_col_t *, inst, ins->ins_cset_posg[2]->ssl_index);
      data_col_t *gdc = QST_BOX (data_col_t *, inst, ins->ins_cset_posg[3]->ssl_index);
      dc_append_int64 (pdc, col->col_csetp->csetp_iri);
      dc_append_int64 (sdc, unbox_iri_id (rd->rd_values[1]));
      dc_append_int64 (gdc, unbox_iri_id (rd->rd_values[0]));
      if (DV_ANY == col->col_sqt.sqt_col_dtp)
	{
	  db_buf_t dv = (db_buf_t) rd->rd_values[rdinx];
	  int len;
	  DB_BUF_TLEN (len, *dv, dv);
	  dc_append_bytes (odc, dv, len, NULL, 0);
	}

    }
}


#define TRAPNC { if ((1004 == itc->itc_insert_key->key_id && 2862 ==  itc->itc_range_fill) \
		     || (1001 == itc->itc_insert_key->key_id && 2 ==  itc->itc_range_fill)) \
bing (); }

int
ce_ins_ck (col_pos_t * cpo, int row, dtp_t flags, db_buf_t val, int len, int64 offset, int rl)
{
  /* for integers, offset is the number.  For anys val is the 1st byte of the any and offset is the tail offset */
  int *match_sets = (int *) cpo->cpo_min_spec;
  int ctr;
  int rc, next;
  int nth_col = cpo->cpo_cl->cl_nth;
  it_cursor_t *itc = cpo->cpo_itc;
  int rdinx = cpo->cpo_cl->cl_nth - itc->itc_insert_key->key_n_significant;
  int nth = itc->itc_match_in;
  for (;;)
    {
      row_delta_t *rd = itc->itc_vec_rds[itc->itc_param_order[match_sets[nth]]];
      caddr_t rd_val = rd->rd_values[rdinx];
      dtp_t dtp = DV_TYPE_OF (rd_val);
      if (DV_ANY == cpo->cpo_cl->cl_sqt.sqt_col_dtp ? (DV_DB_NULL == ((db_buf_t) rd_val)[0]) : DV_DB_NULL == DV_TYPE_OF (rd_val))
	{
	  TRAPNC;
	  rd->rd_values[rdinx] = COL_UPD_NO_CHANGE;
	}
      else
	{
	  rc = ce_col_cmp (val, offset, flags, cpo->cpo_cl, rd_val);
	  if (DVC_UNKNOWN == rc)
	    ;
	  else if (DVC_MATCH == rc)
	    {
	      TRAPNC;
	      rd->rd_values[rdinx] = COL_UPD_NO_CHANGE;
	    }
	  else
	    {
	      itc_add_cset_quad (cpo, rd, rdinx, (insert_node_t *) cpo->cpo_dc);
	    }
	}
      if (++itc->itc_match_in == itc->itc_n_matches)
	return CE_AT_END;
      next = itc->itc_matches[itc->itc_match_in];
      nth++;
      if (next < rl + row)
	continue;
      return next;
    }
}


void
itc_check_cset_quad (it_cursor_t * itc, insert_node_t * ins, int min)
{
  caddr_t err = NULL;
  caddr_t *params;
  caddr_t *inst = itc->itc_out_state;
  QNCAST (QI, qi, inst);
  if (IS_QN (ins, insert_node_input))
    {
      data_col_t *sdc = QST_BOX (data_col_t *, inst, ins->ins_cset_psog[0]->ssl_index);
      if (sdc->dc_n_values > min)
	{
	  static query_t *ins_psog;
	  static query_t *ins_posg;
	  static query_t *ins_op;
	  if (!ins_psog || !ins_posg || !ins_op)
	    {
	      ins_psog =
		  sql_compile ("insert soft rdf_quad index rdf_quad (p, s,o,g) values (?, ?, ?, ?)", qi->qi_client, &err,
		  SQLC_DEFAULT);
	      if (err)
		sqlr_resignal (err);
	      ins_posg =
		  sql_compile ("insert soft rdf_quad index rdf_quad_posg (p, o, s, g) values (?, ?, ?, ?)", qi->qi_client, &err,
		  SQLC_DEFAULT);
	      if (err)
		sqlr_resignal (err);
	      ins_op =
		  sql_compile ("insert soft rdf_quad index rdf_quad_op (o, p) values (?, ?)", qi->qi_client, &err, SQLC_DEFAULT);
	      if (err)
		sqlr_resignal (err);
	    }
	  params = (caddr_t *) list (4, QST_BOX (data_col_t *, inst, ins->ins_cset_psog[0]->ssl_index),
	      QST_BOX (data_col_t *, inst, ins->ins_cset_psog[1]->ssl_index),
	      QST_BOX (data_col_t *, inst, ins->ins_cset_psog[2]->ssl_index),
	      QST_BOX (data_col_t *, inst, ins->ins_cset_psog[3]->ssl_index));
	  err = qr_exec (qi->qi_client, ins_psog, CALLER_LOCAL, "", NULL, NULL, params, NULL, 0);
	  if (err)
	    sqlr_resignal (err);
	  params[0] = QST_BOX (caddr_t, inst, ins->ins_cset_posg[0]->ssl_index);
	  params[1] = QST_BOX (caddr_t, inst, ins->ins_cset_posg[1]->ssl_index);
	  params[2] = QST_BOX (caddr_t, inst, ins->ins_cset_posg[2]->ssl_index);
	  params[3] = QST_BOX (caddr_t, inst, ins->ins_cset_posg[3]->ssl_index);
	  err = qr_exec (qi->qi_client, ins_posg, CALLER_LOCAL, "", NULL, NULL, params, NULL, 0);
	  dk_free_box ((caddr_t) params);
	  if (err)
	    sqlr_resignal (err);
	  params = list (2, QST_BOX (caddr_t, inst, ins->ins_cset_posg[1]->ssl_index),
	      QST_BOX (caddr_t, inst, ins->ins_cset_posg[0]->ssl_index));
	  err = qr_exec (qi->qi_client, ins_posg, CALLER_LOCAL, "", NULL, NULL, params, NULL, 0);
	  dk_free_box ((caddr_t) params);
	  if (err)
	    sqlr_resignal (err);
	}
    }
  else
    {
      update_node_t *upd = (update_node_t *) ins;
      data_col_t *sdc = QST_BOX (data_col_t *, inst, upd->upd_cset_psog[1]->ssl_index);
      if (sdc->dc_n_values > min)
	{
	  static query_t *del_psog;
	  static query_t *del_posg;
	  if (!del_psog || !del_posg)
	    {
	      del_psog =
		  sql_compile
		  ("delete from rdf_quad table option (index rdf_quad) where p = ? and s = ? and o = ? and g = ? option (index rdf_quad, index only)",
		  qi->qi_client, &err, SQLC_DEFAULT);
	      if (err)
		sqlr_resignal (err);
	      del_posg =
		  sql_compile
		  ("delete from rdf_quad table option (index rdf_quad_pogs) where p = ? and s = ? and o = ? and g = ? option (index rdf_quad_pogs, index only)",
		  qi->qi_client, &err, SQLC_DEFAULT);
	      if (err)
		sqlr_resignal (err);
	    }
	  params = (caddr_t *) list (4, QST_BOX (data_col_t *, inst, upd->upd_cset_psog[0]->ssl_index),
	      QST_BOX (data_col_t *, inst, upd->upd_cset_psog[1]->ssl_index),
	      QST_BOX (data_col_t *, inst, upd->upd_cset_psog[2]->ssl_index),
	      QST_BOX (data_col_t *, inst, upd->upd_cset_psog[3]->ssl_index));
	  err = qr_exec (qi->qi_client, del_psog, CALLER_LOCAL, "", NULL, NULL, params, NULL, 0);
	  if (err)
	    sqlr_resignal (err);

	  params[0] = QST_BOX (caddr_t, inst, upd->upd_cset_psog[0]->ssl_index);
	  params[1] = QST_BOX (caddr_t, inst, upd->upd_cset_psog[1]->ssl_index),
	      params[2] = QST_BOX (caddr_t, inst, upd->upd_cset_psog[2]->ssl_index),
	      params[3] = QST_BOX (caddr_t, inst, upd->upd_cset_psog[3]->ssl_index);
	  err = qr_exec (qi->qi_client, del_posg, CALLER_LOCAL, "", NULL, NULL, params, NULL, 0);
	  dk_free_box ((caddr_t) params);
	  if (err)
	    sqlr_resignal (err);
	}
    }
}


extern int col_ins_error;
extern int dbf_col_ins_dbg_log;


void
itc_ck_no_no_chg (it_cursor_t * itc, int up_to)
{
  int set, inx;
  for (set = itc->itc_set; set < up_to; set++)
    {
      row_delta_t *rd = itc->itc_vec_rds[itc->itc_param_order[set]];
      for (inx = 0; inx < rd->rd_n_values; inx++)
	{
	  if (COL_UPD_NO_CHANGE == rd->rd_values[inx])
	    GPF_T1 ("rd with a col with no change should hae been remd");
	}
    }
}


int
itc_cset_update (it_cursor_t * itc, buffer_desc_t * buf, insert_node_t * ins)
{
  int first_set = itc->itc_col_first_set, fill = 0, nth_match = 0, range_fill, n_matches;
  row_no_t point = 0, next = 0;
  col_row_lock_t *clk = NULL;
  int n_dropped = 0;
  caddr_t *inst = itc->itc_out_state;
  int *match_sets;
  int rows_in_seg = -1;
  int inx;
  itc_ck_no_no_chg (itc, itc->itc_set + itc->itc_range_fill);
  for (inx = 0; inx < itc->itc_range_fill; inx++)
    {
      if (inx && itc->itc_ranges[inx].r_first < itc->itc_ranges[inx - 1].r_first)
	{
	  col_ins_error = 1;
	  if (dbf_col_ins_dbg_log)
	    {
	      log_error ("insert of col-wise inx would be out of order, exiting.  Key %s slice %d", itc->itc_insert_key->key_name,
		  itc->itc_tree->it_slice);
	      itc_col_dbg_log (itc);
	      return 0;
	    }
	  return 0;
	  GPF_T1 ("ranges to insert must have a non-decreasing r_first");
	}
      if (itc->itc_ranges[inx].r_first != itc->itc_ranges[inx].r_end)
	{
	  if (COL_NO_ROW == itc->itc_ranges[inx].r_end)
	    itc->itc_ranges[inx].r_end = itc->itc_ranges[inx].r_first + 1;	/* eq on last of seg has end at col no row, means one row */
	  if (1 != itc->itc_ranges[inx].r_end - itc->itc_ranges[inx].r_first)
	    {
	      col_ins_error = 1;
	      log_error ("insert in cset has a matched range of more than 1 rows on unique sg");
	      return 0;
	    }
	  if (itc_is_own_del_clk (itc, itc->itc_ranges[inx].r_first, &clk, &point, &next))
	    {
	      clk->clk_change &= ~CLK_DELETE_AT_COMMIT;
	    }
	  if (-1 == rows_in_seg)
	    {
	      rows_in_seg = cr_n_rows (itc->itc_col_refs[0]);
	      if (!itc->itc_matches || box_length (itc->itc_matches) / sizeof (int) < itc->itc_range_fill)
		{
		  dk_free_box (itc->itc_matches);
		  itc->itc_matches = (row_no_t *) itc_alloc_box (itc, sizeof (row_no_t) * (1000 + itc->itc_range_fill), DV_BIN);
		}
	      QN_CHECK_SETS (ins, inst, itc->itc_range_fill);
	      match_sets = QST_BOX (int *, inst, ins->src_gen.src_sets);
	      itc->itc_match_out = 0;
	    }
	  match_sets[itc->itc_match_out] = first_set + inx;
	  itc->itc_matches[itc->itc_match_out++] = itc->itc_ranges[inx].r_first;
	}
    }
  if (-1 == rows_in_seg)
    return 0;
  n_matches = itc->itc_n_matches = itc->itc_match_out;
  range_fill = itc->itc_range_fill;
  itc_cset_upd_cols (itc, buf, match_sets, rows_in_seg, ins);
  nth_match = 0;
  for (inx = 0; inx < range_fill; inx++)
    {
      if (nth_match < n_matches && match_sets[nth_match] == first_set + inx)
	{
	  n_dropped++;
	  nth_match++;
	  continue;
	}
      itc->itc_param_order[first_set + fill++] = itc->itc_param_order[first_set + inx];
    }
  if (itc->itc_n_sets - first_set - range_fill < 0)
    GPF_T1 ("itc sets ranges bad after cset ins update ck");
  memmove_16 (&itc->itc_param_order[first_set + fill], &itc->itc_param_order[first_set + range_fill],
      sizeof (int) * (itc->itc_n_sets - first_set - range_fill));
  itc->itc_n_sets -= n_dropped;
  itc_ck_no_no_chg (itc, MIN (itc->itc_set + range_fill, itc->itc_n_sets));
  itc_check_cset_quad (itc, ins, 100000);
  itc->itc_range_fill = 0;
  return 1;
}


void
upd_add_del_quad (update_node_t * upd, table_source_t * ts, caddr_t * inst, int set_in_ts, int nth_col)
{
  /* the o is either a null, for no action, a single non array dv for single quad or an dv array for many quads */
  key_source_t *ks = ts->ts_order_ks;
  dv_array_iter_t dvai;
  data_col_t *s_dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[0]->ssl_index);
  dbe_key_t *key = upd->upd_table->tb_primary_key;
  int n_parts = key->key_n_significant;
  data_col_t *g_dc = QST_BOX (data_col_t *, inst, ks->ks_vec_source[1]->ssl_index);
  dbe_column_t *col = (dbe_column_t *) gethash (upd->upd_col_ids[nth_col], wi_inst.wi_schema->sc_id_to_col);
  iri_id_t p = col->col_csetp->csetp_iri;
  data_col_t *o_dc = QST_BOX (data_col_t *, inst, upd->upd_vec_source[nth_col]->ssl_index);
  db_buf_t o = ((db_buf_t *) o_dc->dc_values)[set_in_ts];
  dv_array_iter (&dvai, o);
  data_col_t *psog_p = QST_BOX (data_col_t *, inst, upd->upd_cset_psog[0]->ssl_index);
  data_col_t *psog_s = QST_BOX (data_col_t *, inst, upd->upd_cset_psog[1]->ssl_index);
  data_col_t *psog_o = QST_BOX (data_col_t *, inst, upd->upd_cset_psog[2]->ssl_index);
  data_col_t *psog_g = QST_BOX (data_col_t *, inst, upd->upd_cset_psog[3]->ssl_index);
  db_buf_t o_dv;
  while (o_dv = dvai_next (&dvai))
    {
      int len;
      dc_append_int64 (psog_p, p);
      dc_append_int64 (psog_s, ((int64 *) s_dc->dc_values)[set_in_ts]);
      dc_append_int64 (psog_g, ((int64 *) g_dc->dc_values)[set_in_ts]);
      DB_BUF_TLEN (len, o_dv[0], o_dv);
      dc_append_bytes (psog_o, o_dv, len, NULL, 0);
    }
}



int
upd_cset_set_in_ts (update_node_t * upd, caddr_t * inst, table_source_t ** ts_ret, int set_no)
{
  /* get the set no in the params for the this set no in the upd */
  QNCAST (table_source_t, ts, upd->src_gen.src_prev);
  *ts_ret = ts;
  return QST_BOX (int *, inst, ts->src_gen.src_sets)[set_no];
}


int
ce_del_ck (col_pos_t * cpo, int row, dtp_t flags, db_buf_t val, int len, int64 offset, int rl)
{
  /* for integers, offset is the number.  For anys val is the 1st byte of the any and offset is the tail offset */
  int ctr;
  int rc, next;
  dv_array_iter_t dvai;
  update_node_t *upd = (update_node_t *) cpo->cpo_dc;
  table_source_t *ts = NULL;
  int nth_col = cpo->cpo_cl->cl_nth;
  it_cursor_t *itc = cpo->cpo_itc;
  caddr_t *inst = itc->itc_out_state;
  int rdinx = cpo->cpo_cl->cl_nth;
  int nth = itc->itc_match_in;
  for (;;)
    {
      row_delta_t *rd = itc->itc_vec_rds[itc->itc_param_order[itc->itc_col_first_set + nth]];
      caddr_t rd_val = rd->rd_values[rdinx];
      dtp_t dtp = DV_TYPE_OF (rd_val);
      if (DV_ANY == cpo->cpo_cl->cl_sqt.sqt_col_dtp ? (DV_DB_NULL == ((db_buf_t) rd_val)[0]) : DV_DB_NULL == DV_TYPE_OF (rd_val))
	{
	  itc->itc_cset_null_ctr[itc->itc_match_in]++;
	  rd->rd_values[rdinx] = COL_UPD_NO_CHANGE;
	}
      else
	{
	  int any_match = 0;
	  db_buf_t dv;
	  dv_array_iter (&dvai, rd_val);
	  while (dv = dvai_next (&dvai))
	    {
	      rc = ce_col_cmp (val, offset, flags, cpo->cpo_cl, dv);
	      if (DVC_MATCH != rc)
		{
		  int set_no = itc->itc_match_in + itc->itc_cset_first_set;
		  int set_in_ts = upd_cset_set_in_ts (upd, inst, &ts, set_no);
		  upd_add_del_quad (upd, ts, inst, set_in_ts, rdinx);
		}
	      else
		any_match = 1;
	    }
	  if (!any_match)
	    rd->rd_values[rdinx] = COL_UPD_NO_CHANGE;
	  else
	    {
	      mem_pool_t *mp = itc->itc_upd_mp;
	      caddr_t nu = mp_alloc_box (mp, 2, DV_STRING);
	      nu[0] = DV_DB_NULL;
	      nu[1] = 0;
	      rd->rd_values[rdinx] = nu;
	      itc->itc_cset_null_ctr[itc->itc_match_in]++;
	    }
	}
      if (++itc->itc_match_in == itc->itc_n_matches)
	return CE_AT_END;
      next = itc->itc_matches[itc->itc_match_in];
      if (next < rl + row)
	continue;
      return next;
    }
}


void
itc_all_null_del (it_cursor_t * itc, buffer_desc_t * buf)
{
  dbe_key_t *key = itc->itc_insert_key;
  row_no_t point = 0, next = 0;
  int n_cols = key->key_n_parts - key->key_n_significant;
  int inx;
  for (inx = 0; inx < itc->itc_range_fill; inx++)
    {
      if (n_cols == itc->itc_cset_null_ctr[inx])
	{
	  int rdinx;
	  row_delta_t *rd;
	  col_row_lock_t *clk = itc_clk_at (itc, itc->itc_ranges[inx].r_first, &point, &next);
	  if (!clk || clk->clk_pos != itc->itc_ranges[inx].r_first)
	    continue;
	  clk->clk_change |= CLK_DELETE_AT_COMMIT;
	  rd = itc->itc_vec_rds[inx];
	  DO_BOX (caddr_t, ign, rdinx, rd->rd_values) rd->rd_values[rdinx] = COL_UPD_NO_CHANGE;
	  END_DO_BOX;
	}
    }
}


void
itc_cset_upd_cols (it_cursor_t * itc, buffer_desc_t * buf, int *match_sets, int rows_in_seg, insert_node_t * ins)
{
  dbe_key_t *key = itc->itc_insert_key;
  int n_keys = key->key_n_significant;
  int nth_page, r, target;
  col_data_ref_t *cr;
  row_range_t rng;
  int nth_col = 0;
  col_pos_t cpo;
  memzero (&cpo, sizeof (cpo));
  rng.r_first = itc->itc_matches[0];
  rng.r_end = itc->itc_matches[itc->itc_n_matches - 1] + 1;
  cpo.cpo_min_spec = (search_spec_t *) match_sets;
  cpo.cpo_dc = (data_col_t *) ins;
  cpo.cpo_range = &rng;
  cpo.cpo_value_cb = IS_QN (ins, insert_node_input) ? ce_ins_ck : ce_del_ck;
  cpo.cpo_itc = itc;
  DO_CL (cl, itc->itc_insert_key->key_row_var)
  {
    if (nth_col++ < key->key_n_significant)
      continue;
    itc->itc_match_in = 0;
    int row = 0, end = 0;
    target = itc->itc_matches[0];
    cpo.cpo_clk_inx = 0;
    cpo.cpo_cl = cl;
    cpo.cpo_cmp_min = (caddr_t) gethash ((void *) (ptrlong) cl->cl_col_id, wi_inst.wi_schema->sc_id_to_col);
    itc->itc_match_out = 0;
    cr = itc->itc_col_refs[cl->cl_nth - n_keys];
    if (!cr)
      cr = itc->itc_col_refs[cl->cl_nth - n_keys] = itc_new_cr (itc);
    if (!cr->cr_is_valid)
      itc_fetch_col (itc, buf, cl, 0, COL_NO_ROW);
    for (nth_page = 0; nth_page < cr->cr_n_pages; nth_page++)
      {
	int limit;
	page_map_t *pm = cr->cr_pages[nth_page].cp_map;
	for (r = 0 == nth_page ? cr->cr_first_ce * 2 : 0; r < pm->pm_count; r += 2)
	  {
	    int ce_rows = pm->pm_entries[r + 1];
	    if (row + ce_rows <= target)
	      {
		row += ce_rows;
		continue;
	      }
	    if (row >= cpo.cpo_range->r_end || row >= rows_in_seg)
	      goto next_spec;
	    cpo.cpo_ce_row_no = row;
	    cpo.cpo_string = cr->cr_pages[nth_page].cp_string + pm->pm_entries[r];
	    cpo.cpo_pm = pm;
	    cpo.cpo_pm_pos = r;
	    cpo.cpo_bytes = pm->pm_filled_to - pm->pm_entries[r];
	    limit = nth_page == cr->cr_n_pages - 1 ? cr->cr_limit_ce : pm->pm_count;
	    for (r = r; r < limit; r += 2)
	      row += pm->pm_entries[r + 1];
	    end = MIN (row, cpo.cpo_range[0].r_end);
	    target = cs_decode (&cpo, target, end);
	    if (target >= cpo.cpo_range[0].r_end)
	      goto next_spec;
	    goto next_page;
	  }
      next_page:;
      }
  next_spec:;
  }
  END_DO_CL;
  if (itc->itc_cset_null_ctr)
    itc_all_null_del (itc, buf);
  if (match_sets)
    {
      /* the update part of insert only has the rows where key matched */
      int inx;
      for (inx = 0; inx < itc->itc_n_matches; inx++)
	{
	  itc->itc_ranges[inx].r_first = itc->itc_ranges[match_sets[inx] - itc->itc_col_first_set].r_first;
	  itc->itc_ranges[inx].r_end = itc->itc_ranges[inx].r_first + 1;
	}
      itc->itc_range_fill = itc->itc_n_matches;
    }
  itc->itc_n_matches = 0;
  itc_col_insert_rows (itc, buf, 1);
}


void
upd_cset_delete_unmatched (update_node_t * upd, caddr_t * inst, it_cursor_t * itc)
{
  int *upd_sets;
  int *ts_out;
  int ts_n_out;
  data_col_t *first_dc;
  state_slot_t *first_ssl;
  table_source_t *ts = (table_source_t *) upd->src_gen.src_prev;
  int n_for_ts, inx;
  key_source_t *ks;
  it_cursor_t *order_itc;
  db_buf_t all_null;
  if (!IS_QN (ts, table_source_input_unique))
    sqlr_new_error ("CSDEI", "CCSDEI", "cset delete supposed to have ts right before upd");
  ks = ts->ts_order_ks;
  if (!ks->ks_vec_cast)
    GPF_T1 ("must have vec cast");
  first_ssl = ks->ks_vec_cast[0] ? ks->ks_vec_cast[0] : (state_slot_t *) ks->ks_vec_source[0];
  first_dc = QST_BOX (data_col_t *, inst, first_ssl->ssl_index);
  n_for_ts = first_dc->dc_n_values;
  QN_CHECK_SETS (upd, inst, n_for_ts);
  upd_sets = QST_BOX (int *, inst, upd->src_gen.src_sets);
  int_asc_fill (upd_sets, n_for_ts, 0);
  ts_out = QST_BOX (int *, inst, ts->src_gen.src_sets);
  ts_n_out = QST_INT (inst, ts->src_gen.src_out_fill);
  for (inx = 0; inx < ts_n_out; inx++)
    upd_sets[ts_out[inx]] = -1;
  /* now the sets in upd sets have no match in the cset.  These all become del from quad */
  for (inx = 0; inx < n_for_ts; inx++)
    {
      int col;
      if (-1 == upd_sets[inx])
	{
	  int n_cols = BOX_ELEMENTS (upd->upd_col_ids);
	  for (col = 0; col < n_cols; col++)
	    upd_add_del_quad (upd, ts, inst, upd_sets[inx], col);
	  itc_check_cset_quad (itc, (insert_node_t *) upd, 100000);
	}
    }
}


void
upd_cset_delete_rows (update_node_t * upd, it_cursor_t * itc, buffer_desc_t * buf, mem_pool_t * mp)
{
  /* itc is on page with vec rds set to new values.  A null becomes no change, a match becomes update to null, a non match becomes delete of quad */
  int inx;
  if (!itc->itc_matches || box_length (itc->itc_matches) / sizeof (row_no_t) < itc->itc_range_fill)
    {
      itc_free_box (itc, (caddr_t) itc->itc_matches);
      itc->itc_matches = itc_alloc_box (itc, sizeof (row_no_t) * (itc->itc_range_fill + 1000), DV_BIN);
      itc->itc_cset_null_ctr = mp_alloc_box (mp, box_length (itc->itc_ranges), DV_BIN);
    }
  for (inx = 0; inx < itc->itc_range_fill; inx++)
    itc->itc_matches[inx] = itc->itc_ranges[inx].r_first;
  memzero (itc->itc_cset_null_ctr, box_length (itc->itc_cset_null_ctr));
  itc->itc_n_matches = itc->itc_range_fill;
  itc->itc_upd_mp = mp;
  itc->itc_col_first_set = 0;
  itc_cset_upd_cols (itc, buf, NULL, -1, (insert_node_t *) upd);
  QR_RESET_CTX
  {
    itc_check_cset_quad (itc, (insert_node_t *) upd, 0);
  }
  QR_RESET_CODE
  {
    POP_QR_RESET;
    mp_free (mp);
    longjmp_splice (__self->thr_reset_ctx, reset_code);
  }
  END_QR_RESET;
}




void
csetp_set_bloom (iri_id_t * p_arr, iri_id_t * s_arr, int n_values)
{
  /* set the bloom for the p/s if these fall in a cset */
  cset_t *cset = NULL, *prev_cset = NULL;
  int prev_rn = -1;
  iri_id_t prev_p = 0;
  int inx;
  cset_p_t *csetp = NULL;
  for (inx = 0; inx < n_values; inx++)
    {
      iri_id_t p;
      iri_id_t s = s_arr[inx];
      int rn = IRI_RANGE (s);
      if (!rn)
	continue;
      if (rn != prev_rn)
	{
	  cset = rt_ranges[rn].rtr_cset;
	  prev_rn = rn;
	}
      if (cset)
	{
	  p = p_arr[inx];
	  if (cset != prev_cset || p != prev_p)
	    {
	      csetp = gethash ((void *) p, &cset->cset_p);
	      prev_p = p;
	      prev_cset = cset;
	    }
	  if (csetp)
	    {
	      uint64 h, mask;
	      MHASH_STEP_1 (h, s);
	      mask = BF_MASK (h);
	      mutex_enter (&csetp->csetp_bloom_mtx);
	      if (!csetp->csetp_bloom)
		{
		  csetp->csetp_bloom = dk_alloc_box_zero (8 * 211, DV_BIN);
		  csetp->csetp_n_bloom = 211;
		}
	      csetp->csetp_bloom[BF_WORD (h, csetp->csetp_n_bloom)] |= mask;
	      mutex_leave (&csetp->csetp_bloom_mtx);
	    }
	}
    }
}
