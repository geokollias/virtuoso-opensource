/*
 *  csetquad.c
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
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "security.h"
#include "sqlbif.h"
#include "sqltype.h"
#include "libutil.h"
#include "aqueue.h"
#include "mhash.h"
#include "date.h"
#include "arith.h"
#include "sqlcmps.h"



dk_set_t
ks_col_spec_list (key_source_t * ks, oid_t col_id)
{
  cset_quad_t *csq = ks->ks_ts->ts_csq;
  dk_set_t res = NULL;
  search_spec_t *sp;
  for (sp = ks->ks_spec.ksp_spec_array; sp; sp = sp->sp_next)
    if (col_id == sp->sp_cl.cl_col_id && !(csq->csq_s_min && sp->sp_min_ssl == csq->csq_s_min))
      t_set_push (&res, sp);
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    if (col_id == sp->sp_cl.cl_col_id)
      t_set_push (&res, sp);
  return dk_set_nreverse (res);
}

void
cs_ks_nn_test (key_source_t * cs_ks, dbe_column_t * col)
{
  t_NEW_VARZ (search_spec_t, sp);
  sp->sp_min_op = CMP_NON_NULL;
  sp->sp_cl = *cl_list_find (cs_ks->ks_key->key_row_var, col->col_id);
  ks_spec_add (&cs_ks->ks_row_spec, sp);
}


search_spec_t *
t_sp_copy (search_spec_t * sp)
{
  search_spec_t *cp = (search_spec_t *) t_alloc (sizeof (search_spec_t));
  *cp = *sp;
  cp->sp_next = NULL;
  return cp;
}

void
cs_ks_set_col_spec (key_source_t * cs_ks, char *col_name, dk_set_t sps)
{
  dbe_column_t *col = tb_name_to_column (cs_ks->ks_key->key_table, col_name);
  DO_SET (search_spec_t *, sp, &sps)
  {
    if (0 == col_name[1] && 'S' == col_name[0])
      {
	if (!cs_ks->ks_spec.ksp_spec_array)
	  {
	    cs_ks->ks_spec.ksp_spec_array = t_sp_copy (sp);
	    cs_ks->ks_spec.ksp_spec_array->sp_cl = *key_find_cl (cs_ks->ks_key, col->col_id);
	  }
	else
	  {
	    search_spec_t *cp = sp_copy (sp);
	    cp->sp_cl = *cl_list_find (cs_ks->ks_key->key_row_var, col->col_id);
	    cp->sp_next = cs_ks->ks_row_spec;
	    cs_ks->ks_row_spec = cp;
	  }
      }
    else if (0 == col_name[1] && 'G' == col_name[0])
      {
	if (cs_ks->ks_spec.ksp_spec_array && !cs_ks->ks_spec.ksp_spec_array->sp_next)
	  {
	    cs_ks->ks_spec.ksp_spec_array->sp_next = t_sp_copy (sp);
	    cs_ks->ks_spec.ksp_spec_array->sp_next->sp_cl = *key_find_cl (cs_ks->ks_key, col->col_id);
	  }
	else
	  {
	    search_spec_t *cp = sp_copy (sp);
	    cp->sp_cl = *cl_list_find (cs_ks->ks_key->key_row_var, col->col_id);
	    cp->sp_next = cs_ks->ks_row_spec;
	    cs_ks->ks_row_spec = cp;
	  }
      }
    else
      {
	search_spec_t *cp = sp_copy (sp);
	cp->sp_cl = *cl_list_find (cs_ks->ks_key->key_row_var, col->col_id);
	cp->sp_next = cs_ks->ks_row_spec;
	cs_ks->ks_row_spec = cp;
      }
  }
  END_DO_SET ();
}


void
cs_ks_set_key (key_source_t * cs_ks, cset_p_t * csetp)
{
  search_spec_t *sp;
  dbe_key_t *new_key = csetp->csetp_cset->cset_table->tb_primary_key;
  dbe_key_t *key = cs_ks->ks_key;
  dbe_column_t *s_col = (dbe_column_t *) key->key_parts->data;
  dbe_column_t *g_col = (dbe_column_t *) key->key_parts->next->data;
  dbe_column_t *new_s_col = (dbe_column_t *) new_key->key_parts->data;
  dbe_column_t *new_g_col = (dbe_column_t *) new_key->key_parts->next->data;
  cs_ks->ks_key = new_key;
  for (sp = cs_ks->ks_spec.ksp_spec_array; sp; sp = sp->sp_next)
    {
      if (s_col->col_id == sp->sp_cl.cl_col_id)
	sp->sp_cl = *key_find_cl (new_key, new_s_col->col_id);
      if (g_col->col_id == sp->sp_cl.cl_col_id)
	sp->sp_cl = *key_find_cl (new_key, new_g_col->col_id);
    }
  for (sp = cs_ks->ks_row_spec; sp; sp = sp->sp_next)
    {
      if (s_col->col_id == sp->sp_cl.cl_col_id)
	sp->sp_cl = *cl_list_find (new_key->key_row_var, new_s_col->col_id);
      else if (g_col->col_id == sp->sp_cl.cl_col_id)
	sp->sp_cl = *cl_list_find (new_key->key_row_var, new_g_col->col_id);
      else
	sp->sp_cl = *cl_list_find (new_key->key_row_var, csetp->csetp_col->col_id);
    }
}


void
cs_ks_set_out_cols (query_instance_t * qi, key_source_t * cs_ks, cset_p_t * csetp)
{
  state_slot_t *p_ssl = NULL;
  dbe_key_t *key = cs_ks->ks_key;
  int n_out = box_length (cs_ks->ks_v_out_map) / sizeof (v_out_map_t), inx;
  for (inx = 0; inx < n_out; inx++)
    {
      v_out_map_t *om = &cs_ks->ks_v_out_map[inx];
      dbe_column_t *col = sch_id_to_column (wi_inst.wi_schema, om->om_cl.cl_col_id);
      if (col && 'P' == col->col_name[0] && 0 == col->col_name[1] && 1 == col->col_defined_in->tb_is_rdf_quad)
	{
	  om->om_is_null = OM_CONST;
	  if (!om->om_row_ssl)
	    {
	      p_ssl = om->om_row_ssl = (state_slot_t *) mp_alloc (qi->qi_mp, sizeof (state_slot_t));
	      p_ssl->ssl_type = SSL_CONSTANT;
	      p_ssl->ssl_constant = mp_alloc_box (qi->qi_mp, sizeof (iri_id_t), DV_IRI_ID);
	    }
	  else
	    p_ssl = om->om_row_ssl;
	  *(iri_id_t *) p_ssl->ssl_constant = csetp->csetp_iri;
	}
      else if (col && 'S' == col->col_name[0] && 0 == col->col_name[1])
	om->om_cl = key->key_row_var[0];
      else if (col && 'G' == col->col_name[0] && 0 == col->col_name[1])
	om->om_cl = key->key_row_var[1];
      else
	{
	  om->om_cl = *cl_list_find (key->key_row_var, csetp->csetp_col->col_id);
	}
    }
}


void
cs_ks_extra_preds (key_source_t * ks, caddr_t * inst)
{

}


int
sp_new_key (search_spec_t * sp, cset_p_t * csetp)
{
  cset_t *cset = csetp->csetp_cset;
  dbe_column_t *col = sch_id_to_column (wi_inst.wi_schema, sp->sp_cl.cl_col_id);
  dbe_key_t *key = cset->cset_table->tb_primary_key;
  if (!col)
    return 0;
  if (0 == col->col_name[1])
    {
      switch (col->col_name[0])
	{
	case 'P':
	case 'p':
	  return 0;
	case 'S':
	case 's':
	  sp->sp_cl = key->key_row_var[0];
	  return 1;
	case 'G':
	case 'g':
	  sp->sp_cl = key->key_row_var[1];
	  return 1;
	}
    }
  sp->sp_cl = *cl_list_find (key->key_row_var, csetp->csetp_col->col_id);
  return 1;
}


void
ts_csq_new_cset (table_source_t * ts, caddr_t * inst)
{
  dk_set_t iter, *prev;
  QNCAST (QI, qi, inst);
  key_source_t *ks = ts->ts_order_ks;
  cset_quad_t *csq = ts->ts_csq;
  cset_p_t *csetp = (cset_p_t *) QST_BOX (dk_set_t, inst, csq->csq_cset)->data;
  cset_t *cset = csetp->csetp_cset;
  dbe_table_t *rq = cset->cset_rq_table;
  table_source_t *cs_ts = QST_BOX (table_source_t *, inst, csq->csq_ts);
  key_source_t *cs_ks;
  dbe_column_t *s_col = (dbe_column_t *) dk_set_nth (rq->tb_primary_key->key_parts, 1);
  dbe_column_t *o_col = (dbe_column_t *) dk_set_nth (rq->tb_primary_key->key_parts, 2);
  dbe_column_t *g_col = (dbe_column_t *) dk_set_nth (rq->tb_primary_key->key_parts, 3);
  if (!cs_ts)
    {
      WITH_TMP_POOL (qi->qi_mp)
      {
	dk_set_t lst;
	cs_ts = (table_source_t *) t_alloc (sizeof (table_source_t));
	*cs_ts = *ts;
	cs_ks = cs_ts->ts_order_ks = (key_source_t *) t_alloc (sizeof (key_source_t));
	*cs_ks = *ts->ts_order_ks;
	cs_ts->ts_order_cursor = csq->csq_itc;
	cs_ks->ks_ts = cs_ts;
	cs_ks->ks_row_spec = NULL;
	cs_ks->ks_spec.ksp_spec_array = NULL;
	QST_BOX (table_source_t *, inst, csq->csq_ts) = cs_ts;
	cs_ks->ks_v_out_map = (v_out_map_t *) t_box_copy ((caddr_t) ks->ks_v_out_map);
	cs_ks->ks_out_cols = t_set_copy (ks->ks_out_cols);
	cs_ks->ks_key = cset->cset_table->tb_primary_key;
	lst = ks_col_spec_list (ks, s_col->col_id);
	cs_ks_set_col_spec (cs_ks, "S", lst);
	if (lst)
	  cs_ts->ts_inx_cardinality = cs_ts->ts_cardinality = 1;
	lst = ks_col_spec_list (ks, g_col->col_id);
	cs_ks_set_col_spec (cs_ks, "G", lst);
	lst = ks_col_spec_list (ks, o_col->col_id);
	cs_ks_set_col_spec (cs_ks, csetp->csetp_col->col_name, lst);
	if (!lst)
	  cs_ks_nn_test (cs_ks, csetp->csetp_col);
	cs_ts->ts_csq = (cset_quad_t *) t_alloc (sizeof (cset_quad_t));
	*cs_ts->ts_csq = *ts->ts_csq;
	if (ks->ks_top_oby_spec)
	  {
	    cs_ks->ks_top_oby_spec = t_sp_copy (ks->ks_top_oby_spec);
	    if (sp_new_key (cs_ks->ks_top_oby_spec, csetp))
	      cs_ks->ks_top_oby_spec = NULL;
	  }
	cs_ks->ks_hash_spec = t_set_copy (ks->ks_hash_spec);
	for (iter = ks->ks_hash_spec; iter; iter = iter->next)
	  {
	    iter->data = (void *) sp_copy ((search_spec_t *) iter->data);
	  }
	cs_ts->ts_csq->csq_mode = CSQ_QUAD == ts->ts_csq->csq_mode ? CSQ_CSET : CSQ_CSET_SCAN_CSET;
      }
      END_WITH_TMP_POOL;
    }
  else
    {
      cs_ks = cs_ts->ts_order_ks;
      cs_ks_set_key (cs_ks, csetp);
    }
  cs_ks_set_out_cols ((QI *) inst, cs_ks, csetp);
  prev = &cs_ks->ks_hash_spec;
  for (iter = ks->ks_hash_spec; iter; iter = iter->next)
    {
      if (!sp_new_key ((search_spec_t *) iter->data, csetp))
	{
	  *prev = iter->next;
	  continue;
	}
      prev = &iter->next;
    }
  ks_set_search_params (NULL, NULL, cs_ks);
}



iri_id_t
ts_csq_next_p (table_source_t * ts, caddr_t * inst)
{
  iri_id_t p;
  cset_quad_t *csq = ts->ts_csq;
  int last = QST_INT (inst, csq->csq_last_p);
  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
  data_col_t *p_dc = ITC_P_VEC (itc, 0);
  if (!p_dc)
    {
      if (-1 == last)
	{
	  p = unbox_iri_id (itc->itc_search_params[0]);
	  QST_INT (inst, csq->csq_last_p) = 0;
	  return p;
	}
      return 0;
    }
  else
    {
      int last_p = QST_INT (inst, csq->csq_last_p);
      if (-1 == last_p)
	{
	  QST_INT (inst, csq->csq_last_p) = last_p = 0;
	  return ((iri_id_t *) p_dc->dc_values)[itc->itc_param_order[0]];
	}
      if (last_p < p_dc->dc_n_values)
	{
	  int inx;
	  iri_id_t prev_p = ((iri_id_t *) p_dc->dc_values)[itc->itc_param_order[last_p]];
	  for (inx = last_p + 1; inx < p_dc->dc_n_values; inx++)
	    {
	      iri_id_t next_p = ((iri_id_t *) p_dc->dc_values)[itc->itc_param_order[inx]];
	      if (next_p != prev_p)
		{
		  QST_INT (inst, csq->csq_last_p) = inx;
		  return next_p;
		}
	    }
	}
      return 0;
    }
}


int
itc_param_by_ssl (it_cursor_t * itc, state_slot_t * ssl)
{
  /* gets the search param's  place in itc search_params given the ssl */
  search_spec_t *sp;
  for (sp = itc->itc_key_spec.ksp_spec_array; sp; sp = sp->sp_next)
    {
      if (ssl == sp->sp_min_ssl)
	return sp->sp_min;
      if (ssl == sp->sp_max_ssl)
	return sp->sp_max;
    }
  for (sp = itc->itc_row_specs; sp; sp = sp->sp_next)
    {
      if (ssl == sp->sp_min_ssl)
	return sp->sp_min;
      if (ssl == sp->sp_max_ssl)
	return sp->sp_max;
    }
  GPF_T1 ("cset param ssl not found in rq itc");
  return 0;
}


void
itc_param_copy (it_cursor_t * itc_to, search_spec_t * sp, it_cursor_t * itc_from)
{
  int param_fill = itc_to->itc_search_par_fill;
  for (sp = sp; sp; sp = sp->sp_next)
    {
      if (CMP_HASH_RANGE == sp->sp_min_op)
	continue;
      if (sp->sp_min_ssl)
	{
	  int rq_place = itc_param_by_ssl (itc_from, sp->sp_min_ssl);
	  itc_to->itc_search_params[param_fill] = itc_from->itc_search_params[rq_place];
	  ITC_P_VEC (itc_to, param_fill) = ITC_P_VEC (itc_from, rq_place);
	  param_fill++;
	}
      if (sp->sp_max_ssl)
	{
	  int rq_place = itc_param_by_ssl (itc_from, sp->sp_max_ssl);
	  itc_to->itc_search_params[param_fill] = itc_from->itc_search_params[rq_place];
	  ITC_P_VEC (itc_to, param_fill) = ITC_P_VEC (itc_from, rq_place);
	  param_fill++;
	}
    }
  itc_to->itc_search_par_fill = param_fill;
}


void
itc_cset_search_params (it_cursor_t * itc, it_cursor_t * rq_itc)
{
  /* copy the params 1:1, the sps have the same indices.  Set param order to only have applicable sets */
  key_source_t *ks = itc->itc_ks;
  int inx;
  caddr_t *inst = rq_itc->itc_out_state;
  QNCAST (QI, qi, inst);
  int *param_order;
  cset_quad_t *csq = itc->itc_ks->ks_ts->ts_csq;
  data_col_t *s_dc = ITC_P_VEC (rq_itc, 1);
  int s_given = ks->ks_spec.ksp_spec_array && ks->ks_spec.ksp_spec_array->sp_next
      && CMP_EQ == ks->ks_spec.ksp_spec_array->sp_next->sp_min_op;
  data_col_t *p_dc = ITC_P_VEC (rq_itc, 0);
  cset_p_t *csetp = (cset_p_t *) QST_BOX (dk_set_t, inst, csq->csq_cset)->data;
  int cset_rng = csetp->csetp_cset->cset_id;
  iri_id_t expect_p = p_dc ? csetp->csetp_iri : 0;
  int param_fill = 0, param_size;
  itc_param_copy (itc, itc->itc_key_spec.ksp_spec_array, rq_itc);
  itc_param_copy (itc, itc->itc_row_specs, rq_itc);
  param_order = QST_BOX (int *, inst, csq->csq_param_order);
  param_size = param_order ? box_length (param_order) / sizeof (int) : 0;
  if (param_size < rq_itc->itc_n_sets)
    {
      int new_sz = MIN (rq_itc->itc_n_sets * 1.2, dc_max_batch_sz);
      param_size = new_sz;
      param_order = (int *) mp_alloc_box (qi->qi_mp, new_sz * sizeof (int), DV_BIN);
      QST_BOX (int *, inst, csq->csq_param_order) = param_order;
    }
  for (inx = 0; inx < rq_itc->itc_n_sets; inx++)
    {
      iri_id_t s =
	  s_given ? (s_dc ? ((iri_id_t *) s_dc->dc_values)[rq_itc->itc_param_order[inx]] : unbox_iri_id (rq_itc->
	      itc_search_params[1])) : 0;
      int rng = IRI_RANGE (s);
      if (rng == cset_rng || !s_given)
	{
	  if (!expect_p || expect_p == ((iri_id_t *) p_dc->dc_values)[rq_itc->itc_param_order[inx]])
	    {
	      param_order[param_fill] = rq_itc->itc_param_order[inx];
	      param_fill++;
	    }
	}
    }
  itc->itc_param_order = param_order;
  itc->itc_set = 0;
  itc->itc_n_sets = param_fill;
}

extern int qp_even_if_lock;

int
ks_cset_quad_start_search (key_source_t * ks, caddr_t * inst, buffer_desc_t ** buf_ret)
{
  /* The main ts itc has the params for the quad.  This has a fixed p and gets the suitable subset of sets, already ordered, from this.  Has its own itc param order */
  query_t *qr;
  table_source_t *ts = ks->ks_ts;
  cset_quad_t *csq = ts->ts_csq;
  QNCAST (QI, qi, inst);
  int res;
  it_cursor_t *rq_itc = QST_BOX (it_cursor_t *, inst, csq->csq_rq_itc->ssl_index);
  it_cursor_t *itc = QST_BOX (it_cursor_t *, inst, ts->ts_order_cursor->ssl_index);
  buffer_desc_t *buf;
  if (itc->itc_is_col)
    itc_col_free (itc);
  itc->itc_ks = ks;
  itc->itc_n_results = 0;
  itc->itc_out_state = inst;
  itc->itc_key_spec = ks->ks_spec;
  if (itc->itc_top_row_spec)
    {
      /* if on a previous use there was a top k added to row specs remove because in a subq the next use might start with no top k value.  Also resets a possible added hash spec */
      key_free_trail_specs (itc->itc_row_specs);
      itc->itc_top_row_spec = 0;
      itc->itc_hash_row_spec = 0;
    }
  if (!itc->itc_hash_row_spec)
    itc->itc_row_specs = ks->ks_row_spec;
  if (!itc->itc_hash_row_spec)
    {
      itc->itc_row_specs = ks->ks_row_spec;
      if (ks->ks_hash_spec)
	ks_add_hash_spec (ks, inst, itc);
    }

  itc_from (itc, ks->ks_key, qi->qi_client->cli_slice);
  itc->itc_search_mode = SM_READ;
  if (!itc->itc_key_spec.ksp_key_cmp)
    itc->itc_key_spec.ksp_key_cmp = pg_key_compare;
  itc_free_owned_params (itc);
  ITC_START_SEARCH_PARS (itc);
  ks_vec_new_results (ks, inst, itc);
  itc->itc_v_out_map = ks->ks_v_out_map;
  itc_cset_search_params (itc, rq_itc);
  if (!itc->itc_n_sets)
    return 0;
  itc->itc_ltrx = qi->qi_trx;	/* same qi can continue on different aq thread, make sure itc ltrx agrees */
  itc->itc_insert_key = ks->ks_key;
  if (ks->ks_top_oby_spec)
    ts_top_oby_limit (ts, inst, itc);
  itc->itc_rows_on_leaves = 0;
  itc->itc_rows_selected = 0;
  qr = ts->src_gen.src_query;
  if (ks->ks_lock_mode)
    itc->itc_lock_mode = ks->ks_lock_mode;
  else if (qr->qr_select_node && ts->src_gen.src_query->qr_lock_mode != PL_EXCLUSIVE)
    {
      itc->itc_lock_mode = qi->qi_lock_mode;
    }
  else if (qr->qr_qf_id)
    itc->itc_lock_mode = qr->qr_lock_mode;
  else
    itc->itc_lock_mode = qp_even_if_lock ? PL_SHARED : (PL_EXCLUSIVE == qr->qr_lock_mode ? PL_EXCLUSIVE : PL_SHARED);
  /* if the statement is not a SELECT, take excl. lock */
  if (!ks->ks_is_vec_plh && qi->qi_isolation < ISO_REPEATABLE && qi->qi_client->cli_row_autocommit && (enable_mt_txn
	  || qi->qi_non_txn_insert))
    itc->itc_lock_mode = PL_SHARED;
  itc->itc_isolation = qi->qi_isolation;
  if (ks->ks_isolation)
    itc->itc_isolation = ks->ks_isolation;
  if (itc->itc_isolation < ISO_COMMITTED && PL_EXCLUSIVE == itc->itc_lock_mode)
    itc->itc_isolation = ISO_COMMITTED;

  if (itc->itc_isolation <= ISO_COMMITTED && PL_SHARED == itc->itc_lock_mode && !itc->itc_is_vacuum)
    {
      itc->itc_dive_mode = PA_READ_ONLY;
      itc->itc_simple_ps = 1;
    }
  if (!ks->ks_row_check)
    ks->ks_row_check = itc_row_check;
  itc_set_param_row (itc, 0);
  ITC_FAIL (itc)
  {
    if (PL_EXCLUSIVE == itc->itc_lock_mode || itc->itc_isolation > ISO_COMMITTED)
      cl_enlist_ck (itc, NULL);
    if (ks->ks_key->key_is_col)
      itc_col_init (itc);
    ks_set_dfg_queue (ks, inst, itc);
    buf = ts_initial_itc (ts, inst, itc);
    if (!buf && ts->ts_in_sdfg)
      return 2;			/* sdfg run to completion, return from the ts input */
    itc->itc_n_results = 0;
    res = itc_vec_next (itc, &buf);
    itc->itc_rows_selected += itc->itc_n_results;
    if (itc->itc_n_results == itc->itc_batch_size && !(itc->itc_set == itc->itc_n_sets - 1 && DVC_GREATER == res))
      {
	/* full batch, will be continuable.  Except if at end of sets and last rc not a match */
	*buf_ret = buf;
	return 1;		/* full, must continue to see if more */
      }
    itc_page_leave (itc, buf);
    return 0;
  }
  ITC_FAILED itc_assert_no_reg (itc);
  END_FAIL (itc);
  return 0;			/* never executed */
}


void
ts_csq_cset_qp (table_source_t * ts, caddr_t * inst)
{
  /* a ts getting a cset by s is called on qp thread.  Set the cset */
  cset_quad_t *csq = ts->ts_csq;
  table_source_t *cs_ts;
  int step = QST_INT (inst, csq->csq_nth_step);
  if (CSQS_QP_START == step)
    {
      QST_INT (inst, csq->csq_nth_step) = CSQS_QP_RUN;
      ts_csq_new_cset (ts, inst);
    }
  cs_ts = QST_BOX (table_source_t *, inst, csq->csq_ts);
  table_source_input (cs_ts, inst, NULL);
}


void
ts_csq_ret (table_source_t * ts, caddr_t * inst)
{
  cset_quad_t *csq = ts->ts_csq;
  table_source_t *csq_ts;
  int new_cset = 1;
  int nth = QST_INT (inst, ts->ts_csq->csq_nth_step);
  iri_id_t p;
  int aq_state = ts->ts_aq ? QST_INT (inst, ts->ts_aq_state) : TS_AQ_NONE;
  if (TS_AQ_NONE != aq_state && TS_AQ_COORD != aq_state)
    return;
  if (CSQS_INIT == nth)
    {
      QST_INT (inst, csq->csq_last_p) = -1;
      QST_INT (inst, csq->csq_cset) = 0;
      nth = QST_INT (inst, csq->csq_nth_step) = CSQS_NEW_P;
    }
  new_cset = 1;
  for (;;)
    {
      nth = QST_INT (inst, csq->csq_nth_step);
      if (CSQS_NEW_P == nth)
	{
	  dk_set_t csp_list;
	new_p:
	  p = ts_csq_next_p (ts, inst);
	  if (!p)
	    {
	      SRC_IN_STATE (ts, inst) = NULL;
	      return;
	    }
	  csp_list = (dk_set_t) gethash ((void *) p, &p_to_csetp_list);
	  if (!csp_list)
	    goto new_p;
	  QST_BOX (dk_set_t, inst, csq->csq_cset) = csp_list;
	  nth = QST_INT (inst, csq->csq_nth_step) = CSQS_CONTINUE;
	  new_cset = 1;
	}
      if (CSQS_NEW_CSET == nth)
	{
	  dk_set_t next_csp = QST_BOX (dk_set_t, inst, csq->csq_cset)->next;
	  if (!next_csp)
	    {
	      nth = QST_INT (inst, csq->csq_nth_step) = CSQS_NEW_P;
	      continue;
	    }
	  QST_BOX (dk_set_t, inst, csq->csq_cset) = next_csp;
	  new_cset = 1;
	}
      if (new_cset)
	ts_csq_new_cset (ts, inst);
      csq_ts = QST_BOX (table_source_t *, inst, csq->csq_ts);
      table_source_input (csq_ts, inst, new_cset ? inst : NULL);
    }
}


void
ts_csq_end (table_source_t * ts, caddr_t * inst)
{
  cset_quad_t *csq = ts->ts_csq;
  if (CSQ_CSET_SCAN == csq->csq_mode)
    {
      if (CSQS_EXC_SCAN == QST_INT (inst, csq->csq_nth_step))
	QST_INT (inst, csq->csq_nth_step) = 0;
      return;
    }
  if (CSQ_CSET_SCAN_CSET == csq->csq_mode || CSQ_SP == csq->csq_mode)
    return;
  if (CSQ_QUAD == csq->csq_mode)
    {
      QST_INT (inst, csq->csq_nth_step) = CSQS_INIT;
      QST_INT (inst, csq->csq_last_p) = -1;
      SRC_IN_STATE (ts, inst) = inst;
    }
  else
    {
      int aq_state = ts->ts_aq_state ? QST_INT (inst, ts->ts_aq_state) : TS_AQ_NONE;
      if (TS_AQ_NONE == aq_state || TS_AQ_COORD == aq_state || TS_AQ_COORD_AQ_WAIT == aq_state)
	{
	  QST_INT (inst, csq->csq_nth_step) = CSQS_NEW_CSET;
	  SRC_IN_STATE (ts, inst) = inst;
	  QST_INT (inst, csq->csq_at_end) = 1;
	}
    }
}


void
ts_psog_scan (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  int step, qp_start = 0;
  table_source_t *cs_ts;
  int n_sets = QST_INT (inst, ts->src_gen.src_prev->src_out_fill);
  cset_quad_t *csq = ts->ts_csq;
  caddr_t *o_mode = (caddr_t *) qst_get (inst, csq->csq_o_scan_mode);
  cset_t *cset;
  if (state && !o_mode)
    {
      /* The cols  are bound by posg, output 1:1 */
      QN_CHECK_SETS (ts, inst, n_sets + 1);
      QST_INT (inst, ts->src_gen.src_out_fill) = n_sets;
      int_asc_fill (QST_BOX (int *, inst, ts->src_gen.src_sets), n_sets, 0);
      if (csq->csq_org_s)
	vec_ssl_assign (inst, csq->csq_s_out, csq->csq_org_s);
      if (csq->csq_org_o)
	vec_ssl_assign (inst, csq->csq_o_out, csq->csq_org_o);
      if (csq->csq_org_g)
	vec_ssl_assign (inst, csq->csq_g_out, csq->csq_org_g);
      SRC_IN_STATE (ts, inst) = NULL;
      qn_send_output ((data_source_t *) ts, inst);
      return;
    }
  cset = cset_by_id (unbox (o_mode[0]));
  step = QST_INT (inst, csq->csq_nth_step);
  if (!step)
    step = QST_INT (inst, csq->csq_nth_step) = CSQS_CSET_SCAN_COORD;
  if (CSQS_QP_START == step)
    {
      qp_start = 1;
      QST_INT (inst, csq->csq_nth_step) = CSQS_QP_RUN;
    }
  for (;;)
    {
      if (state || qp_start)
	{
	  iri_id_t p = unbox_iri_id (o_mode[1]);
	  dk_set_t csp = (dk_set_t) gethash ((void *) p, &p_to_csetp_list);
	  for (csp = csp; csp; csp = csp->next)
	    {
	      QNCAST (cset_p_t, csetp, csp->data);
	      if (p == csetp->csetp_iri)
		{
		  QST_BOX (dk_set_t, inst, csq->csq_cset) = csp;
		  goto found;
		}
	    }
	  log_error ("csetp for o scan after posg not found");
	  SRC_IN_STATE (ts, inst) = NULL;
	  return;
	found:
	  QST_INT (inst, csq->csq_at_end) = 0;
	  ts_csq_new_cset (ts, inst);
	  qst_set (inst, csq->csq_s_min, box_iri_id ((int64) cset->cset_id << 53));
	  qst_set (inst, csq->csq_s_max, box_iri_id ((int64) (cset->cset_id + 1) << 53));
	  SRC_IN_STATE (ts, inst) = inst;
	  cs_ts = QST_BOX (table_source_t *, inst, csq->csq_ts);
	  cs_ts->ts_inx_cardinality = cs_ts->ts_cardinality = dbe_key_count (cs_ts->ts_order_ks->ks_key);
	  cs_ts->ts_cost_after = cs_ts->ts_cardinality * 1.5;
	  if (qp_start)
	    goto cont;
	  table_source_input (cs_ts, inst, state);
	  state = NULL;
	}
      else
	{
	  if (QST_INT (inst, csq->csq_at_end))
	    {
	      QST_INT (inst, csq->csq_at_end) = 0;
	      if (CSQS_QP_RUN == QST_INT (inst, csq->csq_nth_step))
		{
		  SRC_IN_STATE (ts, inst) = NULL;
		  QST_INT (inst, csq->csq_nth_step) = 0;
		  return;
		}
	      if (CSQS_CSET_SCAN_COORD == QST_INT (inst, csq->csq_nth_step))
		{
		  QST_INT (inst, csq->csq_nth_step) = CSQS_EXC_SCAN;
		  table_source_input (ts, inst, inst);
		  state = NULL;
		}
	      if (CSQS_EXC_SCAN == QST_INT (inst, csq->csq_nth_step))
		{
		  SRC_IN_STATE (ts, inst) = NULL;
		  return;
		}
	      else
		{
		  QST_INT (inst, csq->csq_nth_step) = 0;
		  SRC_IN_STATE (ts, inst) = NULL;
		  return;
		}
	    }
	  else
	    {
	    cont:
	      if (qp_start)
		{
		  qp_start = 0;
		  QST_INT (inst, csq->csq_nth_step) = CSQS_QP_RUN;
		}
	      if (CSQS_QP_RUN == step || CSQS_CSET_SCAN_COORD == step)
		{
		  cs_ts = QST_BOX (table_source_t *, inst, csq->csq_ts);
		  table_source_input (cs_ts, inst, NULL);
		  if (CSQS_QP_RUN == step && !SRC_IN_STATE (ts, inst))
		    return;
		}
	      else
		{
		  table_source_input (ts, inst, state);
		  if (!SRC_IN_STATE (ts, inst))
		    return;
		}
	    }
	}
    }
}


void
ts_sp_cset_p (table_source_t * ts, caddr_t * inst, caddr_t * state)
{
  key_source_t *ks = ts->ts_order_ks;
  iri_id_t s;
  it_cursor_t *itc = TS_ORDER_ITC (ts, inst);
  data_col_t *s_dc = ITC_P_VEC (itc, 0);
  cset_quad_t *csq = ts->ts_csq;
  dk_set_t ssl_iter;
  data_col_t *s_out = NULL, *p_out = NULL;
  int batch_size = QST_INT (inst, ts->src_gen.src_batch_size), nth_set, nth_p;
  if (state)
    {
      QST_INT (inst, csq->csq_nth_set) = itc->itc_first_set;
      QST_INT (inst, csq->csq_last_p) = 0;
    }
  ssl_iter = ks->ks_out_slots;
  DO_SET (dbe_column_t *, col, &ks->ks_out_cols)
  {
    if ('S' == col->col_name[0])
      s_out = QST_BOX (data_col_t *, inst, ((state_slot_t *) ssl_iter->data)->ssl_index);
    else if ('P' == col->col_name[0])
      p_out = QST_BOX (data_col_t *, inst, ((state_slot_t *) ssl_iter->data)->ssl_index);
    ssl_iter = ssl_iter->next;
  }
  END_DO_SET ();
  if (!s_dc)
    {
      s = unbox_iri_id (itc->itc_search_params[0]);
    }
  nth_set = QST_INT (inst, csq->csq_nth_set);
  nth_p = QST_INT (inst, csq->csq_last_p);
  for (nth_set = nth_set; nth_set < itc->itc_n_sets; nth_set++)
    {
      uint64 rng;
      if (s_dc)
	s = ((iri_id_t *) s_dc->dc_values)[nth_set];
      rng = IRI_RANGE (s);
      if (rng)
	{
	  cset_t *cset = (cset_t *) gethash ((void *) rng, &id_to_cset);
	  if (cset)
	    {
	      cset_p_t **ps = cset_p_array (cset);
	      int n_ps = BOX_ELEMENTS (ps);
	      for (nth_p = nth_p; nth_p < n_ps; nth_p++)
		{
		  if (s_out)
		    dc_append_int64 (s_out, s);
		  if (p_out)
		    dc_append_int64 (p_out, ps[nth_p]->csetp_iri);
		  qn_result (ts, inst, nth_set);
		  if (batch_size == QST_INT (inst, ts->src_gen.src_out_fill))
		    {
		      QST_INT (inst, csq->csq_last_p) = nth_p + 1;
		      QST_INT (inst, csq->csq_nth_set) = nth_set;
		      SRC_IN_STATE (ts, inst) = inst;
		      qn_ts_send_output ((data_source_t *) ts, inst, NULL);
		      batch_size = QST_INT (inst, ts->src_gen.src_batch_size);
		      ks_vec_new_results (ks, inst, itc);
		    }
		}
	    }
	}
    }
  ts_at_end (ts, inst);
}
