/*
 *  csetgen.c
 *
 *  $Id$
 *
 *  cset executable graph generation
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


#include "libutil.h"
#include "sqlnode.h"
#include "eqlcomp.h"
#include "sqlfn.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlbif.h"
#include "arith.h"
#include "security.h"
#include "sqlo.h"
#include "list2.h"
#include "xmlnode.h"
#include "xmltree.h"
#include "rdfinf.h"



caddr_t csg_s_uname;
caddr_t csg_s_param_uname;
caddr_t csg_s_param_2_uname;
caddr_t csg_g_uname;
caddr_t csg_o_uname;
caddr_t csg_p_uname;

#define S_NAME csg_s_uname
#define G_NAME csg_g_uname
#define S_PARAM_NAME csg_s_param_uname
#define S_PARAM_NAME_2 csg_s_param_2_uname
#define O_NAME csg_o_uname
#define P_NAME csg_p_uname




typedef struct csg_col_s
{
  dbe_column_t *csgc_ref_col;
  dbe_column_t *csgc_col;
  float csgc_selectivity;	/* expected matches on this col / count of cset table  */
} csg_col_t;

ST *
csg_param_tree (sqlo_t * so, state_slot_t * ssl)
{
  /* ssl is from the containing query, make a stand-in tree and dfe for it */
  sql_comp_t *sc = so->so_sc;
  col_ref_rec_t *crr;
  char tmp[20];
  ST *tree;
  if (SSL_CONSTANT == ssl->ssl_type)
    return (ST *) ssl->ssl_constant;
  if (!sc->sc_cset_param)
    sc->sc_cset_param = hash_table_allocate (11);
  snprintf (tmp, sizeof (tmp), "csp_%d", ssl->ssl_index);
  tree = (ST *) gethash ((void *) (ptrlong) ssl->ssl_index, sc->sc_cset_param);
  if (tree)
    return tree;
  tree = t_listst (3, COL_DOTTED, NULL, t_box_string (tmp));
  crr = (col_ref_rec_t *) t_alloc (sizeof (col_ref_rec_t));
  memzero (crr, sizeof (col_ref_rec_t));
  crr->crr_col_ref = tree;
  crr->crr_ssl = ssl;
  t_set_push (&so->so_sc->sc_col_ref_recs, (void *) crr);
  sethash ((void *) (ptrlong) ssl->ssl_index, sc->sc_cset_param, (void *) tree);
  return tree;
}


int
csg_should_add_spec (search_spec_t * sp)
{
  int min_op, max_op;
  if (!sp)
    return 0;
  min_op = sp->sp_min_op;
  max_op = sp->sp_max_op;
  if (max_op)
    return 1;
  return CMP_EQ == min_op || CMP_GT == min_op || CMP_GTE == min_op;
}



int
csg_dfe_add_spec (sqlo_t * so, df_elt_t * tb_dfe, search_spec_t * s_spec, ST * col_tree)
{
  ST *s_exp;
  df_elt_t *cond_dfe, *s_par_dfe;
  if (s_spec->sp_min_op)
    {
      BIN_OP (s_exp, dvc_to_bop (s_spec->sp_min_op), col_tree, csg_param_tree (so, s_spec->sp_min_ssl));
      cond_dfe = sqlo_df (so, s_exp);
      s_par_dfe = cond_dfe->_.bin.right;
      s_par_dfe->dfe_ssl = s_spec->sp_min_ssl;
      t_set_push (&tb_dfe->_.table.col_preds, cond_dfe);
    }
  if (s_spec->sp_max_op)
    {
      BIN_OP (s_exp, dvc_to_bop (s_spec->sp_max_op), col_tree, csg_param_tree (so, s_spec->sp_max_ssl));
      cond_dfe = sqlo_df (so, s_exp);
      s_par_dfe = cond_dfe->_.bin.right;
      s_par_dfe->dfe_ssl = s_spec->sp_max_ssl;
      t_set_push (&tb_dfe->_.table.col_preds, cond_dfe);
    }
}

void
csg_s_cond (sqlo_t * so, df_elt_t * tb_dfe, df_elt_t ** first_dfe, df_elt_t ** first_s_col_dfe, search_spec_t * s_spec)
{
  /* if first put the s_spec as s cond.  If not join s to the first s */
  ST *s_exp;
  df_elt_t *cond_dfe;
  df_elt_t *s_col = sqlo_df (so, t_listst (3, COL_DOTTED, tb_dfe->_.table.ot->ot_new_prefix, t_box_string ("S")));
  if (!*first_dfe)
    {
      df_elt_t *s_col_dfe = sqlo_df (so, t_listst (3, COL_DOTTED, tb_dfe->_.table.ot->ot_new_prefix, S_NAME));
      if (s_spec)
	{
	  csg_dfe_add_spec (so, tb_dfe, s_spec, s_col->dfe_tree);
	}
      else
	{
	  /* if rq as first tb of a cset, the s is bounded to cset */
	  caddr_t cset_low = t_box_iri_id (((int64) so->so_sc->sc_csg_cset->cset_id) << CSET_RNG_SHIFT);
	  caddr_t cset_high = t_box_iri_id (((int64) so->so_sc->sc_csg_cset->cset_id + 1) << CSET_RNG_SHIFT);
	  ST *s_low_st, *s_high_st;
	  BIN_OP (s_low_st, BOP_GTE, s_col->dfe_tree, cset_low);
	  BIN_OP (s_high_st, BOP_LT, s_col->dfe_tree, cset_high);
	  t_set_push (&tb_dfe->_.table.col_preds, sqlo_df (so, s_low_st));
	  t_set_push (&tb_dfe->_.table.col_preds, sqlo_df (so, s_high_st));
	}
      *first_s_col_dfe = s_col_dfe;
      *first_dfe = tb_dfe;
    }
  else
    {
      BIN_OP (s_exp, BOP_EQ, s_col->dfe_tree, (*first_s_col_dfe)->dfe_tree);
      cond_dfe = sqlo_df (so, s_exp);
      t_set_push (&tb_dfe->_.table.col_preds, cond_dfe);
      t_set_pushnew (&(*first_dfe)->_.table.out_cols, *first_s_col_dfe);
    }

}


void
csg_p_cond (sqlo_t * so, df_elt_t * rq_dfe, iri_id_t iri)
{
  ST *exp;
  BIN_OP (exp, BOP_EQ, t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix, P_NAME), (ST *) t_box_iri_id (iri));
  t_set_push (&rq_dfe->_.table.col_preds, sqlo_df (so, exp));
}


void
csg_g_cond (sqlo_t * so, df_elt_t * tb_dfe, search_spec_t * g_spec)
{
  if (csg_should_add_spec (g_spec))
    csg_dfe_add_spec (so, tb_dfe, g_spec, t_listst (3, COL_DOTTED, tb_dfe->_.table.ot->ot_new_prefix, G_NAME));
}


int
csg_o_cond (sqlo_t * so, df_elt_t * tb_dfe, dbe_column_t * col, table_source_t * ts, ST * col_tree)
{
  int any = 0;
  key_source_t *ks = ts->ts_order_ks;
  search_spec_t *sp;
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    {
      if (col->col_id == sp->sp_cl.cl_col_id)
	{
	  if (!any)
	    any = 1;
	  if (csg_should_add_spec (sp))
	    {
	      any = 2;
	      csg_dfe_add_spec (so, tb_dfe, sp, col_tree);
	    }
	}
    }
  return any;
}


int
csg_o_scan_cond (sqlo_t * so, df_elt_t * cset_dfe, dbe_column_t * col, table_source_t * ts, ST * col_tree)
{
  caddr_t *o_mode = so->so_sc->sc_csg_o_mode;
  ST *tree;
  int min_op = unbox (o_mode[2]);
  int max_op = unbox (o_mode[4]);
  if (CMP_NONE != min_op)
    {
      BIN_OP (tree, dvc_to_bop (min_op), col_tree, o_mode[3]);
      t_set_push (&cset_dfe->_.table.col_preds, sqlo_df (so, tree));
    }
  if (CMP_NONE != max_op)
    {
      BIN_OP (tree, dvc_to_bop (max_op), col_tree, o_mode[5]);
      t_set_push (&cset_dfe->_.table.col_preds, sqlo_df (so, tree));
    }
}


df_elt_t *
csg_add_tb (sqlo_t * so, dbe_key_t * key)
{
  char tmp[20];
  t_NEW_VARZ (op_table_t, cset_ot);
  df_elt_t *cset_dfe = sqlo_new_dfe (so, DFE_TABLE, NULL);
  cset_dfe->_.table.key = key;
  snprintf (tmp, sizeof (tmp), "c%d", so->so_name_ctr++);
  cset_ot->ot_new_prefix = t_box_string (tmp);
  cset_ot->ot_dfe = cset_dfe;
  cset_ot->ot_table = key->key_table;
  cset_dfe->_.table.ot = cset_ot;
  sqlo_place_dfe_after (so, LOC_LOCAL, so->so_gen_pt, cset_dfe);
  t_set_push (&so->so_tables, cset_ot);
  return cset_dfe;
}


dbe_column_t *
cset_col_by_iri (cset_t * cset, iri_id_t iri)
{
  DO_SET (dbe_column_t *, col, &cset->cset_table->tb_primary_key->key_parts)
  {
    if (!col->col_csetp)
      continue;
    if (iri == col->col_csetp->csetp_iri)
      return col;
  }
  END_DO_SET ();
  return NULL;
}

int
csgc_cmp (const void *s1, const void *s2)
{
  csg_col_t *c1 = (csg_col_t *) s1;
  csg_col_t *c2 = (csg_col_t *) s2;
  if (c1->csgc_ref_col->col_is_cset_opt && !c2->csgc_ref_col->col_is_cset_opt)
    return 1;
  if (!c1->csgc_ref_col->col_is_cset_opt && c2->csgc_ref_col->col_is_cset_opt)
    return -1;
  return c1->csgc_selectivity < c2->csgc_selectivity ? -1 : 1;
}


float
csgc_selectivity (cset_t * cset, table_source_t * ts, dbe_column_t * ref_col, dbe_column_t * cset_col)
{
  key_source_t *ks = ts->ts_order_ks;
  float tb_count = dbe_key_count (cset->cset_table->tb_primary_key);
  float col_count = ref_col->col_count;
  float sel = MIN (col_count / (tb_count + 1), 1);
  search_spec_t *sp;
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    if (sp->sp_selectivity)
      sel *= sp->sp_selectivity;
  DO_SET (search_spec_t *, sp, &ks->ks_hash_spec) sel *= sp->sp_selectivity ? sp->sp_selectivity : 0.5;
  END_DO_SET ();
  return sel;
}


int
csg_col_is_o_by_index (sql_comp_t * sc, table_source_t * ts, dbe_column_t * col)
{
  if (CSQ_LOOKUP == sc->sc_csg_mode)
    {
      cset_ts_t *csts = ts->ts_csts;
      if (csts->csts_o_index_p && col->col_cset_iri == csts->csts_o_index_p)
	return 1;
    }
  return 0;
}


void
csg_ts_frame (sqlo_t * so, op_table_t * top_ot, cset_t * cset, table_source_t * ts)
{
  cset_ts_t *csts = ts->ts_csts;
  sql_comp_t *sc = so->so_sc;
  caddr_t *o_mode = sc->sc_csg_o_mode;
  csg_col_t *csgc_arr;
  op_table_t *cset_ot = NULL;
  df_elt_t *cset_dfe = NULL, *rq_dfe = NULL, *first_dfe = NULL, *first_s_col_dfe = NULL;
  int cset_any_reqd = 0;
  dk_hash_t *ht = hash_table_allocate (23);
  int n_cols, fill = 0, inx, cset_placed = 0;
  search_spec_t *s_spec = NULL, *g_spec = NULL, *g_inx_spec = NULL, *sp;
  key_source_t *ks = ts->ts_order_ks;
  oid_t g_col_id = ((dbe_column_t *) ks->ks_key->key_parts->next->data)->col_id;
  if (CSQ_LOOKUP == sc->sc_csg_mode && csts->csts_o_from_posg)
    {
      sc->sc_cset_param = hash_table_allocate (11);
      sethash ((void *) (ptrlong) csts->csts_o_from_posg->ssl_index, sc->sc_cset_param, (void *) 1);
    }
  if (ks->ks_spec.ksp_spec_array)
    {
      s_spec = ks->ks_spec.ksp_spec_array;
      g_inx_spec = ks->ks_spec.ksp_spec_array->sp_next;
    }
  if (CSQ_TABLE == sc->sc_csg_mode && s_spec)
    {
      /* if making a scan for a non indexed o value, there is no s spec */
      s_spec = NULL;
      if (g_inx_spec)
	g_spec = g_inx_spec;
    }
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    {
      if (sp->sp_col == (dbe_column_t *) ks->ks_key->key_parts->data
	  || sp->sp_col == (dbe_column_t *) ks->ks_key->key_parts->next->data)
	continue;		/* s or g specs do not make rq access */
      if (csg_col_is_o_by_index (sc, ts, sp->sp_col))
	continue;
      sethash ((void *) sp->sp_col, ht, (void *) 1);
      if (sp->sp_cl.cl_col_id == g_col_id)
	{
	  if (g_spec && CMP_EQ == g_spec->sp_min_op)
	    ;
	  else
	    g_spec = sp;
	}
      else if (sp->sp_col == (dbe_column_t *) ks->ks_key->key_parts->data
	  || sp->sp_col == (dbe_column_t *) ks->ks_key->key_parts->next->data)
	continue;		/* s or g specs do not make rq access */
      if (csts->csts_o_index_p && sp->sp_col->col_cset_iri == csts->csts_o_index_p)
	continue;
      sethash ((void *) sp->sp_col, ht, (void *) 1);
    }
  DO_SET (dbe_column_t *, col, &ks->ks_key->key_parts)
  {
    if (!col->col_cset_iri	/*not s or g */
	|| csg_col_is_o_by_index (sc, ts, col))
      continue;
    sethash ((void *) col, ht, (void *) 2);
  }
  END_DO_SET ();
  n_cols = ht->ht_count;
  csgc_arr = (csg_col_t *) t_alloc_box (sizeof (csg_col_t) * n_cols, DV_BIN);
  DO_HT (dbe_column_t *, col, ptrlong, fl, ht)
  {
    dbe_column_t *cset_col = cset_col_by_iri (cset, col->col_cset_iri);
    csgc_arr[fill].csgc_ref_col = col;
    csgc_arr[fill].csgc_selectivity = csgc_selectivity (cset, ts, col, cset_col);
    csgc_arr[fill++].csgc_col = cset_col;
  }
  END_DO_HT;
  qsort (csgc_arr, ht->ht_count, sizeof (csg_col_t), csgc_cmp);
  for (inx = 0; inx < ht->ht_count; inx++)
    {
      csg_col_t *csgc = &csgc_arr[inx];
      dbe_column_t *ref_col = csgc->csgc_ref_col;
      df_elt_t *tb_dfe = NULL;
      if (csgc->csgc_col)
	{
	  int any_cond = 0;
	  df_elt_t *cset_col;
	  if (!cset_placed)
	    {
	      cset_dfe = csg_add_tb (so, cset->cset_table->tb_primary_key);
	      tb_dfe = cset_dfe;
	      csg_s_cond (so, tb_dfe, &first_dfe, &first_s_col_dfe, s_spec);
	      csg_g_cond (so, tb_dfe, g_spec);
	      cset_ot = cset_dfe->_.table.ot;
	      if (!first_dfe)
		first_dfe = cset_dfe;
	      cset_placed = 1;
	    }
	  cset_col = sqlo_df (so, t_listst (3, COL_DOTTED, cset_ot->ot_new_prefix, csgc->csgc_col->col_name));
	  if (dk_set_member (ks->ks_out_cols, csgc->csgc_ref_col))
	    {
	      t_set_push (&cset_dfe->_.table.out_cols, cset_col);
	    }
	  if (o_mode && unbox_iri_id (o_mode[1]) == cset_col->_.col.col->col_csetp->csetp_iri)
	    any_cond =
		csg_o_scan_cond (so, cset_dfe, csgc->csgc_ref_col, ts, t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix,
		    csgc->csgc_col->col_name));
	  any_cond |=
	      csg_o_cond (so, cset_dfe, csgc->csgc_ref_col, ts, t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix,
		  csgc->csgc_col->col_name));
	  if (any_cond)
	    cset_any_reqd = 1;
	  if (!csgc->csgc_ref_col->col_is_cset_opt && !any_cond)
	    {
	      ST *nnt;
	      BIN_OP (nnt, BOP_NULL, cset_col->dfe_tree, NULL);
	      df_elt_t *cp = sqlo_df (so, nnt);
	      cset_any_reqd = 1;
	      cp->_.bin.is_not = 1;

	      t_set_push (&cset_dfe->_.table.col_preds, cp);
	    }
	  rq_dfe = csg_add_tb (so, cset->cset_rq_table->tb_primary_key);
	  csg_p_cond (so, rq_dfe, ref_col->col_cset_iri);
	  csg_s_cond (so, rq_dfe, &first_dfe, &first_s_col_dfe, s_spec);
	  csg_g_cond (so, rq_dfe, g_spec);
	  csg_o_cond (so, rq_dfe, ref_col, ts, t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix, O_NAME));
	  if (o_mode && unbox_iri_id (o_mode[1]) == ref_col->col_cset_iri)
	    csg_o_scan_cond (so, rq_dfe, csgc->csgc_ref_col, ts, t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix,
		    t_box_string ("O")));
	  if (dk_set_member (ks->ks_out_cols, csgc->csgc_ref_col))
	    {
	      ST *tree = t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix, O_NAME);
	      df_elt_t *rq_col = sqlo_df (so, tree);
	      t_set_push (&rq_dfe->_.table.out_cols, rq_col);
	    }
	}
      else
	{
	  int any_cond = 0;
	  rq_dfe = csg_add_tb (so, cset->cset_rq_table->tb_primary_key);
	  csg_p_cond (so, rq_dfe, ref_col->col_cset_iri);
	  csg_s_cond (so, rq_dfe, &first_dfe, &first_s_col_dfe, s_spec);
	  csg_g_cond (so, rq_dfe, g_spec);
	  csg_o_cond (so, rq_dfe, ref_col, ts, t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix, O_NAME));
	  if (dk_set_member (ks->ks_out_cols, csgc->csgc_ref_col))
	    {
	      ST *tree = t_listst (3, COL_DOTTED, rq_dfe->_.table.ot->ot_new_prefix, O_NAME);
	      df_elt_t *rq_col = sqlo_df (so, tree);
	      t_set_push (&rq_dfe->_.table.out_cols, rq_col);
	    }
	}
    }
  if (cset_dfe && !cset_any_reqd)
    cset_dfe->_.table.ot->ot_is_outer = 1;
  top_ot->ot_csgc = csgc_arr;
  hash_table_free (ht);
}


df_elt_t *
csg_dfe (sqlo_t * so, op_table_t * top_ot, cset_t * cset, table_source_t * ts, int mode)
{
  so->so_gen_pt = top_ot->ot_work_dfe->_.sub.first;
  switch (mode)
    {
    case CSQ_TABLE:
    case CSQ_LOOKUP:
      csg_ts_frame (so, top_ot, cset, ts);
      break;
    default:
      sqlr_new_error ("NCSMD", "NCSMD", "cset mode not implemented");
    }
  return top_ot->ot_work_dfe;
}


state_slot_t **
csg_exc_bits (sql_comp_t * sc, csg_col_t * csgc_arr, int n_in_cset)
{
  int n_ssls = _RNDUP (n_in_cset, 64) / 64;
  int inx;
  state_slot_t **ssls = (state_slot_t **) dk_alloc_box (n_ssls * sizeof (caddr_t), DV_BIN);
  for (inx = 0; inx < n_ssls; inx++)
    ssls[inx] = ssl_new_vec (sc->sc_cc, "cset_bits", DV_LONG_INT);
  return ssls;
}

search_spec_t *
csg_add_row_spec (sqlo_t * so, key_source_t * ks, search_spec_t * sp)
{
  sql_comp_t *sc = so->so_sc;
  search_spec_t *cp = sp_copy (sp);
  if (CMP_HASH_RANGE == sp->sp_min_op)
    {
      hash_range_spec_t *hrng_cp = (hash_range_spec_t *) dk_alloc (sizeof (hash_range_spec_t));
      hash_range_spec_t *hrng = (hash_range_spec_t *) cp->sp_min_ssl;
      memcpy (hrng_cp, hrng, sizeof (hash_range_spec_t));
      cp->sp_min_ssl = (state_slot_t *) hrng_cp;
      hrng_cp->hrng_ssls = (state_slot_t *) box_copy ((caddr_t) hrng_cp->hrng_ssls);
      dk_set_push (&ks->ks_hash_spec, (void *) cp);
    }
  else
    {
      cp->sp_next = ks->ks_row_spec;
      ks->ks_row_spec = cp;
      if (!sc->sc_cset_param)
	sc->sc_cset_param = hash_table_allocate (11);
      if (cp->sp_min_ssl)
	sethash ((void *) (ptrlong) cp->sp_min_ssl->ssl_index, sc->sc_cset_param, (void *) 1);
      if (cp->sp_max_ssl)
	sethash ((void *) (ptrlong) cp->sp_max_ssl->ssl_index, sc->sc_cset_param, (void *) 1);
    }
  return cp;
}


void
csg_sp_set_col (sqlo_t * so, table_source_t * ts, search_spec_t * sp, int is_rq)
{
  key_source_t *ks = ts->ts_order_ks;
  int inx;
  csg_col_t *csgc_arr = so->so_top_ot->ot_csgc;
  int n_cols = box_length (csgc_arr) / sizeof (csg_col_t);
  int nth_in_cset = 0;
  dbe_column_t *ref_col = sp->sp_col;
  for (inx = 0; inx < n_cols; inx++)
    {
      if (!is_rq && ref_col == csgc_arr[inx].csgc_col)
	{			/* cset table, col is already right */
	  if (csgc_arr[inx].csgc_col->col_is_cset_opt)
	    sp->sp_is_cset_opt = 1;
	  sp->sp_ordinal = nth_in_cset;
	  return;
	}
      if (csgc_arr[inx].csgc_ref_col == ref_col)
	{
	  if (is_rq)
	    {
	      dbe_column_t *o_col;
	      if (!stricmp (ref_col->col_name, "G"))
		o_col = tb_name_to_column (ks->ks_key->key_table, "G");
	      else
		o_col = tb_name_to_column (ks->ks_key->key_table, "O");
	      sp->sp_col = o_col;
	    }
	  else
	    sp->sp_col = csgc_arr[inx].csgc_col;
	  sp->sp_cl = *cl_list_find (ks->ks_key->key_row_var, sp->sp_col->col_id);
	  if (!is_rq && csgc_arr[inx].csgc_col->col_is_cset_opt)
	    sp->sp_is_cset_opt = 1;
	  sp->sp_ordinal = nth_in_cset;
	  return;
	}
      else if (csgc_arr[inx].csgc_col)
	nth_in_cset++;
    }
}


void
csg_cset_extra (sqlo_t * so, table_source_t * ts, table_source_t * model_ts)
{
  key_source_t *model_ks = model_ts->ts_order_ks;
  key_source_t *ks = ts->ts_order_ks;
  search_spec_t *sp = model_ks->ks_row_spec;
  for (sp = sp; sp; sp = sp->sp_next)
    if (!csg_should_add_spec (sp))
      csg_add_row_spec (so, ks, sp);
  DO_SET (search_spec_t *, sp, &model_ks->ks_hash_spec) csg_add_row_spec (so, ks, sp);
  END_DO_SET ();
  for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
    csg_sp_set_col (so, ts, sp, 0);
  DO_SET (search_spec_t *, sp, &ks->ks_hash_spec) csg_sp_set_col (so, ts, sp, 0);
  END_DO_SET ();
}

void
csa_add_ssl (cset_align_node_t * csa, state_slot_t * res_ssl, state_slot_t * first, state_slot_t * second, int n_out, int flag)
{
  int nth;
  if (!csa->csa_ssls)
    {
      int bytes = sizeof (cset_align_t) * n_out;
      csa->csa_ssls = (cset_align_t *) dk_alloc_box (bytes, DV_BIN);
      memzero (csa->csa_ssls, bytes);
    }
  for (nth = 0; nth < n_out; nth++)
    {
      if (!csa->csa_ssls[nth].csa_res)
	{
	  csa->csa_ssls[nth].csa_res = res_ssl;
	  csa->csa_ssls[nth].csa_first = first;
	  csa->csa_ssls[nth].csa_second = second;
	  csa->csa_ssls[nth].csa_flag = flag;
	  break;
	}
    }
}


void
csg_rq_o_cols (sqlo_t * so, table_source_t * model_ts, table_source_t * ts, table_source_t * cset_ts)
{
  sql_comp_t *sc = so->so_sc;
  oid_t cset_col_id = 0;
  int n_out;
  cset_align_node_t *csa;
  key_source_t *rq_ks = ts->ts_order_ks;
  iri_id_t iri = unbox_iri_id (rq_ks->ks_spec.ksp_spec_array->sp_min_ssl->ssl_constant);
  cset_mode_t *csm = ts->ts_csm;
  cset_mode_t *cset_csm = cset_ts ? cset_ts->ts_csm : NULL;
  int inx;
  dk_set_t s_iter = rq_ks->ks_out_slots;
  DO_SET (dbe_column_t *, col, &ts->ts_order_ks->ks_out_cols)
  {
    if ('O' == col->col_name[0])
      {
	ts->ts_csm->csm_rq_o = (state_slot_t *) s_iter->data;
	goto found;
      }
    s_iter = s_iter->next;
  }
  END_DO_SET ();
  return;
found:
  if (cset_ts)
    {
      s_iter = cset_ts->ts_order_ks->ks_out_slots;
      DO_SET (dbe_column_t *, col, &cset_ts->ts_order_ks->ks_out_cols)
      {
	if (col->col_csetp && iri == col->col_csetp->csetp_iri)
	  {
	    cset_col_id = col->col_id;
	    ts->ts_csm->csm_cset_o = (state_slot_t *) s_iter->data;
	    goto found2;
	  }
	s_iter = s_iter->next;
      }
      END_DO_SET ();
    found2:;
      n_out = dk_set_length (cset_ts->ts_order_ks->ks_out_slots);
      if (!cset_csm->csm_cset_col_bits)
	{
	  cset_csm->csm_cset_col_bits = (short *) dk_alloc_box_zero (sizeof (short) * n_out, DV_BIN);
	  memset (cset_csm->csm_cset_col_bits, OM_NO_CSET, box_length (cset_csm->csm_cset_col_bits));
	}
      inx = 0;
      DO_SET (dbe_column_t *, col, &cset_ts->ts_order_ks->ks_out_cols)
      {
	if (cset_col_id == col->col_id)
	  {
	    cset_csm->csm_cset_col_bits[inx] = ts->ts_csm->csm_bit;
	    break;
	  }
	inx++;
      }
      END_DO_SET ();
    }
  csa = (cset_align_node_t *) qn_next_qn ((data_source_t *) ts, (qn_input_fn) cset_align_input);
  n_out = dk_set_length (model_ts->ts_order_ks->ks_out_slots);
  if (CSQ_LOOKUP == sc->sc_csg_mode && model_ts->ts_csts->csts_posg_o_out)
    csa_add_ssl (csa, model_ts->ts_csts->csts_posg_o_out, model_ts->ts_csts->csts_o_from_posg, NULL, n_out, 0);
  s_iter = model_ts->ts_order_ks->ks_out_slots;
  DO_SET (dbe_column_t *, col, &model_ts->ts_order_ks->ks_out_cols)
  {
    if (iri == col->col_cset_iri)
      {
	state_slot_t *res_ssl = (state_slot_t *) s_iter->data;
	csa_add_ssl (csa, res_ssl, csm->csm_cset_o, csm->csm_rq_o, n_out, 0);
      }
    s_iter = s_iter->next;
  }
  END_DO_SET ();
}

state_slot_t *
csg_ts_sg_out_ssl (sqlo_t * so, table_source_t * ts, char *col_name, int add)
{
  /* return out ssl for s or g col or optionally add one */
  if (!ts)
    return NULL;
  key_source_t *ks = ts->ts_order_ks;
  state_slot_t *ssl;
  dbe_column_t *col;
  dk_set_t ssl_iter = ks->ks_out_slots;
  DO_SET (dbe_column_t *, col, &ks->ks_out_cols)
  {
    if (!stricmp (col->col_name, col_name))
      return (state_slot_t *) ssl_iter->data;
    ssl_iter = ssl_iter->next;
  }
  END_DO_SET ();
  if (!add)
    return NULL;
  col = tb_name_to_column (ks->ks_key->key_table, col_name);
  if (!col)
    sqlc_new_error (so->so_sc->sc_cc, "CSETC", "CSETC", "No column s or g in cset or rq table, internal");
  ssl = ssl_new_column (so->so_sc->sc_cc, "cs", col);
  dk_set_push (&ks->ks_out_cols, (void *) col);
  dk_set_push (&ks->ks_out_slots, (void *) ssl);
  return ssl;
}


void
csg_sg_out (sqlo_t * so, cset_align_node_t * csa, table_source_t * model_ts)
{
  /* if the model ts returns s or g add these to the cset ts.  The 2nd is in principle the s from the first exception ts in a scan */
  state_slot_t *first = NULL, *second = NULL;
  int n_out = dk_set_length (model_ts->ts_order_ks->ks_out_slots);
  table_source_t *rq_ts = NULL;
  table_source_t *cset_ts = NULL;
  state_slot_t *s_out = csg_ts_sg_out_ssl (so, model_ts, "S", 0);
  state_slot_t *g_out = csg_ts_sg_out_ssl (so, model_ts, "G", 0);
  table_source_t *ts;
  query_t *qr = so->so_sc->sc_cc->cc_query;
  for (ts = (table_source_t *) qr->qr_head_node; ts; ts = (table_source_t *) qn_next ((data_source_t *) ts))
    {
      if (IS_TS (ts))
	{
	  if (tb_is_rdf_quad (ts->ts_order_ks->ks_key->key_table))
	    {
	      rq_ts = ts;
	      break;
	    }
	  else
	    cset_ts = ts;
	}
    }
  if (s_out)
    {
      if (cset_ts)
	first = csg_ts_sg_out_ssl (so, cset_ts, "S", 1);
      if (rq_ts)
	second = csg_ts_sg_out_ssl (so, rq_ts, "S", 1);
      if (first || second)
	csa_add_ssl (csa, s_out, first, second, n_out, CSA_S);
    }
  first = second = NULL;
  if (g_out)
    {
      if (cset_ts)
	first = csg_ts_sg_out_ssl (so, cset_ts, "G", 1);
      if (rq_ts)
	second = csg_ts_sg_out_ssl (so, rq_ts, "G", 1);
      if (first || second)
	csa_add_ssl (csa, g_out, first, second, n_out, CSA_G);
    }
}


void
csg_rq_top_oby (sqlo_t * so, table_source_t * ts, table_source_t * model_ts)
{
  key_source_t *ks = ts->ts_order_ks;
  key_source_t *model_ks = model_ts->ts_order_ks;
  ks->ks_top_oby_spec = sp_copy (model_ks->ks_top_oby_spec);
  ks->ks_top_oby_col = tb_name_to_column (ks->ks_key->key_table, "O");
  ks->ks_top_oby_spec->sp_col = ks->ks_top_oby_col;
  ks->ks_top_oby_spec->sp_cl = *cl_list_find (ks->ks_key->key_row_var, ks->ks_top_oby_col->col_id);
  ks->ks_top_oby_setp = model_ks->ks_top_oby_setp;
  ks->ks_top_oby_top_setp = model_ks->ks_top_oby_top_setp;
  ks->ks_top_oby_cnt = model_ks->ks_top_oby_cnt;
  ks->ks_top_oby_skip = model_ks->ks_top_oby_skip;
  ks->ks_top_oby_nth = model_ks->ks_top_oby_nth;
}


void
csg_cset_top_oby (sqlo_t * so, table_source_t * ts, table_source_t * model_ts)
{
  key_source_t *ks = ts->ts_order_ks;
  key_source_t *model_ks = model_ts->ts_order_ks;
  dbe_column_t *col = NULL;
  csg_col_t *csgc_arr = so->so_top_ot->ot_csgc;
  int n_cols = box_length (csgc_arr) / sizeof (csg_col_t), inx;
  for (inx = 0; inx < n_cols; inx++)
    {
      if (csgc_arr[inx].csgc_ref_col->col_cset_iri == model_ks->ks_top_oby_col->col_cset_iri)
	{
	  if (!(col = csgc_arr[inx].csgc_col))
	    return;
	  break;
	}
    }
  ks->ks_top_oby_spec = sp_copy (model_ks->ks_top_oby_spec);
  ks->ks_top_oby_col = col;
  ks->ks_top_oby_spec->sp_col = ks->ks_top_oby_col;
  ks->ks_top_oby_spec->sp_cl = *cl_list_find (ks->ks_key->key_row_var, ks->ks_top_oby_col->col_id);
  ks->ks_top_oby_setp = model_ks->ks_top_oby_setp;
  ks->ks_top_oby_top_setp = model_ks->ks_top_oby_top_setp;
  ks->ks_top_oby_cnt = model_ks->ks_top_oby_cnt;
  ks->ks_top_oby_skip = model_ks->ks_top_oby_skip;
  ks->ks_top_oby_nth = model_ks->ks_top_oby_nth;

}


void
csg_rq_set_exc_scan (sql_comp_t * sc, table_source_t * ts, table_source_t * cset_ts)
{
  search_spec_t *s_row_sp;
  cset_mode_t *csm = ts->ts_csm;
  key_source_t *exc_ks = (key_source_t *) dk_alloc (sizeof (key_source_t));
  NEW_VARZ (search_spec_t, s_sp);
  *exc_ks = *ts->ts_order_ks;
  csm->csm_cset_scan_state = cset_ts->ts_csm->csm_cset_scan_state;
  csm->csm_exc_scan_ks = exc_ks;
  *s_sp = *ts->ts_order_ks->ks_spec.ksp_spec_array;
  exc_ks->ks_row_spec = sp_list_copy (exc_ks->ks_row_spec);
  exc_ks->ks_hash_spec = dk_set_copy (exc_ks->ks_hash_spec);
  if (exc_ks->ks_top_oby_spec)
    exc_ks->ks_top_oby_spec = sp_copy (exc_ks->ks_top_oby_spec);
  if (s_sp->sp_next)
    {
      search_spec_t *g_sp = sp_copy (s_sp->sp_next);
      g_sp->sp_next = exc_ks->ks_row_spec;
      exc_ks->ks_row_spec = g_sp;
    }
  s_sp->sp_next = NULL;
  s_sp->sp_min_op = CMP_GT;
  s_sp->sp_max_op = CMP_LT;
  s_sp->sp_min_ssl = csm->csm_exc_scan_lower = ssl_new_scalar (sc->sc_cc, "exc_lower", DV_IRI_ID);
  s_sp->sp_max_ssl = csm->csm_exc_scan_upper = ssl_new_scalar (sc->sc_cc, "exc_upper", DV_IRI_ID);
  exc_ks->ks_spec.ksp_spec_array = s_sp;
  s_row_sp = sp_copy (s_sp);
  s_row_sp->sp_next = exc_ks->ks_row_spec;
  exc_ks->ks_row_spec = s_row_sp;
  {
    NEW_VARZ (search_spec_t, s_exc_sp);
    s_exc_sp->sp_min_op = CMP_ORD_NOT_IN;
    csm->csm_exc_scan_exc_s = s_exc_sp->sp_min_ssl = ssl_new_vec (sc->sc_cc, "exc_s_set", DV_IRI_ID);
    s_exc_sp->sp_next = exc_ks->ks_row_spec;
    exc_ks->ks_row_spec = s_exc_sp;
  }
  ks_set_search_params (NULL, NULL, exc_ks);
}


void
csg_rq_set_cycle (sql_comp_t * sc, table_source_t * rq_ts)
{
  cset_mode_t *csm = rq_ts->ts_csm;
  if (-1 != csm->csm_bit)
    {
      if (rq_ts->ts_is_outer)
	ts_set_cycle (sc, rq_ts, 1, (qn_input_fn *) list (3, cset_psog_cset_values, cset_psog_input, psog_outer_nulls));
      else if (csm->csm_cset_scan_state)
	ts_set_cycle (sc, rq_ts, 1, (qn_input_fn *) list (3, cset_psog_cset_values, cset_psog_input, psog_cset_scan_exceptions));
      else
	ts_set_cycle (sc, rq_ts, 1, (qn_input_fn *) list (2, cset_psog_cset_values, cset_psog_input));
    }
  else if (rq_ts->ts_is_outer)
    ts_set_cycle (sc, rq_ts, 1, (qn_input_fn *) list (2, cset_psog_input, psog_outer_nulls));
  else
    ts_set_cycle (sc, rq_ts, 1, (qn_input_fn *) list (1, cset_psog_input));
  {
  }
}


void
csg_extra_specs (sqlo_t * so, cset_t * cset, query_t * qr, table_source_t * model_ts)
{
  cset_align_node_t *csa;
  search_spec_t *sp;
  table_source_t *cset_ts = NULL;
  key_source_t *model_ks = model_ts->ts_order_ks;
  csg_col_t *csgc_arr = so->so_top_ot->ot_csgc;
  csg_col_t *csgc;
  int nth_rq = 0, n_in_cset = 0, inx, is_first = 1, any_rq = 0;
  sql_comp_t *sc = so->so_sc;
  int is_rq;
  int n_bits = 0;
  int n_cols = box_length (csgc_arr) / sizeof (csg_col_t);
  cset_ts_t *csts = model_ts->ts_csts;
  ssl_index_t tmp_bits = cc_new_instance_slot (sc->sc_cc);
  state_slot_t **exc_bits = NULL;
  data_source_t *qn;
  for (inx = 0; inx < n_cols; inx++)
    if (csgc_arr[inx].csgc_col)
      n_in_cset++;
  for (qn = qr->qr_head_node; qn; qn = qn_next (qn))
    {
      if (IS_QN (qn, set_ctr_input) || IS_QN (qn, outer_seq_end_input))
	continue;
      if (IS_TS (qn))
	{
	  QNCAST (table_source_t, ts, qn);
	  key_source_t *ks = ts->ts_order_ks;
	  cset_mode_t *csm = NULL;
	  int rq_in_cset = 0;
	  is_rq = tb_is_rdf_quad (ks->ks_key->key_table);
	  csgc = is_rq ? &csgc_arr[nth_rq] : NULL;
	  NEW_VARZ (cset_mode_t, csm2);
	  csm = csm2;
	  csm->csm_cset_bit_bytes = ALIGN_8 (n_in_cset + 1) / 8;
	  csm->csm_reqd_ps = (iri_id_t *) box_copy ((caddr_t) csts->csts_reqd_ps);
	  if (is_rq)
	    any_rq = 1;
	  csm->csm_mode = cc_new_instance_slot (sc->sc_cc);
	  if (is_first)
	    {
#define TSCP(m) ts->m = model_ts->m
#define TSCP_B(m)  *(caddr_t*)&ts->m = box_copy ((caddr_t)model_ts->m)
	      TSCP (ts_cardinality);
	      TSCP (ts_inx_cardinality);
	      TSCP (ts_cost_after);
	      TSCP (ts_cost);
	      TSCP (ts_aq);
	      TSCP (ts_aq_qis);
	      TSCP_B (ts_branch_ssls);
	      TSCP_B (ts_sdfg_params);
	      TSCP_B (ts_sdfg_param_refs);
	      TSCP (ts_qf);
	      TSCP_B (ts_branch_sets);
	      TSCP (ts_branch_col);
	      TSCP (ts_agg_node);
	      TSCP (ts_in_sdfg);
	      TSCP (ts_aq_state);
	      is_first = 0;
	    }
	  csm->csm_cset = cset;
	  ts->ts_csm = csm;
	  rq_in_cset = is_rq && csgc->csgc_col;
	  if (rq_in_cset || !is_rq)
	    {
	      csm->csm_exc_tmp_bits = tmp_bits;
	      csm->csm_exc_bits_in = (state_slot_t **) box_copy ((caddr_t) exc_bits);
	      exc_bits = csm->csm_exc_bits_out = csg_exc_bits (sc, csgc, n_in_cset);
	      csm->csm_n_bits = 64 * BOX_ELEMENTS (exc_bits);
	    }
	  if (is_rq && model_ks->ks_top_oby_col && model_ks->ks_top_oby_col->col_cset_iri == csgc->csgc_ref_col->col_cset_iri)
	    csg_rq_top_oby (so, ts, model_ts);
	  if (!is_rq && model_ks->ks_top_oby_col)
	    csg_cset_top_oby (so, ts, model_ts);
	  if (is_rq && !rq_in_cset)
	    csm->csm_bit = -1;
	  if (!is_rq)
	    {
	      csm->csm_role = TS_CSET_SG;
	      csm->csm_at_end = cc_new_instance_slot (sc->sc_cc);
	      csg_cset_extra (so, ts, model_ts);
	      ts->ts_set_card_bits = cc_new_sets_slot (sc->sc_cc);
	      cset_ts = ts;
	      csm->csm_exc_s = ssl_new_vec (sc->sc_cc, "exc_s", DV_IRI_ID);
	      csm->csm_exc_s_row = cc_new_instance_slot (sc->sc_cc);
	      if (CSQ_TABLE == sc->sc_csg_mode && !any_rq)
		{
		  /* if cset table is first, then need case for scan where s in rq and not in cset */
		  csm->csm_cset_scan_state = cc_new_instance_slot (sc->sc_cc);
		  ts->ts_branch_by_value = 1;
		}
	    }
	  else
	    {
	      key_source_t *ks = ts->ts_order_ks;
	      ts->ts_csm->csm_role = TS_CSET_PSOG;
	      if (rq_in_cset)
		{
		  csm->csm_bit = n_bits;
		  csm->csm_n_from_cset = cc_new_instance_slot (sc->sc_cc);
		  n_bits++;
		}
	      csg_rq_o_cols (so, model_ts, ts, cset_ts);
	      if (csgc->csgc_ref_col->col_is_cset_opt)
		{
		  ts->ts_set_card_bits = cc_new_instance_slot (sc->sc_cc);
		  ts->ts_csm->csm_last_outer = cc_new_instance_slot (sc->sc_cc);
		  ts->ts_is_outer = 1;
		}
	      for (sp = model_ks->ks_row_spec; sp; sp = sp->sp_next)
		if (csgc->csgc_ref_col->col_id == sp->sp_cl.cl_col_id && !csg_should_add_spec (sp))
		  csg_add_row_spec (so, ks, sp);
	      DO_SET (search_spec_t *, sp, &model_ks->ks_hash_spec) if (csgc->csgc_ref_col->col_id == sp->sp_cl.cl_col_id)
		csg_add_row_spec (so, ks, sp);
	      END_DO_SET ();
	      for (sp = ks->ks_row_spec; sp; sp = sp->sp_next)
		csg_sp_set_col (so, ts, sp, 1);
	      DO_SET (search_spec_t *, sp, &ks->ks_hash_spec) csg_sp_set_col (so, ts, sp, 1);
	      END_DO_SET ();
	      nth_rq++;
	    }
	  ks_set_search_params (NULL, NULL, ts->ts_order_ks);
	  if (is_rq && 0 == csm->csm_bit && n_bits && CSQ_TABLE == sc->sc_csg_mode && cset_ts->ts_csm->csm_cset_scan_state)
	    csg_rq_set_exc_scan (sc, ts, cset_ts);
	  if (is_rq)
	    csg_rq_set_cycle (sc, ts);
	}
    }
  if (cset_ts)
    cset_ts->ts_csm->csm_bit = n_bits;
  csa = (cset_align_node_t *) qn_next_qn (qr->qr_head_node, (qn_input_fn) cset_align_input);
  csg_sg_out (so, csa, model_ts);
  csa->csa_model_ts = model_ts;
  csa->csa_exc_bits = exc_bits;
  csa->csa_no_cset_bit = cset_ts ? cset_ts->ts_csm->csm_bit : -1;
}


query_t *
csg_top (sql_comp_t * sc, cset_t * cset, table_source_t * ts, int mode)
{
  df_elt_t *best;
  t_NEW_VARZ (sqlo_t, so);
  t_NEW_VARZ (op_table_t, top_ot);
  int inx;
  cset_ts_t *csts = ts->ts_csts;
  comp_context_t *cc = sc->sc_cc;
  cc->cc_in_cset_gen = 1;
  for (inx = BOX_ELEMENTS (csts->csts_vec_ssls) - 1; inx >= 0; inx--)
    {
      dk_set_push (&cc->cc_qi_vec_places, (void *) (ptrlong) csts->csts_vec_ssls[inx]->ssl_index);
      dk_set_push (&cc->cc_qi_box_places, (void *) (ptrlong) csts->csts_vec_ssls[inx]->ssl_box_index);
    }

  for (inx = BOX_ELEMENTS (csts->csts_scalar_ssls) - 1; inx >= 0; inx--)
    {
      dk_set_push (&cc->cc_qi_scalar_places, (void *) (ptrlong) csts->csts_scalar_ssls[inx]->ssl_index);
    }
  for (inx = (box_length (csts->csts_qi_places) / sizeof (ssl_index_t)) - 1; inx >= 0; inx--)
    dk_set_push (&cc->cc_qi_places, (void *) (uptrlong) csts->csts_qi_places[inx]);
  for (inx = (box_length (csts->csts_qi_sets) / sizeof (ssl_index_t)) - 1; inx >= 0; inx--)
    dk_set_push (&cc->cc_qi_sets_places, (void *) (uptrlong) csts->csts_qi_sets[inx]);
  so->so_is_select = 1;
  so->so_sc = sc;
  sc->sc_so = so;
  so->so_df_elts = sqlo_allocate_df_elts (201);
  top_ot->ot_dfe = sqlo_df (so, top_ot->ot_dt);
  top_ot->ot_work_dfe = dfe_container (so, DFE_DT, NULL);
  top_ot->ot_work_dfe->_.sub.ot = top_ot;
  top_ot->ot_work_dfe->_.sub.in_arity = 1;
  top_ot->ot_work_dfe->dfe_locus = LOC_LOCAL;
  top_ot->ot_is_cset_dt = 1;
  top_ot->ot_new_prefix = t_box_string ("ct0");
  so->so_name_ctr = 1;
  so->so_dfe = top_ot->ot_work_dfe;
  so->so_top_ot = top_ot;
  best = csg_dfe (so, top_ot, cset, ts, mode);
  so->so_sc->sc_delay_colocate = 1;	/* in cluster, the cset expansion is all colocated with the cset model ts */
  sqlg_top (so, best);
  csg_extra_specs (so, cset, so->so_sc->sc_cc->cc_query, ts);
  return sc->sc_cc->cc_query;
}


query_t *
csg_query (cset_t * cset, table_source_t * ts, int mode, caddr_t * o_mode, caddr_t * err)
{
  client_connection_t *cli = sqlc_client ();
  caddr_t cc_error;
  du_thread_t *self = THREAD_CURRENT_THREAD;
  struct TLSF_struct *save_tlsf = self->thr_tlsf;
  SCS_STATE_FRAME;
  comp_context_t cc;
  sql_comp_t sc;
  query_t *volatile qr;
  client_connection_t *old_cli = sqlc_client ();
  thr_set_tlsf (THREAD_CURRENT_THREAD->thr_tlsf, sqlc_tlsf);
  DK_ALLOC_QUERY (qr);
  memset (&sc, 0, sizeof (sc));
  CC_INIT (cc, cli);
  sc.sc_cc = &cc;
  sc.sc_client = cli;
  cc.cc_query = qr;
  sc.sc_csg_mode = mode;
  sc.sc_csg_o_mode = o_mode;
  sc.sc_csg_cset = cset;
  sql_warnings_clear ();
  MP_START ();
  mp_comment (THR_TMP_POOL, "cset_compile ", "");
  SCS_STATE_PUSH;
  sqlc_set_client (cli);
  top_sc = &sc;
  sql_err_state[0] = 0;
  sql_err_native[0] = 0;
  sqlp_bin_op_serial = 0;
  qr->qr_qualifier = box_string (sqlc_client ()->cli_qualifier);
  qr->qr_owner = box_string (CLI_OWNER (sqlc_client ()));

  CATCH (CATCH_LISP_ERROR)
  {
    query_t *qr;
    csg_top (&sc, cset, ts, mode);
    qr = sc.sc_cc->cc_query;
    qr_set_local_code_and_funref_flag (qr);
    qr->qr_proc_vectored = QR_VEC_STMT;
    t_set_push (&sc.sc_vec_pred, (void *) ts);
    sqlg_vector (&sc, qr);
    qr->qr_head_node->src_prev = ts->src_gen.src_prev;
  }
  THROW_CODE
  {
    qr_free (qr);
    qr = NULL;
    cc_error = (caddr_t) THR_ATTR (THREAD_CURRENT_THREAD, TA_SQLC_ERROR);
    *err = cc_error;
  }
  END_CATCH;
  sqlc_set_client (old_cli);
  SCS_STATE_POP;
  sc_free (&sc);
  MP_DONE ();
  if (qr)
    qr->qr_is_complete = 1;
  self->thr_tlsf = save_tlsf;
  return qr;
}


void
csts_free (cset_ts_t * csts)
{
  DO_HT (ptrlong, flags, query_t *, qr, &csts->csts_cset_plan)
  {
    data_source_t *qn;
    for (qn = qr->qr_head_node; qn; qn = qn_next (qn))
      {
	if (IS_TS (qn))
	  {
	    QNCAST (table_source_t, ts, qn);
	    cset_mode_t *csm = ts->ts_csm;
	    if (csm)
	      {
		dk_free_box ((caddr_t) csm->csm_reqd_ps);
		dk_free_box ((caddr_t) csm->csm_exc_bits_in);
		dk_free_box ((caddr_t) csm->csm_exc_bits_out);
		ts->ts_csm = NULL;
		dk_free ((caddr_t) csm, sizeof (cset_mode_t));
#define TSCLR(m) ts->m = NULL;
		TSCLR (ts_aq);
		TSCLR (ts_aq_qis);
		TSCLR (ts_branch_ssls);
		TSCLR (ts_sdfg_params);
		TSCLR (ts_sdfg_param_refs);
		TSCLR (ts_qf);
		TSCLR (ts_branch_sets);
		TSCLR (ts_branch_col);
	      }
	  }
      }
    qr_free (qr);
  }
  END_DO_HT;
  hash_table_destroy (&csts->csts_cset_plan);
  dk_mutex_destroy (&csts->csts_mtx);
  dk_free ((caddr_t) csts, sizeof (cset_ts_t));
}

void
csg_init ()
{
  csg_s_uname = box_dv_short_string ("S");
  csg_g_uname = box_dv_short_string ("G");
  csg_o_uname = box_dv_short_string ("O");
  csg_p_uname = box_dv_short_string ("P");
  csg_s_param_uname = box_string ("__s_param");
  csg_s_param_2_uname = box_string ("__s_param_2");
}
