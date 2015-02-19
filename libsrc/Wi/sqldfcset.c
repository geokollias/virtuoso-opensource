/*
 *  sqldfcset.c
 *
 *  $Id$
 *
 *  sql expression dependencies and code layout
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

#include <string.h>
#include "Dk.h"
#include "Dk/Dkpool.h"
#include "libutil.h"
#include "sqlnode.h"
#include "eqlcomp.h"
#include "sqlfn.h"
#include "lisprdr.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlbif.h"
#include "arith.h"
#include "security.h"
#include "sqlpfn.h"
#include "sqlo.h"
#include "list2.h"
#include "sqloinv.h"
#include "rdfinf.h"
#include "mhash.h"


void
st_hash_print (id_hash_t * ht)
{
  DO_IDHASH (ST *, t1, ST *, t2, ht)
  {
    sqlo_box_print ((caddr_t) t1);
    printf ("-> ");
    sqlo_box_print ((caddr_t) t2);
  }
  END_DO_IDHASH;
}


iri_id_t
sqlo_fixed_p (df_elt_t * dt_dfe, df_elt_t * tb_dfe)
{
  caddr_t ctx_name = sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_RDF_INFERENCE);
  dbe_column_t *p_col = tb_name_to_column (tb_dfe->_.table.ot->ot_table, "P");
  dk_set_t preds = tb_dfe->_.table.ot->ot_is_outer ? tb_dfe->_.table.ot->ot_join_preds : dt_dfe->_.sub.ot->ot_preds;
  caddr_t p_box = NULL;
  DO_SET (df_elt_t *, pred, &preds)
  {
    if (dfe_is_eq_pred (pred) && pred->dfe_tables && !pred->dfe_tables->next &&
	tb_dfe->_.table.ot == (op_table_t *) pred->dfe_tables->data)
      {
	if (DFE_COLUMN == pred->_.bin.left->dfe_type && p_col == pred->_.bin.left->_.col.col)
	  {
	    p_box = (caddr_t) pred->_.bin.right->dfe_tree;
	    if (DV_IRI_ID == DV_TYPE_OF (p_box))
	      break;
	    return 0;
	  }
      }
  }
  END_DO_SET ();
  if (ctx_name)
    {
      rdf_inf_ctx_t *ric = rdf_name_to_ctx (ctx_name);
      if (ric)
	{
	  rdf_sub_t *sub = ric_iri_to_sub (ric, p_box, RI_SUBPROPERTY, 0);
	  if (sub && sub->rs_sub)
	    return 0;
	}
    }
  return unbox_iri_id (p_box);
}


int
st_is_ss_eq (ST * tree)
{
  return ST_P (tree, BOP_EQ) && ST_P (tree->_.bin_exp.left, COL_DOTTED) && ST_P (tree->_.bin_exp.right, COL_DOTTED)
      && !stricmp ("S", tree->_.bin_exp.left->_.col_ref.name) && !stricmp ("S", tree->_.bin_exp.right->_.col_ref.name);
}

int
dfe_is_ss_eq (df_elt_t * pred)
{
  return (dfe_is_eq_pred (pred) && DFE_COLUMN == pred->_.bin.left->dfe_type
      && DFE_COLUMN == pred->_.bin.right->dfe_type && pred->_.bin.left->_.col.col == pred->_.bin.right->_.col.col
      && 'S' == pred->_.bin.left->_.col.col->col_name[0]);
}

void
sqlo_dfe_csets (sqlo_t * so, df_elt_t * dt_dfe)
{
  /* cset candidates are rdf quad dfes that have a p that is a cset p */
  static caddr_t s_str;
  df_elt_t *s_dfe;
  if (!s_str)
    s_str = box_dv_short_string ("S");
  DO_SET (df_elt_t *, tb_dfe, &dt_dfe->_.sub.ot->ot_from_dfes)
  {
    if (DFE_TABLE == tb_dfe->dfe_type && tb_is_rdf_quad (tb_dfe->_.table.ot->ot_table)
	&& !sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_NO_DT_INLINE)
	&& !sqlo_opt_value (tb_dfe->_.table.ot->ot_opts, OPT_SELF))
      {
	iri_id_t fixed_p = sqlo_fixed_p (dt_dfe, tb_dfe);
	dk_set_t cset_ps = gethash ((void *) fixed_p, &p_to_csetp_list);
	if (!cset_ps)
	  continue;
	tb_dfe->_.table.ot->ot_fixed_p = fixed_p;
	DO_SET (cset_p_t *, csetp, &cset_ps) t_set_pushnew (&tb_dfe->_.table.ot->ot_csets, csetp->csetp_cset);
	END_DO_SET ();
	s_dfe = sqlo_df_elt (so, t_listst (3, COL_DOTTED, tb_dfe->_.table.ot->ot_new_prefix, s_str));
      }
  }
  END_DO_SET ();
}


int
dfe_is_s_col (df_elt_t * dfe, df_elt_t * tb_dfe)
{
  return DFE_COLUMN == dfe->dfe_type && (!tb_dfe || (void *) tb_dfe->_.table.ot == dfe->dfe_tables->data)
      && dfe->_.col.col && !stricmp ("S", dfe->_.col.col->col_name);
}


void
ot_add_s_eqs (op_table_t * ot, dk_set_t preds)
{
  DO_SET (df_elt_t *, pred, &preds)
      if (PRED_IS_EQ (pred) && (dfe_is_s_col (pred->_.bin.left, NULL) || dfe_is_s_col (pred->_.bin.right, NULL)))
    t_set_push (&ot->ot_s_eqs, (void *) pred);
  END_DO_SET ();
}


void
ot_set_s_eqs (op_table_t * ot)
{
  DO_SET (df_elt_t *, oj, &ot->ot_from_dfes) if (DFE_TABLE == oj->dfe_type && oj->_.table.ot->ot_is_outer)
    ot_add_s_eqs (ot, oj->_.table.ot->ot_join_preds);
  END_DO_SET ();
  ot_add_s_eqs (ot, ot->ot_preds);
}


df_elt_t *
pred_s_value (df_elt_t * pred, df_elt_t * tb_dfe)
{
  /* if pred gives a value to s of tb, return this */
  if (dk_set_member (pred->dfe_tables, (void *) tb_dfe->_.table.ot))
    {
      if (dfe_is_s_col (pred->_.bin.left, tb_dfe))
	return pred->_.bin.right;
      if (dfe_is_s_col (pred->_.bin.right, tb_dfe))
	return pred->_.bin.left;
    }
  return NULL;
}


void
sqlo_add_s_eq (dk_set_t * res, df_elt_t * col)
{
  if (DFE_COLUMN == col->dfe_type && col->dfe_tables && 'S' == col->_.col.col->col_name[0])
    t_set_pushnew (res, ((op_table_t *) col->dfe_tables->data)->ot_dfe);
}


void
sqlo_add_ind_s_eqs (df_elt_t * dt_dfe, df_elt_t * tb_dfe, dk_set_t * res)
{
  /* add the tables whose s equal x where s of tb_dfe = x */
  op_table_t *ot = dt_dfe->_.sub.ot;
  DO_SET (df_elt_t *, pred, &ot->ot_s_eqs)
  {
    df_elt_t *s_dfe = pred_s_value (pred, tb_dfe);
    if (s_dfe)
      {
	DO_SET (df_elt_t *, other_s, &ot->ot_s_eqs)
	{
	  if (other_s == pred)
	    continue;
	  if (other_s->_.bin.left == s_dfe)
	    sqlo_add_s_eq (res, other_s->_.bin.right);
	  if (other_s->_.bin.right == s_dfe)
	    sqlo_add_s_eq (res, other_s->_.bin.left);
	}
	END_DO_SET ();
      }
  }
  END_DO_SET ();
}


dk_set_t
sqlo_s_eqs (df_elt_t * dt_dfe, df_elt_t * tb_dfe, dk_set_t res)
{
  sqlo_add_ind_s_eqs (dt_dfe, tb_dfe, &res);
  DO_SET (df_elt_t *, oj, &dt_dfe->_.sub.ot->ot_from_dfes)
  {
    if (DFE_TABLE == oj->dfe_type && oj->_.table.ot->ot_is_outer && oj->_.table.ot->ot_csets)
      {
	DO_SET (df_elt_t *, pred, &oj->_.table.ot->ot_join_preds)
	{
	  if (dk_set_member (pred->dfe_tables, (void *) tb_dfe->_.table.ot) && dfe_is_ss_eq (pred))
	    t_set_pushnew (&res, (void *) oj);
	}
	END_DO_SET ();
      }
  }
  END_DO_SET ();
  DO_SET (df_elt_t *, pred, &dt_dfe->_.sub.ot->ot_preds)
  {
    op_table_t *other;
    if (!dfe_is_ss_eq (pred) || !dk_set_member (pred->dfe_tables, (void *) tb_dfe->_.table.ot))
      continue;
    other =
	tb_dfe->_.table.ot ==
	(op_table_t *) pred->dfe_tables->data ? (op_table_t *) pred->dfe_tables->next->data : (op_table_t *) pred->dfe_tables->data;
    t_set_pushnew (&res, (void *) other->ot_dfe);
  }
  END_DO_SET ();
  return res;
}

float
cset_count (cset_t * cset)
{
  if (cset->cset_table)
    return dbe_key_count (cset->cset_table->tb_primary_key);
  return 1;
}


void
cset_tbs_cset (sqlo_t * so, df_elt_t * dt_dfe, dk_set_t * cset_tbs)
{
  /* given a set of quads joined on s eq, pick the cset that covers most */
  int best_len = 0;
  cset_t *best = NULL;
  dk_hash_t *cset_tbs_ht = hash_table_allocate (11);
  dk_hash_t *p_to_tb = hash_table_allocate (11);
  DO_SET (df_elt_t *, tb_dfe, cset_tbs)
  {
    iri_id_t fixed_p = tb_dfe->_.table.ot->ot_fixed_p;
    df_elt_t *prev_dfe = gethash ((void *) fixed_p, p_to_tb);
    if (!prev_dfe)
      sethash ((void *) fixed_p, p_to_tb, (void *) tb_dfe);
    else
      {
	dk_set_t ign = NULL;
	/* the p occurs twice in the cset, leave the other */
	int sc1 = dfe_join_score (so, dt_dfe->_.sub.ot, tb_dfe, &ign);
	int sc2 = dfe_join_score (so, dt_dfe->_.sub.ot, prev_dfe, &ign);
	if (sc2 > sc1)
	  t_set_delete (cset_tbs, (void *) tb_dfe);
	else
	  t_set_delete (cset_tbs, (void *) tb_dfe);
      }
  }
  END_DO_SET ();
  DO_SET (df_elt_t *, tb_dfe, cset_tbs)
  {
    op_table_t *ot = tb_dfe->_.table.ot;
    DO_SET (cset_t *, cset, &ot->ot_csets)
    {
      dk_set_t *tbs = gethash ((void *) cset, cset_tbs_ht);
      sethash ((void *) cset, cset_tbs_ht, CONS (tb_dfe, tbs));
    }
    END_DO_SET ();
  }
  END_DO_SET ();
  DO_HT (cset_t *, cset, dk_set_t, tbs, cset_tbs_ht)
  {
    int len = dk_set_length (tbs);
    if (len > best_len)
      {
	best_len = len;
	best = cset;
      }
    else if (len == best_len)
      {
	if (best->cset_table && cset->cset_table && cset_count (cset) > cset_count (best))
	  best = cset;
      }
  }
  END_DO_HT;
  t_set_push (cset_tbs, best);
  hash_table_free (cset_tbs_ht);
  hash_table_free (p_to_tb);
}


dbe_column_t *
cset_new_col (dbe_table_t * tb, caddr_t name, dtp_t dtp, int nn, int is_opt)
{
  NEW_VARZ (dbe_column_t, col);
  NCONCF1 (tb->tb_primary_key->key_parts, col);
  col->col_id = dk_set_length (tb->tb_primary_key->key_parts);
  col->col_sqt.sqt_dtp = dtp;
  col->col_sqt.sqt_col_dtp = dtp;
  col->col_sqt.sqt_non_null = nn;
  col->col_name = box_dv_short_string (name);
  col->col_is_cset_opt = is_opt;
  id_hash_set (tb->tb_name_to_col, (caddr_t) & col->col_name, (caddr_t) & col);
  col->col_defined_in = tb;
  return col;
}

dk_set_t
dk_set_union (dk_set_t s1, dk_set_t s2)
{
  DO_SET (void *, x, &s2);
  dk_set_pushnew (&s1, x);
  END_DO_SET ();
  return s1;
}


void
sqlo_cset_partition (dbe_table_t * tb)
{
  /* the abstract table is partitioned pon s like rdf_quad */
  cset_t *cset = tb->tb_closest_cset;
  key_partition_def_t *kpd;
  if (CL_RUN_LOCAL == cl_run_local_only)
    return;
  kpd = kpd_copy (cset->cset_rq_table->tb_primary_key->key_partition);
  tb->tb_primary_key->key_partition = kpd;
  kpd->kpd_cols[0]->cp_col_id = 1;
}


dk_set_t
cset_temp_tables (sqlo_t * so, dk_set_t cset_tbs)
{
  /* the list starts with the best cset, followed by the quad tb dfes  for the cols */
  sql_comp_t *sc = so->so_sc;
  dbe_column_t *col;
  int found;
  float p_stat[4];
  dk_set_t res = NULL;
  DO_SET (dk_set_t, cols, &cset_tbs)
  {
    cset_t *best_cset = (cset_t *) cols->data;
    char tmp[MAX_QUAL_NAME_LEN + 100];
    dbe_key_t *key = (dbe_key_t *) dk_alloc (sizeof (dbe_key_t));
    NEW_VARZ (dbe_table_t, tb);
    memzero (key, sizeof (dbe_key_t));
    key->key_id = KI_TEMP;
    key->key_table = tb;
    key->key_is_primary = 1;
    tb->tb_primary_key = key;
    dk_set_push (&tb->tb_keys, (void *) key);
    key->key_is_cset_temp = 1;
    key->key_is_col = 1;
    key->key_n_significant = 2;
    tb->tb_name_to_col = id_str_hash_create (11);
    snprintf (tmp, sizeof (tmp), "%s_cv_%d", best_cset->cset_table->tb_name, so->so_name_ctr++);
    tb->tb_name = box_dv_short_string (tmp);
    tb->tb_closest_cset = best_cset;
    tb->tb_name_only = strrchr (tb->tb_name, '.') + 1;
    key->key_name = box_dv_short_string (tmp);
    dk_set_push (&top_sc->sc_temp_tables, (void *) tb);
    tb->tb_temp_id = dk_set_length (top_sc->sc_temp_tables);
    top_sc->sc_cc->cc_query->qr_temp_tables = top_sc->sc_temp_tables;
    cset_new_col (tb, "S", DV_IRI_ID_8, 1, 0);
    cset_new_col (tb, "G", DV_IRI_ID_8, 1, 0);
    sqlo_cset_partition (tb);
    snprintf (tmp, sizeof (tmp), "c%d", so->so_name_ctr++);
    key->key_cset_prefix = box_dv_short_string (tmp);
    t_set_push (&res, (void *) t_list (3, TABLE_REF, t_list (5, TABLE_DOTTED, tb->tb_name, key->key_cset_prefix, NULL, NULL, NULL),
	    NULL, NULL));
    DO_SET (df_elt_t *, rq_dfe, &cols->next)
    {
      ST *new_col_ref, *prev_col_ref;
      op_table_t *ot = rq_dfe->_.table.ot;
      dk_set_t csplist = (dk_set_t) gethash ((void *) ot->ot_fixed_p, &p_to_csetp_list);
      cset_p_t *csetp = (cset_p_t *) csplist->data;
      found = ric_p_stat_from_cache (dfe_ric (rq_dfe), rq_dfe->_.table.ot->ot_table->tb_primary_key, csetp->csetp_iri, p_stat);
      col = cset_new_col (tb, csetp->csetp_col->col_name, DV_ANY, 0, rq_dfe->_.table.ot->ot_is_outer);
      col->col_cset_iri = csetp->csetp_iri;
      if (found)
	col->col_count = p_stat[0];
      key->key_over_csets = dk_set_union (key->key_over_csets, ot->ot_csets);
      prev_col_ref = t_listst (3, COL_DOTTED, ot->ot_new_prefix, t_box_string ("S"));
      new_col_ref = t_listst (3, COL_DOTTED, key->key_cset_prefix, t_box_string ("S"));
      t_id_hash_set (sc->sc_cset_col_subst, (caddr_t) & prev_col_ref, (caddr_t) & new_col_ref);
      prev_col_ref = t_listst (3, COL_DOTTED, ot->ot_new_prefix, t_box_string ("O"));
      new_col_ref = t_listst (3, COL_DOTTED, key->key_cset_prefix, csetp->csetp_col->col_name);
      t_id_hash_set (sc->sc_cset_col_subst, (caddr_t) & prev_col_ref, (caddr_t) & new_col_ref);
      prev_col_ref = t_listst (3, COL_DOTTED, ot->ot_new_prefix, t_box_string ("P"));
      new_col_ref = NULL;
      t_id_hash_set (sc->sc_cset_col_subst, (caddr_t) & prev_col_ref, (caddr_t) & new_col_ref);
      prev_col_ref = t_listst (3, COL_DOTTED, ot->ot_new_prefix, t_box_string ("G"));
      new_col_ref = t_listst (3, COL_DOTTED, key->key_cset_prefix, t_box_string ("G"));
      t_id_hash_set (sc->sc_cset_col_subst, (caddr_t) & prev_col_ref, (caddr_t) & new_col_ref);
    }
    END_DO_SET ();
    key->key_options = (caddr_t *) list (1, box_dv_short_string ("column"));
    dbe_key_layout_1 (key);
  }
  END_DO_SET ();
  return res;
}


int
dfe_in_csets (dk_set_t all_csets, df_elt_t * from_dfe)
{
  DO_SET (dk_set_t, cset_dfes, &all_csets)
  {
    if (dk_set_member (cset_dfes, (void *) from_dfe))
      return 1;
  }
  END_DO_SET ();
  return 0;
}


int
sqlo_oj_deps_in_from (dk_set_t placed_ojs, op_table_t * oj_ot)
{
  /* if oj_ot depends on a oj, see the oj is in the placed_ojs */
  DO_SET (df_elt_t *, join_pred, &oj_ot->ot_join_preds)
  {
    DO_SET (op_table_t *, dep_ot, &join_pred->dfe_tables)
    {
      if (dep_ot->ot_is_outer && dep_ot != oj_ot && !dk_set_member (placed_ojs, (void *) dep_ot))
	return 0;
    }
    END_DO_SET ();
  }
  END_DO_SET ();
  return 1;
}

int
sqlo_pref_in_dfes (caddr_t pref, dk_set_t dfes)
{
  DO_SET (df_elt_t *, dfe, &dfes)
  {
    if (!strcmp (dfe->_.table.ot->ot_new_prefix, pref))
      return 1;
  }
  END_DO_SET ();
  return 0;
}


int
sqlo_prefix_same_cset (sql_comp_t * sc, caddr_t p1, caddr_t p2)
{
  DO_SET (dk_set_t, cset, &sc->sc_all_csets)
  {
    if (sqlo_pref_in_dfes (p1, cset->next) && sqlo_pref_in_dfes (p2, cset->next))
      return 1;
  }
  END_DO_SET ();
  return 0;
}


int
sqlo_is_remd_cmp (sqlo_t * so, ST * tree)
{
  sql_comp_t *sc = so->so_sc;
  if (ST_P (tree, BOP_EQ))
    {
      ST **place = (ST **) id_hash_get (so->so_sc->sc_cset_col_subst, (caddr_t) & tree->_.bin_exp.left);
      if (place && !*place)
	return 1;
      if (st_is_ss_eq (tree)
	  && sqlo_prefix_same_cset (sc, tree->_.bin_exp.left->_.col_ref.prefix, tree->_.bin_exp.right->_.col_ref.prefix))
	return 1;
    }
  return 0;
}


int
sqlo_cset_repl_cb (ST ** ptree, void *cd)
{
  ST **place;
  sqlo_t *so = (sqlo_t *) cd;
  ST *tree = *ptree;
  if (ST_P (tree, BOP_AND))
    {
      dk_set_t list = sqlo_st_connective_list (tree, BOP_AND);
      dk_set_t drop = NULL;
      DO_SET (ST *, term, &list)
      {
	if (sqlo_is_remd_cmp (so, term))
	  t_set_push (&drop, (void *) term);
      }
      END_DO_SET ();
      if (drop)
	{
	  ST *new_and = NULL;
	  DO_SET (ST *, tree, &list)
	  {
	    if (!dk_set_member (drop, (void *) tree))
	      {
		sqlo_map_st_ref (&tree, sqlo_cset_repl_cb, (void *) so);
		t_st_and (&new_and, tree);
	      }
	  }
	  END_DO_SET ();
	  *ptree = new_and;
	  return 1;
	}
      return 0;
    }
  if (ST_P (tree, COL_DOTTED))
    {
      place = (ST **) id_hash_get (so->so_sc->sc_cset_col_subst, (caddr_t) & tree);
      if (place && *place)
	{
	  *ptree = (ST *) t_box_copy_tree ((caddr_t) * place);
	  return 1;
	}
    }
  return 0;
}


ST *
sqlo_cset_ref_replace (sqlo_t * so, ST ** ptree)
{
  sqlo_map_st_ref (ptree, sqlo_cset_repl_cb, (void *) so);
  return *ptree;
}

void
sqlo_dt_replace_csets (sqlo_t * so, df_elt_t * dt_dfe, dk_set_t all_csets, dk_set_t cset_tables)
{
  ST *from_st = NULL;
  ST *tree, *texp;
  sql_comp_t *sc = so->so_sc;
  dk_set_t placed_ojs = NULL, prev_ojs;
  dk_set_t from = NULL, ojs = NULL;
  tree = dt_dfe->_.sub.ot->ot_dt;
  texp = tree->_.select_stmt.table_exp;
  DO_SET (df_elt_t *, from_dfe, &dt_dfe->_.sub.ot->ot_from_dfes)
  {
    if (dfe_in_csets (all_csets, from_dfe))
      {
	if (from_dfe->_.table.ot->ot_join_cond)
	  t_st_and (&texp->_.table_exp.where, from_dfe->_.table.ot->ot_join_cond);
	continue;
      }
    if (from_dfe->_.table.ot->ot_is_outer)
      {
	t_set_push (&ojs, (void *) from_dfe->_.table.ot);
	continue;
      }
    t_set_push (&from, (void *) from_dfe->_.sub.ot);
    if (from_dfe->_.table.ot->ot_join_cond)
      t_st_and (&texp->_.table_exp.where, from_dfe->_.table.ot->ot_join_cond);
  }
  END_DO_SET ();
  DO_SET (dbe_table_t *, cset_tb, &cset_tables)
  {
    ST *ref = cset_tb;
    if (!from_st)
      from_st = ref;
    else
      from_st = t_listst (6, JOINED_TABLE, 0, J_CROSS, from_st, ref, NULL);
  }
  END_DO_SET ();
  DO_SET (op_table_t *, from_ot, &from)
  {
    ST *ref =
	from_ot->ot_table ? t_listst (3, TABLE_REF, t_listst (6, TABLE_DOTTED, from_ot->ot_table->tb_name, from_ot->ot_new_prefix,
	    NULL, NULL, from_ot->ot_opts), from_ot->ot_new_prefix) : t_listst (4, DERIVED_TABLE, from_ot->ot_dt,
	from_ot->ot_new_prefix, from_ot->ot_opts);
    if (!from_st)
      from_st = ref;
    else
      from_st = t_list (6, JOINED_TABLE, 0, J_CROSS, from_st, ref, NULL);
  }
  END_DO_SET ();
  do
    {
      prev_ojs = placed_ojs;
      DO_SET (op_table_t *, oj_ot, &ojs)
      {
	if (dk_set_member (placed_ojs, oj_ot))
	  continue;
	if (sqlo_oj_deps_in_from (placed_ojs, oj_ot))
	  {
	    ST *jc;
	    ST *ref =
		oj_ot->ot_table ? t_listst (3, TABLE_REF, t_listst (6, TABLE_DOTTED, oj_ot->ot_table->tb_name, oj_ot->ot_new_prefix,
		    NULL, NULL, oj_ot->ot_opts), oj_ot->ot_new_prefix) : t_listst (4, DERIVED_TABLE, oj_ot->ot_dt,
		oj_ot->ot_new_prefix, oj_ot->ot_opts);
	    t_set_push (&placed_ojs, (void *) oj_ot);
	    jc = oj_ot->ot_join_cond;
	    from_st = t_listst (6, JOINED_TABLE, 0, OJ_LEFT, from_st, ref, sqlo_cset_ref_replace (so, &jc));
	  }
      }
      END_DO_SET ();
    }
  while (placed_ojs != prev_ojs);
  if (!ST_P (tree, SELECT_STMT))
    SQL_GPF_T1 (sc->sc_cc, "should be a select in tree of dt dfe");
  sqlo_cset_ref_replace (so, (ST **) & tree->_.select_stmt.selection);
  texp->_.table_exp.from = t_listst (1, from_st);
  sqlo_cset_ref_replace (so, &texp->_.table_exp.where);
  sqlo_cset_ref_replace (so, (ST **) & texp->_.table_exp.group_by);
  sqlo_cset_ref_replace (so, (ST **) & texp->_.table_exp.group_by_full);
  sqlo_cset_ref_replace (so, (ST **) & texp->_.table_exp.order_by);
  sqlo_cset_ref_replace (so, &texp->_.table_exp.having);
  sc->sc_cset_col_subst = NULL;
}


void
sqlo_eq_sets (sqlo_t * so, df_elt_t * dt_dfe)
{
  /* flag quads with possible csets.  Then put them in eq sets */
  sql_comp_t *sc = so->so_sc;
  dk_set_t cset_tables;
  int changed;
  dk_set_t eqs;
  dk_set_t from_dfes = dt_dfe->_.sub.ot->ot_from_dfes;
  dk_set_t all_csets = NULL;
  dk_hash_t *done = hash_table_allocate (11);
  ot_set_s_eqs (dt_dfe->_.sub.ot);
  sqlo_dfe_csets (so, dt_dfe);
  DO_SET (df_elt_t *, tb_dfe, &from_dfes)
  {
    dk_set_t cset_tbs = NULL;
    eqs = NULL;
    if (DFE_TABLE != tb_dfe->dfe_type || gethash ((void *) tb_dfe, done) || !tb_dfe->_.sub.ot->ot_csets)
      continue;
    if (tb_dfe->_.table.ot->ot_is_outer)
      continue;
    eqs = sqlo_s_eqs (dt_dfe, tb_dfe, NULL);
    do
      {
	changed = 0;
	DO_SET (df_elt_t *, eq_tb, &eqs)
	{
	  dk_set_t more_eqs = sqlo_s_eqs (dt_dfe, eq_tb, eqs);
	  if (more_eqs != eqs)
	    {
	      changed = 1;
	      eqs = more_eqs;
	    }
	}
	END_DO_SET ();
      }
    while (changed);
    DO_SET (df_elt_t *, eqt, &eqs)
    {
      sethash ((void *) eqt, done, (void *) 1);
      if (eqt->_.table.ot->ot_csets)
	t_set_push (&cset_tbs, (void *) eqt);
    }
    END_DO_SET ();
    cset_tbs_cset (so, dt_dfe, &cset_tbs);
    if (cset_tbs->data)
      t_set_push (&all_csets, (void *) cset_tbs);
  }
  END_DO_SET ();
  dt_dfe->_.sub.ot->ot_csets = all_csets;
  sc->sc_cset_col_subst = t_id_hash_allocate (61, sizeof (caddr_t), sizeof (caddr_t), treehash, treehashcmp);
  cset_tables = cset_temp_tables (so, all_csets);
  sc->sc_all_csets = all_csets;
  sqlo_dt_replace_csets (so, dt_dfe, all_csets, cset_tables);
}


int
sqlo_cset_pre_cb (ST * tree, void *cd)
{
  sqlo_t *so = (sqlo_t *) cd;
  df_elt_t *dfe;
  sql_comp_t *sc = so->so_sc;
  if (!ST_P (tree, SELECT_STMT))
    return 0;
  dfe = sqlo_df_elt (so, tree);
  sethash ((void *) tree, sc->sc_tree_to_dfe, (void *) dfe);
  return 0;
}


int
sqlo_cset_cb (ST ** ptree, void *cd)
{
  ST *tree = *ptree;
  sqlo_t *so = (sqlo_t *) cd;
  sql_comp_t *sc = so->so_sc;
  df_elt_t *dfe;
  if (!ST_P (tree, SELECT_STMT))
    return 0;
  dfe = (df_elt_t *) gethash ((void *) tree, sc->sc_tree_to_dfe);
  if (!dfe)
    return 0;
  sqlo_eq_sets (so, dfe);
  return 0;
}


df_elt_t *
sqlo_add_csets (sqlo_t * so, ST ** ptree)
{
  sql_comp_t *sc = so->so_sc;
  ST *tree = *ptree;
  sc->sc_tree_to_dfe = hash_table_allocate (11);
  sqlo_map_st (*ptree, sqlo_cset_pre_cb, (void *) so);
  sqlo_map_st_ref (ptree, sqlo_cset_cb, (void *) so);
  hash_table_free (sc->sc_tree_to_dfe);
  so->so_const_subqs = NULL;
  *ptree = tree = (ST *) t_box_copy_tree ((caddr_t) tree);
  sqlo_scope (so, &tree);
  return sqlo_df (so, *ptree);
}


void
sqlo_cset_s_cost (df_elt_t * tb_dfe, df_elt_t * s_pred, df_elt_t * g_pred, index_choice_t * ic)
{
  memzero (ic, sizeof (index_choice_t));
  dfe_table_cost_ic (tb_dfe, ic, 0);
}


df_elt_t *
sqlo_cset_inx_dfe (sqlo_t * so, df_elt_t * cset_dfe)
{
  ST *s_eq, *p_eq;
  op_table_t *cset_ot = cset_dfe->_.table.ot;
  cset_t *cset = cset_dfe->_.table.ot->ot_table->tb_closest_cset;
  if (!cset_ot->ot_cset_inx_dfe)
    {
      char tmp[20];
      df_elt_t *inx_dfe = sqlo_new_dfe (so, DFE_TABLE, NULL);
      t_NEW_VARZ (op_table_t, inx_ot);
      snprintf (tmp, sizeof (tmp), "ci%d", so->so_name_ctr++);
      inx_ot->ot_new_prefix = t_box_string (tmp);
      inx_ot->ot_table = cset->cset_rq_table;
      inx_ot->ot_dfe = inx_dfe;
      inx_dfe->_.table.ot = inx_ot;
      inx_dfe->_.table.key = tb_px_key (inx_ot->ot_table, tb_name_to_column (inx_ot->ot_table, "O"));
      cset_ot->ot_cset_inx_dfe = inx_dfe;
      t_set_push (&so->so_tables, (void *) inx_ot);
      inx_dfe->_.table.cset_role = TS_CSET_POSG;
      BIN_OP (s_eq, BOP_EQ, t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix, t_box_string ("S")), t_listst (3,
	      COL_DOTTED, inx_ot->ot_new_prefix, t_box_string ("S")));
      inx_ot->ot_cset_inx_s_eq = sqlo_df (so, s_eq);
      t_set_push (&inx_dfe->_.table.out_cols, inx_ot->ot_cset_inx_s_eq->_.bin.right);
      BIN_OP (p_eq, BOP_EQ, t_listst (3, COL_DOTTED, inx_dfe->_.table.ot->ot_new_prefix, t_box_string ("P")),
	  t_box_iri_id (so->so_name_ctr++));
      inx_ot->ot_cset_inx_p_eq = sqlo_df (so, p_eq);
    }
  return cset_ot->ot_cset_inx_dfe;
}


ST *
sqlo_copy_replace (ST * tree, ST * old_tree, ST * new_tree)
{
  tree = (ST *) t_box_copy_tree ((caddr_t) tree);
  sqlo_replace_tree (&tree, old_tree, new_tree);
  return tree;
}


void
sqlo_cset_inx_g_eq (sqlo_t * so, df_elt_t * inx_dfe, df_elt_t * cset_dfe, df_elt_t * g_pred)
{
  /* if a set of triple paterns has a g in common and this ais a cset and is done by index on o then the o pattern needs to join with the g. Otherwise o index does not imply g */
  return;
  if (g_pred && PRED_IS_EQ (g_pred))
    return;
  {
    caddr_t g = t_box_string ("G");
    ST *tree;
    df_elt_t *g_eq;
    ST *r = t_listst (3, COL_DOTTED, inx_dfe->_.table.ot->ot_new_prefix, g);
    ST *l = t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix, g);
    BIN_OP (tree, BOP_EQ, l, r);
    g_eq = sqlo_df (so, tree);
    t_set_pushnew (&cset_dfe->_.table.col_preds, (void *) g_eq);
    t_set_pushnew (&inx_dfe->_.table.out_cols, (void *) g_eq->_.bin.right);
  }
}


void
sqlo_place_cset_inx (sqlo_t * so, df_elt_t * cset_dfe, df_elt_t * o_col, df_elt_t * s_pred, df_elt_t * g_pred)
{
  /* set the plan to have the cset by the o_cond or by scan is o_cond is null */
  ST *inx_s, *cset_s, *inx_eq;
  df_elt_t *inx_dfe, *inx_to_cset_pred;
  dk_set_t inx_preds = NULL, remd_preds = NULL;
  df_elt_t *p_const;
  op_table_t *inx_ot;
  caddr_t col_p;
  if (!o_col)
    {
      inx_dfe = cset_dfe->_.table.ot->ot_cset_inx_dfe;
      if (!inx_dfe)
	return;
      if (inx_dfe == cset_dfe->dfe_prev)
	{
	  dfe_unplace_in_middle (inx_dfe);
	  t_set_delete (&cset_dfe->_.table.col_preds, inx_dfe->_.table.ot->ot_cset_inx_s_eq);
	}
      return;
    }
  col_p = t_box_iri_id (o_col->_.col.col->col_cset_iri);
  inx_dfe = sqlo_cset_inx_dfe (so, cset_dfe);
  if (inx_dfe != cset_dfe->dfe_prev)
    {
      if (inx_dfe->dfe_prev)
	dfe_unplace_in_middle (inx_dfe);
      sqlo_place_dfe_after (so, LOC_LOCAL, cset_dfe->dfe_prev, inx_dfe);
      t_set_pushnew (&cset_dfe->_.table.col_preds, inx_dfe->_.table.ot->ot_cset_inx_s_eq);
    }
  inx_ot = inx_dfe->_.table.ot;
  p_const = inx_ot->ot_cset_inx_p_eq->_.bin.right;
  p_const->dfe_tree = col_p;
  DO_SET (df_elt_t *, cp, &cset_dfe->_.table.col_preds)
  {
    df_elt_t *left_col = dfe_left_col (cset_dfe, cp);
    if (o_col->_.col.col == left_col->_.col.col)
      {
	df_elt_t *inx_pred =
	    sqlo_df (so, sqlo_copy_replace (cp->dfe_tree, t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix,
		    o_col->_.col.col->col_name), t_listst (3, COL_DOTTED, inx_dfe->_.table.ot->ot_new_prefix, t_box_string ("O"))));
	t_set_push (&inx_preds, (void *) inx_pred);
	t_set_push (&remd_preds, cp);
      }
  }
  END_DO_SET ();
  cset_dfe->_.table.col_preds = t_set_diff (cset_dfe->_.table.col_preds, remd_preds);
  inx_s = t_listst (3, COL_DOTTED, inx_dfe->_.table.ot->ot_new_prefix, t_box_string ("S"));
  cset_s = t_listst (3, COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix, t_box_string ("S"));
  BIN_OP (inx_eq, BOP_EQ, cset_s, inx_s);
  inx_to_cset_pred = sqlo_df (so, inx_eq);
  t_set_push (&cset_dfe->_.table.col_preds, (void *) inx_to_cset_pred);
  inx_dfe->_.table.col_preds = t_CONS (inx_ot->ot_cset_inx_p_eq, inx_preds);
  inx_dfe->dfe_arity = inx_dfe->dfe_unit = 0;
  if (g_pred)
    {
      df_elt_t *inx_g =
	  sqlo_df (so, sqlo_copy_replace (g_pred->dfe_tree, t_listst (COL_DOTTED, cset_dfe->_.table.ot->ot_new_prefix,
		  t_box_string ("G")), t_listst (3, COL_DOTTED, inx_ot->ot_new_prefix, t_box_string ("G"))));
      t_set_push (&inx_dfe->_.table.col_preds, inx_g);
    }
  /* the inx dfe must return the s.  I */
  t_set_pushnew (&inx_dfe->_.table.out_cols, (void *) inx_to_cset_pred->_.bin.right);
  sqlo_cset_inx_g_eq (so, inx_dfe, cset_dfe, g_pred);
  inx_dfe->_.table.key = tb_px_key (inx_ot->ot_table, tb_name_to_column (inx_ot->ot_table, "O"));
}


void
sqlo_cset_inx_cost (sqlo_t * so, df_elt_t * tb_dfe, df_elt_t * o_col, df_elt_t * s_pred, df_elt_t * g_pred, index_choice_t * ic)
{
  df_elt_t *from_dfe;
  float u1 = 0, a1 = 0, ov = 0;
  sqlo_place_cset_inx (so, tb_dfe, o_col, s_pred, g_pred);
  tb_dfe->_.table.key = tb_dfe->_.table.ot->ot_table->tb_primary_key;
  if (o_col)
    from_dfe = tb_dfe->dfe_prev;
  else
    from_dfe = tb_dfe;
  memzero (ic, sizeof (index_choice_t));
  tb_dfe->dfe_unit = 0;
  tb_dfe->dfe_arity = 0;
  dfe_list_cost (from_dfe, &ic->ic_unit, &ic->ic_arity, &ic->ic_overhead, LOC_LOCAL);
}


void
sqlo_cset_choose_index (sqlo_t * so, df_elt_t * tb_dfe, dk_set_t * col_preds, dk_set_t * after_preds)
{
  float ov = 0;
  dk_set_t done_cols = NULL, save_cp;
  index_choice_t s_ic;
  df_elt_t o_dfe;
  index_choice_t best_ic;
  df_elt_t *best_col = NULL;
  index_choice_t ic;
  int best_unq = 0;
  caddr_t opt_inx_name;
  op_table_t *ot = dfe_ot (tb_dfe);
  int is_pk_inx = 0, is_txt_inx = 0;
  dbe_table_t *cset_tb = tb_dfe->_.table.ot->ot_table->tb_closest_cset;
  dbe_key_t *pk = tb_dfe->_.table.ot->ot_table->tb_primary_key;
  dbe_column_t *s_col = (dbe_column_t *) pk->key_parts->data;
  dbe_column_t *g_col = (dbe_column_t *) pk->key_parts->next->data;
  df_elt_t *s_pred = sqlo_key_part_best (s_col, tb_dfe->_.table.col_preds, 0);
  df_elt_t *g_pred = sqlo_key_part_best (s_col, tb_dfe->_.table.col_preds, 0);
  if (tb_dfe->_.table.key)
    return;
  opt_inx_name = sqlo_opt_value (ot->ot_opts, OPT_INDEX);
  memset (&best_ic, 0, sizeof (best_ic));
  if (opt_inx_name && !strcmp (opt_inx_name, "PRIMARY KEY"))
    is_pk_inx = 1;
  else if (opt_inx_name && !strcmp (opt_inx_name, "TEXT KEY"))
    is_txt_inx = 1;
  if (is_pk_inx)
    {
      tb_dfe->_.table.key = ot->ot_table->tb_primary_key;
      tb_dfe->dfe_unit = 0;
      dfe_table_cost_ic (tb_dfe, &best_ic, 0);
      best_unq = tb_dfe->_.table.is_unique;
    }
  else if (is_txt_inx)
    {
      if (!tb_dfe->_.table.text_pred)
	sqlc_new_error (so->so_sc->sc_cc, "22022", "SQ190",
	    "TABLE OPTION INDEX requires the usage of free-text index for table %s, but there's no free-text search condition.",
	    tb_dfe->_.table.ot->ot_table->tb_name);

      tb_dfe->_.table.is_text_order = 1;
      tb_dfe->_.table.key = tb_text_key (tb_dfe->_.table.ot->ot_table);
      tb_dfe->dfe_unit = 0;
      dfe_table_cost_ic (tb_dfe, &best_ic, 0);
      best_unq = 0;
    }
  else
    {
      sqlo_cset_s_cost (tb_dfe, s_pred, g_pred, &s_ic);
      DO_SET (df_elt_t *, cp, &tb_dfe->_.table.col_preds)
      {
	if (cp == s_pred || cp == g_pred)
	  continue;
	df_elt_t *left_col = dfe_left_col (tb_dfe, cp);
	dbe_column_t *col = left_col->_.col.col;
	dbe_column_t *equiv_col = dfe_cset_equiv_col (tb_dfe, left_col);
	cset_p_t *csp = equiv_col ? equiv_col->col_csetp : NULL;
	if (col == s_col || col == g_col)
	  continue;
	if (dk_set_member (done_cols, left_col))
	  continue;
	if (csp && !csp->csetp_index_o)
	  continue;
	memzero (&ic, sizeof (ic));
	save_cp = tb_dfe->_.table.col_preds;
	sqlo_cset_inx_cost (so, tb_dfe, left_col, s_pred, g_pred, &ic);
	tb_dfe->_.table.col_preds = save_cp;
	t_set_push (&done_cols, left_col);
	if (ic.ic_unit < best_ic.ic_unit || !best_col)
	  {
	    best_col = left_col;
	    best_ic = ic;
	  }
      }
      END_DO_SET ();
    }
  if (opt_inx_name && !best_col)
    sqlc_new_error (so->so_sc->sc_cc, "22023", "SQ188", "TABLE OPTION index %s not defined for table %s",
	opt_inx_name, ot->ot_table->tb_name);

  if (tb_dfe->_.table.text_pred)
    {
      if (!opt_inx_name && !best_ic.ic_is_unique && sqlo_is_text_order (so, tb_dfe))
	{
	  tb_dfe->_.table.is_text_order = 1;
	  tb_dfe->_.table.key = tb_dfe->_.table.ot->ot_table->tb_primary_key;
	  tb_dfe->dfe_unit = 0;
	  dfe_table_cost (tb_dfe, &tb_dfe->dfe_unit, &tb_dfe->dfe_arity, &ov, 0);
	  return;
	}
      else
	{
	  dbe_column_t *col = (dbe_column_t *) tb_text_key (dfe_ot (tb_dfe)->ot_table)->key_parts->data;
	  df_elt_t *col_dfe = sqlo_df (so, t_listst (3, COL_DOTTED, dfe_ot (tb_dfe)->ot_new_prefix, col->col_name));
	  sqlo_place_exp (so, tb_dfe->dfe_super, col_dfe);
	}

    }

  if (s_ic.ic_unit < best_ic.ic_unit || !best_col)
    {
      best_col = NULL;
      tb_dfe->_.table.inx_card = s_ic.ic_inx_card;
    }
  sqlo_place_cset_inx (so, tb_dfe, best_col, s_pred, g_pred);
  tb_dfe->_.table.key = tb_dfe->_.table.ot->ot_table->tb_primary_key;
}

dbe_column_t *
dfe_rq_cset_equiv_col (df_elt_t * dfe, cset_p_t * csetp, df_elt_t * left_col)
{
  dbe_table_t *cset_tb = dfe->_.table.key->key_table;
  dbe_column_t *same_col;
  if (0 == strcmp (left_col->_.col.col->col_name, "O"))
    return csetp->csetp_col;
  if (0 == strcmp (left_col->_.col.col->col_name, "P"))
    return NULL;
  same_col = tb_name_to_column (cset_tb, left_col->_.col.col->col_name);
  if (!same_col)
    SQL_GPF_T1 (top_sc->sc_cc, "rdf quad going to cset has non-equality P condition or other column not in PSOG");
  return same_col;
}



void
dfe_rq_cset_cost (df_elt_t * dfe, cset_p_t * csetp, index_choice_t * ic)
{
  dbe_column_t *equiv;
  dk_set_t na_preds = NULL;
  dk_set_t org_cp = dfe->_.table.col_preds;
  cset_t *cset = csetp->csetp_cset;
  dk_hash_t col_bk[100];
  dbe_key_t *save_key = dfe->_.table.key;
  hash_table_init (col_bk, 11);
  dfe->_.table.key = cset->cset_table->tb_primary_key;
  DO_SET (df_elt_t *, cp, &dfe->_.table.col_preds)
  {
    df_elt_t *left_col = dfe_left_col (dfe, cp);
    if (!left_col)
      continue;
    equiv = dfe_rq_cset_equiv_col (dfe, csetp, left_col);
    if (!equiv)
      t_set_push (&na_preds, (void *) cp);
    if (!gethash ((void *) left_col, &col_bk))
      {
	sethash ((void *) left_col, &col_bk, left_col->_.col.col);
	left_col->_.col.col = equiv;
      }
  }
  END_DO_SET ();
  if (na_preds)
    dfe->_.table.col_preds = t_set_diff (dfe->_.table.col_preds, na_preds);
  dfe_table_cost_ic_2 (dfe, ic, 0);
  dfe->_.table.col_preds = org_cp;
  DO_HT (df_elt_t *, left_col, dbe_column_t *, cset_col, &col_bk) left_col->_.col.col = cset_col;
  END_DO_HT;
  hash_table_destroy (&col_bk);
  dfe->_.table.key = save_key;
}


int
sqlo_cset_quad_choose_index (sqlo_t * so, df_elt_t * tb_dfe, dk_set_t * col_preds, dk_set_t * after_preds)
{
  /* if this is a quad that can fall on a cset then ps forces pk and po forces posg followed by psog for cases of  o needing scan.  If this is a quad that cannot fall on a cset then return 0 */
  int extra_psog = 0, n_csets;
  index_choice_t ic;
  dk_set_t csp = NULL;
  dbe_table_t *tb = tb_dfe->_.table.ot->ot_table;
  rq_cols_t rq;
  memzero (&ic, sizeof (ic));
  memzero (&rq, sizeof (rq));
  tb_dfe->_.table.key = tb->tb_primary_key;
  rq_cols_init (tb_dfe, &rq);
  tb_dfe->_.table.key = NULL;
  if (RQ_CONST_EQ == rq.rq_s.rqp_op)
    {
      iri_id_t s = unbox_iri_id (dfe_right (tb_dfe, rq.rq_s.rqp_lower)->dfe_tree);
      if (IRI_RANGE (s))
	goto psog;
      return 0;
    }
  if (RQ_CONST_EQ == rq.rq_p.rqp_op)
    {
      iri_id_t p = unbox_iri_id (dfe_right (tb_dfe, rq.rq_p.rqp_lower)->dfe_tree);
      csp = (dk_set_t) gethash ((void *) p, &p_to_csetp_list);
      if (!csp)
	return 0;
    }
  if (RQ_BOUND_EQ == rq.rq_s.rqp_op)
    goto psog;
  if (RQ_UNBOUND == rq.rq_o.rqp_op)
    goto psog;
  if (csp)
    {
      if (RQ_BOUND_EQ == rq.rq_s.rqp_op)
	goto psog;
      DO_SET (cset_p_t *, csetp, &csp)
      {
	if (!csetp->csetp_index_o)
	  goto posg_cset;
	if (RQ_CONST_EQ == rq.rq_o.rqp_op)
	  {
	    caddr_t o = rq.rq_o.rqp_lower->dfe_tree;
	    if (csetp->csetp_non_index_o && id_hash_get (csetp->csetp_non_index_o, (caddr_t) & o))
	      goto posg_cset;
	  }
      }
      END_DO_SET ();
      goto posg;
    }
posg_cset:
  extra_psog = 1;
posg:
  tb_dfe->_.table.key = tb_px_key (tb, rq.rq_o_col);
  if (extra_psog)
    {
      tb_dfe->_.table.cset_role = TS_CSET_POSG;
      tb_dfe->_.table.add_cset_psog = 1;
    }
  return 1;
psog:
  tb_dfe->_.table.key = tb_px_key (tb, rq.rq_s_col);
  dfe_table_cost_ic (tb_dfe, &ic, 0);
  n_csets = 1 + dk_set_length (csp);
  DO_SET (cset_p_t *, csetp, &csp)
  {
    index_choice_t ic2;
    memzero (&ic2, sizeof (index_choice_t));
    dfe_rq_cset_cost (tb_dfe, csetp, &ic2);
    ic.ic_unit += ic2.ic_unit;
    ic.ic_arity += ic2.ic_arity;
    ic.ic_inx_card += ic2.ic_inx_card;
    if (ic2.ic_altered_col_pred)
      {
	ic.ic_altered_col_pred = ic2.ic_altered_col_pred;
	if (ic2.ic_after_test)
	  ic.ic_after_test = ic2.ic_after_test;
      }
  }
  END_DO_SET ();
  ic.ic_unit /= n_csets;
  ic.ic_arity /= n_csets;
  ic.ic_inx_card /= n_csets;
  dfe_table_set_by_best (tb_dfe, &ic, -1, col_preds, after_preds);
  return 1;
}
