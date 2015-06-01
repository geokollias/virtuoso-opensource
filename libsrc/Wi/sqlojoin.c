/*
 *  sqlojoin.c
 *
 *  $Id: sqlcost.c,v 1.51.2.54.2.71 2013/08/20 13:26:25 oerling Exp $
 *
 *  sql cost functions
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
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlbif.h"
#include "arith.h"
#include "security.h"
#include "remote.h"
#include "sqlo.h"
#include "list2.h"
#include "rdfinf.h"


int32 enable_join_sample = 0;
int32 sqlo_sample_batch_sz = 10;

void
sqlo_init_dep_sample (sqlo_t * so)
{
  sql_comp_t *sc = so->so_sc;
  caddr_t *inst = sc->sc_sample_inst;
  if (!inst)
    {
      int inx;
      client_connection_t *cli = sqlc_client ();
      inst = (caddr_t *) t_alloc_box (sizeof (query_instance_t) + sizeof (caddr_t) * SQLO_MAX_SAMPLE_COLS * 54, DV_QI);
      QNCAST (QI, qi, inst);
      memzero (inst, box_length (inst));
      qi->qi_trx = cli->cli_trx;
      qi->qi_client = cli;
      sc->sc_sample_inst = inst;
      sc->sc_sample_oms = (v_out_map_t **) t_alloc_box (SQLO_MAX_SAMPLE_COLS * sizeof (caddr_t), DV_BIN);
      for (inx = 1; inx <= SQLO_MAX_SAMPLE_COLS; inx++)
	{
	  sc->sc_sample_oms[inx - 1] = (v_out_map_t *) t_alloc_box (sizeof (v_out_map_t) * inx, DV_BIN);
	  memzero (sc->sc_sample_oms[inx - 1], box_length (sc->sc_sample_oms[inx - 1]));
	}
      {
	key_source_t *ks;
	t_NEW_VARZ (table_source_t, ts);
	sc->sc_sample_ts = ts;
	ks = ts->ts_order_ks = (key_source_t *) t_alloc (sizeof (key_source_t));
	memzero (ts->ts_order_ks, sizeof (key_source_t));
	ks->ks_ts = ts;
      }
      sc->sc_sample_ssls = (state_slot_t *) t_alloc_box (sizeof (state_slot_t) * SQLO_MAX_SAMPLE_COLS, DV_BIN);
    }
}


dbe_col_loc_t *
key_dep_cl (dbe_key_t * key, dbe_column_t * col)
{
  if (key->key_is_col)
    return cl_list_find (key->key_row_var, col->col_id);
  else
    return key_find_cl (key, col->col_id);
}

void
sqlo_sample_out_cols (sqlo_t * so, df_elt_t * tb_dfe, it_cursor_t * itc)
{
  table_source_t *ts;
  key_source_t *ks;
  v_out_map_t *om;
  sql_comp_t *sc = so->so_sc;
  dbe_key_t *key = itc->itc_insert_key;
  int n = 0, fill, n_cols, fetch_any = 0;
  op_table_t *ot = so->so_this_dt;
  dk_set_t cols = NULL;
  caddr_t *inst;
  query_instance_t *qi;
  if (!enable_join_sample || !key->key_is_col)
    return;
  if (key->key_partition && clm_replicated != key->key_partition->kpd_map)
    return;			/* not for non-local */
  DO_SET (df_elt_t *, pred, &ot->ot_preds)
  {
    if (!pred->dfe_is_placed && PRED_IS_EQ (pred) && pred->dfe_tables && pred->dfe_tables->next
	&& dk_set_member (pred->dfe_tables, tb_dfe->_.table.ot))
      {
	df_elt_t *col = dfe_left_col (tb_dfe, pred);
	if (!col || !col->_.col.col || !key_dep_cl (key, col->_.col.col))
	  continue;
	t_set_pushnew (&cols, (void *) col);
	if (!col->dfe_dc)
	  fetch_any = 1;
	if (++n == SQLO_MAX_SAMPLE_COLS)
	  break;
      }
  }
  END_DO_SET ();
  DO_SET (op_table_t *, from_ot, &ot->ot_from_ots)
  {
    DO_SET (df_elt_t *, pred, &from_ot->ot_join_preds)
    {
      if (!pred->dfe_is_placed && PRED_IS_EQ (pred) && pred->dfe_tables && pred->dfe_tables->next
	  && dk_set_member (pred->dfe_tables, tb_dfe->_.table.ot))
	{
	  df_elt_t *col = dfe_left_col (tb_dfe, pred);
	  if (!col || !col->_.col.col || !key_dep_cl (key, col->_.col.col))
	    continue;
	  t_set_pushnew (&cols, (void *) col);
	  if (!col->dfe_dc)
	    fetch_any = 1;
	  if (++n == SQLO_MAX_SAMPLE_COLS)
	    break;
	}
    }
    END_DO_SET ();
  }
  END_DO_SET ();
  if (!fetch_any || !cols)
    return;
  sqlo_init_dep_sample (so);
  qi = (QI *) (inst = sc->sc_sample_inst);
  n_cols = n;
  om = sc->sc_sample_oms[n_cols - 1];
  fill = sizeof (query_instance_t) / sizeof (caddr_t);
  n = 0;
  DO_SET (df_elt_t *, col, &cols)
  {
    state_slot_t *ssl = &sc->sc_sample_ssls[n];
    memzero (ssl, sizeof (state_slot_t));
    ssl->ssl_index = fill++;
    ssl->ssl_box_index = fill++;
    ssl->ssl_sqt = col->_.col.col->col_sqt;
    ssl->ssl_type = SSL_VEC;
    om[n].om_ssl = ssl;
    om[n].om_cl = *key_dep_cl (key, col->_.col.col);
    if (!col->dfe_dc)
      col->dfe_dc = mp_data_col (THR_TMP_POOL, ssl, sqlo_sample_batch_sz);
    dc_reset (col->dfe_dc);
    QST_BOX (data_col_t *, inst, ssl->ssl_index) = col->dfe_dc;
    n++;
  }
  END_DO_SET ();
  itc->itc_out_state = inst;
  ts = sc->sc_sample_ts;
  ks = itc->itc_ks = ts->ts_order_ks;
  ks->ks_v_out_map = om;
  ks->ks_key = itc->itc_insert_key;
  itc->itc_v_out_map = ks->ks_v_out_map;
  itc->itc_n_results = 0;
  ts->src_gen.src_sets = fill++;
  ts->src_gen.src_out_fill = fill++;
  ts->src_gen.src_batch_size = fill++;
  if (!tb_dfe->dfe_sets)
    tb_dfe->dfe_sets = (int *) t_alloc_box (sizeof (int) * sqlo_sample_batch_sz, DV_BIN);
  QST_BOX (int *, inst, ts->src_gen.src_sets) = tb_dfe->dfe_sets;
  QST_INT (inst, ts->src_gen.src_batch_size) = sqlo_sample_batch_sz;;
  QST_INT (inst, ts->src_gen.src_out_fill) = 0;
  itc->itc_ltrx = qi->qi_trx;
  itc->itc_batch_size = sqlo_sample_batch_sz;
  itc->itc_n_sets = 1;
  ks->ks_row_check = itc_col_row_check_dummy;
  itc->itc_param_order = (int *) t_alloc_box (sizeof (int), DV_BIN);
  memzero (itc->itc_param_order, box_length (itc->itc_param_order));
  itc_col_init (itc);
}

void
smp_get_cols (tb_sample_t * smp, it_cursor_t * itc)
{
  int n_rows, irow, col;
  caddr_t **rows = smp->smp_rows;
  if (!rows)
    return;
  n_rows = BOX_ELEMENTS (rows);
  for (irow = 0; irow < n_rows; irow++)
    {
      caddr_t *row = rows[irow];
      int n_cols = BOX_ELEMENTS (row);
      for (col = 0; col < n_cols; col++)
	{
	  v_out_map_t *om = &itc->itc_v_out_map[col];
	  data_col_t *dc = QST_BOX (data_col_t *, itc->itc_out_state, om->om_ssl->ssl_index);
	  dc_append_box (dc, row[col]);
	}
    }
}


caddr_t *
smp_rows (int n, int is_mp)
{
  caddr_t r =
      is_mp ? t_alloc_box (n * sizeof (caddr_t), DV_ARRAY_OF_POINTER) : dk_alloc_box (n * sizeof (caddr_t), DV_ARRAY_OF_POINTER);
  memzero (r, box_length (r));
  return (caddr_t *) r;
}

void
smp_set_cols (tb_sample_t * smp, it_cursor_t * itc, int is_mp)
{
  v_out_map_t *oms = itc->itc_v_out_map;
  v_out_map_t *om;
  data_col_t *dc;
  int n_rows, n_cols, irow, col;
  caddr_t **rows, *row;
  if (!oms)
    return;
  n_cols = box_length (oms) / sizeof (v_out_map_t);
  om = oms;
  dc = QST_BOX (data_col_t *, itc->itc_out_state, om->om_ssl->ssl_index);
  n_rows = dc->dc_n_values;
  rows = (caddr_t **) smp_rows (n_rows, is_mp);
  for (irow = 0; irow < n_rows; irow++)
    {
      row = rows[irow] = smp_rows (n_cols, is_mp);
      for (col = 0; col < n_cols; col++)
	{
	  caddr_t value = dc_box (dc, irow);
	  if (is_mp)
	    {
	      row[col] = mp_full_box_copy_tree (THR_TMP_POOL, value);
	      dk_free_tree (value);
	    }
	  else
	    row[col] = value;
	}
    }
  smp->smp_rows = rows;
}
