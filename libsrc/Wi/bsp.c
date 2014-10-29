
/*
 *  bsp.c
 *
 *  $Id$
 *
 *  Transitive Node with dfg
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
#include "eqlcomp.h"
#include "sqlfn.h"
#include "sqlpar.h"
#include "sqlpfn.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "sqlo.h"
#include "list2.h"
#include "xmlnode.h"
#include "xmltree.h"
#include "arith.h"
#include "rdfinf.h"
#include "rdf_core.h"
#include "mhash.h"




void
end_node_input_bsp (end_node_t * ins, caddr_t * inst, caddr_t * state)
{

}


void
setp_trans_agg (setp_node_t * setp, caddr_t * inst, gb_op_t * go, chash_t * cha, uint64 hash_no, int64 ** groups, int first_set,
    int last_set)
{
  /* add path where new.  If complement, check the complement */

}


void
cha_next_superstep (table_source_t * ts, caddr_t * inst)
{
  key_source_t *ks = ts->ts_order_ks;
  setp_node_t *setp = ks->ks_from_setp;
  trans_read_t *trr = ts->ts_trans_read;
  index_tree_t *tree = QST_BOX (index_tree_t *, inst, setp->setp_ha->ha_tree->ssl_index);
  chash_t *top_cha = tree->it_hi->hi_chash, *cha;
  int part;
  cha = setp_fill_cha (setp, inst, tree);
  if (!cha)
    return;
  QST_GET_V (inst, setp->setp_trans_any) = NULL;
  for (part = 0; part < cha->cha_n_partitions; part++)
    {
      chash_page_t *chp, *next;
      chash_t *cha_p = CHA_PARTITION (cha, part);
      chp = cha_p->cha_init_page;
      for (chp = chp; chp; chp = next)
	{
	  chash_page_t *next = chp->h.h.chp_next;
	  if (chp->h.h.chp_prev_fill)
	    {
	      chp->h.h.chp_prev_start = chp->h.h.chp_prev_fill;
	      chp->h.h.chp_prev_fill = 0;
	    }
	  if (!next)
	    chp->h.h.chp_prev_fill = chp->h.h.chp_fill;
	}
    }
}


void
tn_cl_start (query_frag_t * qf, caddr_t * inst)
{
  /* start dfg in slices which the qf has inited */
}


void
tn_single_start (trans_node_t * tn, caddr_t * inst)
{
  query_t *qr = tn->tn_inlined_step;
  table_source_t *rdr = (table_source_t *) qr->qr_head_node;
  setp_node_t *setp = rdr->ts_order_ks->ks_from_setp;
  trans_read_t *trr = rdr->ts_trans_read;
  int max_steps = 0, ctr = 0;
  if (tn->tn_max_depth)
    max_steps = unbox (qst_get (inst, tn->tn_max_depth));
  for (;;)
    {
      int inx;
      caddr_t **qis;
      qst_set_long (inst, trr->trr_superstep, ctr);
      ts_sliced_reader (rdr, inst, rdr->ts_order_ks->ks_from_setp->setp_ha);
      qis = QST_BOX (caddr_t **, inst, rdr->ts_aq_qis->ssl_index);
      DO_BOX (caddr_t *, slice_inst, inx, qis)
      {
	if (QST_GET_V (slice_inst, setp->setp_trans_any))
	  goto cont;
      }
      END_DO_BOX;
      return;
    cont:
      if (max_steps == ++ctr)
	break;
    }
}


void
cha_bsp_result (table_source_t * ts, caddr_t * inst, chash_t * cha, int64 * ent)
{
  /* produce a trans result as multiple rows */
}


void
tn_init (trans_node_t * tn, caddr_t * inst)
{
  if (CL_RUN_LOCAL == cl_run_local_only)
    {
      caddr_t **qis;
      stage_node_t *stn = (stage_node_t *) tn->tn_init;
      table_source_t *rdr = (table_source_t *) tn->tn_inlined_step->qr_head_node;
      cl_op_t *itcl_clo;
      cl_req_group_t *clrg;
      cll_in_box_t *rcv_clib;
      ts_sdfg_init (rdr, inst);
      itcl_clo = QST_BOX (cl_op_t *, inst, rdr->clb.clb_itcl->ssl_index);
      clrg = itcl_clo->_.itcl.itcl->itcl_clrg;
      rcv_clib = (cll_in_box_t *) clrg->clrg_clibs->data;
      qst_set_long (inst, stn->stn_coordinator_req_no, rcv_clib->clib_req_no);
      stn->stn_nth = 1;
      qn_input (tn->tn_init, inst, inst);
      qis =
	  (caddr_t **) dk_alloc_box_zero (sizeof (caddr_t) *
	  stn->stn_loc_ts->ts_order_ks->ks_key->key_partition->kpd_map->clm_distinct_slices, DV_ARRAY_OF_POINTER);
      qst_set (inst, stn->stn_slice_qis, (caddr_t) qis);
      DO_RBUF (cl_message_t *, cm, rbe, rbe_inx, &rcv_clib->clib_in)
      {
	/* partitioning step produced a message, make a slice qi for the partition */
	caddr_t *slice_inst = qf_slice_qi (stn->stn_qf, inst, 0);
	QNCAST (QI, slice_qi, slice_inst);
	qis[cm->cm_slice] = slice_inst;
	slice_qi->qi_slice = cm->cm_slice;
	QST_INT (slice_inst, stn->stn_coordinator_id) = local_cll.cll_this_host;
	qst_set_long (slice_inst, stn->stn_coordinator_req_no, rcv_clib->clib_req_no);
	SRC_IN_STATE (stn, slice_inst) = slice_inst;
      }
      END_DO_RBUF;
      ts_sdfg_run (stn->stn_loc_ts, inst);
    }
  else
    qn_input (tn->tn_init, inst, inst);
}


void
tn_bsp_run (trans_node_t * tn, caddr_t * inst)
{
  int inx;
  QNCAST (QI, qi, inst);
  static caddr_t tnull;
  int n_sets = QST_INT (inst, tn->src_gen.src_prev->src_out_fill);
  table_source_t *rdr;
  id_hash_t *sets = QST_BOX (id_hash_t *, inst, tn->tn_input_sets);
  data_col_t *ext_set_dc = QST_BOX (data_col_t *, inst, tn->tn_ext_sets->ssl_index);
  data_col_t *int_set_dc = tn->tn_int_set_no ? QST_BOX (data_col_t *, inst, tn->tn_int_set_no->ssl_index) : NULL;
  int ctr = 0;
  if (!tnull)
    tnull = dk_alloc_box (0, DV_DB_NULL);
  dc_reset_array (inst, (data_source_t *) tn, tn->tn_input, -1);
  dc_reset (ext_set_dc);
  if (int_set_dc)
    {
      dc_reset (int_set_dc);
      DC_CHECK_LEN (int_set_dc, sets->ht_count - 1);
    }
  subq_init (tn->tn_inlined_step, inst);
  DO_IDHASH (ptrlong, id, trans_set_t *, ts, sets)
  {
    int inx;
    caddr_t *ext_sets = dk_set_to_array (ts->ts_input_set_nos);
    dc_append_box (ext_set_dc, ext_sets);
    if (int_set_dc)
      dc_append_int64 (int_set_dc, ctr);
    ctr++;
    DO_BOX (caddr_t, in, inx, ts->ts_value)
    {
      dc_append_box (QST_BOX (data_col_t *, inst, tn->tn_input[inx]->ssl_index), in);
    }
    END_DO_BOX;
  }
  END_DO_IDHASH;
  tn->tn_init->src_prev = tn->src_gen.src_prev;
  qi->qi_n_sets = n_sets;
  DO_BOX (state_slot_t *, ssl, inx, tn->tn_data) qst_set_all (inst, ssl, tnull);
  END_DO_BOX;
  if (CL_RUN_LOCAL == cl_run_local_only)
    {
      table_source_t *rdr = (table_source_t *) tn->tn_inlined_step->qr_head_node;
      qst_set (inst, rdr->ts_aq_qis, NULL);
    }
  tn_init (tn, inst);
  if (CL_RUN_LOCAL == cl_run_local_only)
    tn_single_start (tn, inst);
  else
    tn_cl_start (tn, inst);
}


int
dfe_is_1_input (df_elt_t * dfe)
{
  for (dfe = dfe->dfe_prev; dfe; dfe = dfe->dfe_prev)
    {
      if (DFE_DT == dfe->dfe_type && !dfe->_.sub.hash_filler_of)
	return 0;
      if (DFE_TABLE == dfe->dfe_type && !dfe->_.table.hash_filler_of)
	return 0;
    }
  return 1;
}


void
tn_setp_loc_ts (sql_comp_t * sc, trans_node_t * tn, setp_node_t * setp)
{
  if (CL_RUN_LOCAL == cl_run_local_only)
    {
      sdfg_setp_loc_ts (sc, setp);
    }
}



setp_node_t *
setp_tn_state (sql_comp_t * sc, trans_node_t * tn, state_slot_t ** in, setp_node_t * init_setp)
{
  int inx, min_depth = 0;
  SQL_NODE_INIT (setp_node_t, setp, setp_node_input, setp_node_free);
  if (tn->tn_min_depth && SSL_CONSTANT == tn->tn_min_depth->ssl_type)
    min_depth = unbox (tn->tn_min_depth->ssl_constant);
  DO_BOX (state_slot_t *, in, inx, tn->tn_input) dk_set_push (&setp->setp_keys, (void *) in);
  END_DO_BOX;
  if (!tn->tn_is_single_state)
    {
      dk_set_push (&setp->setp_keys, tn->tn_int_set_no);
      setp->setp_set_no_in_key = 1;
    }
  setp->setp_keys = dk_set_nreverse (setp->setp_keys);
  if (tn->tn_step_no_ret)
    {
      NEW_VARZ (gb_op_t, go);
      go->go_op = AMMSC_TRANS;
      go->go_ua_arglist = box_concat ((caddr_t) tn->tn_input, (caddr_t) tn->tn_data);
      dk_set_push (&setp->setp_gb_ops, (void *) go);
      if (!tn->tn_path)
	tn->tn_path = ssl_new_variable (sc->sc_cc, "path", DV_ANY);
      dk_set_push (&setp->setp_dependent, (void *) tn->tn_path);
    }
  if (min_depth)
    {
      setp->setp_extra_bits++;
      setp->setp_tn_min_step = min_depth;
      if (!tn->tn_superstep)
	tn->tn_superstep = ssl_new_variable (sc->sc_cc, "tn_superstep", DV_LONG_INT);
      setp->setp_superstep = tn->tn_superstep;
    }
  setp_distinct_hash (sc, setp, 1000000, HA_GROUP);
  tn_setp_loc_ts (sc, tn, setp);
  if (init_setp)
    {
      setp->setp_ha->ha_tree = init_setp->setp_ha->ha_tree;
      setp->setp_ht_id = init_setp->setp_ht_id;
    }
  else if (CL_RUN_LOCAL != cl_run_local_only)
    setp->setp_ht_id = ssl_new_variable (sc->sc_cc, "tn_ht_id", DV_LONG_INT);
  setp->setp_partitioned = 1;
  setp->setp_cl_partition = HS_CL_PART;
  return setp;
}


#define KCP(m) ks->m = org_ks->m

table_source_t *
rdr_copy (sql_comp_t * sc, table_source_t * org_ts)
{
  key_source_t *ks, *org_ks = org_ts->ts_order_ks;
  SQL_NODE_INIT (table_source_t, ts, chash_read_input, ts_free);
  ks = ts->ts_order_ks = (key_source_t *) dk_alloc (sizeof (key_source_t));
  memzero (ks, sizeof (key_source_t));
  ks->ks_out_slots = dk_set_copy (org_ks->ks_out_slots);
  ks->ks_out_cols = dk_set_copy (org_ks->ks_out_cols);
  KCP (ks_key);
  KCP (ks_from_setp);
  KCP (ks_ht_id);
  KCP (ks_ht);
  KCP (ks_set_no);		/* if ks reading a gby/oby temp, this is ssl ref to the set no from the set ctr at the start of the qr */
  KCP (ks_pos_in_temp);
  KCP (ks_nth_cha_part);
  KCP (ks_cha_chp);
  ts->ts_trans_read = (trans_read_t *) dk_alloc (sizeof (trans_read_t));
  *ts->ts_trans_read = *org_ts->ts_trans_read;
  return ts;
}

#undef KCP

void
qr_add_setp_1 (sql_comp_t * sc, query_t * qr, data_source_t * qn, table_source_t * rdr, setp_node_t * setp)
{
  data_source_t *next;
  query_t *inner_qr = qn->src_query;
  inner_qr->qr_head_node = (data_source_t *) rdr;
  rdr->src_gen.src_continuations = CONS (qn, NULL);
  dk_set_delete (&qr->qr_nodes, (void *) rdr);
  dk_set_conc (qr->qr_nodes, CONS (rdr, NULL));
  for (qn = qn; qn; qn = next)
    {
      next = qn_next (qn);
      if (IS_QN (next, select_node_input) || IS_QN (next, select_node_input_subq))
	{
	  qn->src_continuations->data = (void *) setp;
	  setp->src_gen.src_pre_code = next->src_pre_code;
	  next->src_pre_code = NULL;
	  dk_set_delete (&qr->qr_nodes, (void *) setp);
	  dk_set_push (&qr->qr_nodes, (void *) setp);
	  break;
	}
    }
  if (CL_RUN_LOCAL == cl_run_local_only)
    sqlg_parallel_ts_seq (sc, sqlg_qn_dfe ((data_source_t *) inner_qr), rdr, NULL, NULL);
  else
    {
    }
}


void
qr_add_setp (sql_comp_t * sc, query_t * qr, table_source_t * rdr, setp_node_t * setp)
{
  if (IS_QN (qr->qr_head_node, subq_node_input))
    {
      query_frag_t *qf = sc->sc_qf;
      dk_set_t *terms = NULL;
      QNCAST (subq_source_t, sqs, qr->qr_head_node);
      if (sqlg_union_all_list (sqs, &terms))
	{
	  int is_first = 1;
	  DO_SET (data_source_t *, head, &terms)
	  {
	    sc->sc_qf = qf;
	    if (!is_first)
	      {
		setp = setp_copy (setp);
		rdr = rdr_copy (sc, rdr);
	      }
	    qr_add_setp_1 (sc, qr, head, rdr, setp);
	    is_first = 0;
	  }
	  END_DO_SET ();
	}
    }
  else
    qr_add_setp_1 (sc, qr, qr->qr_head_node, rdr, setp);
}


stage_node_t *
sqlg_tn_part (sql_comp_t * sc, trans_node_t * tn)
{
  query_frag_t *save = sc->sc_qf;
  sc->sc_qf = NULL;
  if (CL_RUN_LOCAL == cl_run_local_only)
    {
      sqlg_parallel_ts_seq (sc, tn->tn_init);
      tn->tn_init = sc->sc_qf_ret->qf_head_node;
      dk_set_push (&sc->sc_qf_ret->qf_nodes, (void *) tn->tn_init);
      sc->sc_qf_ret = NULL;
      return (stage_node_t *) tn->tn_init;
    }
}


void
tn_add_trans_dfg (sql_comp_t * sc, trans_node_t * tn)
{
  setp_node_t *setp, *init_setp;
  stage_node_t *stn = NULL;
  query_t *qr = tn->tn_inlined_step;
  query_t *save = sc->sc_cc->cc_query;
  key_partition_def_t *kpd = NULL;
  data_source_t *qn;
  for (qn = qr->qr_head_node; qn; qn = qn_next (qn))
    {
      if (IS_TS (qn))
	{
	  QNCAST (table_source_t, ts, qn);
	  key_source_t *ks = ts->ts_order_ks;
	  kpd = ts->ts_order_ks->ks_key->key_partition;
	  if (kpd)
	    {
	      search_spec_t *sp = ks_find_eq_sp_ssl (ks, kpd->kpd_cols[0]->cp_col_id);

	    }
	}
    }
  init_setp = setp = setp_tn_state (sc, tn, tn->tn_input, NULL);
  tn->tn_init = (data_source_t *) setp;
  tn->tn_tree = setp->setp_ha->ha_tree;
  tn->tn_ht_id = setp->setp_ht_id;
  stn = sqlg_tn_part (sc, tn);
  if (!tn->tn_is_second_in_direction3)
    {
      table_source_t *ts;
      trans_read_t *trr = dk_alloc (sizeof (trans_read_t));
      dk_set_t out_cols = NULL, out_slots = NULL;
      ptrlong inx;
      sc->sc_sort_insert_node = (setp_node_t *) qn_next (tn->tn_init);
      DO_BOX (state_slot_t *, out, inx, tn->tn_output)
      {
	NCONCF1 (out_cols, (void *) inx);
	NCONCF1 (out_slots, (void *) out);
      }
      END_DO_BOX;
      sc->sc_set_no_ssl = tn->tn_int_set_no;
      ts = sqlc_make_sort_out_node (sc, out_cols, out_slots, NULL, 1);
      dk_set_free (out_cols);
      memzero (trr, sizeof (trans_read_t));
      trr->trr_ext_sets = tn->tn_ext_sets;
      trr->trr_exclude_init = tn->tn_min_depth ? unbox (tn->tn_min_depth->ssl_constant) != 0 : 0;
      trr->trr_path_col = dk_set_length (ts->ts_order_ks->ks_out_slots);
      trr->trr_path_no_ret = tn->tn_path_no_ret;
      trr->trr_step_no_ret = tn->tn_step_no_ret;
      ts->ts_trans_read = trr;
      dk_set_delete (&sc->sc_cc->cc_query->qr_nodes, (void *) ts);
      dk_set_ins_before (&sc->sc_cc->cc_query->qr_nodes, (void *) tn, (void *) ts);
      ts->src_gen.src_continuations = tn->src_gen.src_continuations;
      tn->src_gen.src_continuations = CONS (ts, NULL);
      ts->src_gen.src_after_test = tn->tn_after_join_test;
      tn->tn_after_join_test = NULL;
    }
  sc->sc_cc->cc_query = qr;
  setp = setp_tn_state (sc, tn, tn->tn_output, init_setp);
  setp->setp_trans_any = ssl_new_variable (sc->sc_cc, "tn_any", DV_LONG_INT);
  {
    table_source_t *ts;
    trans_read_t *trr = dk_alloc (sizeof (trans_read_t));
    dk_set_t out_cols = NULL, out_slots = NULL;
    ptrlong inx;
    sc->sc_sort_insert_node = setp;
    DO_BOX (state_slot_t *, in, inx, tn->tn_input)
    {
      NCONCF1 (out_cols, (void *) inx);
      NCONCF1 (out_slots, (void *) in);
    }
    END_DO_BOX;
    if (tn->tn_path_no_ret)
      {
	NCONCF1 (out_slots, tn->tn_path);
	NCONCF1 (out_cols, (void *) inx);
      }
    sc->sc_set_no_ssl = tn->tn_int_set_no;
    ts = sqlc_make_sort_out_node (sc, out_cols, out_slots, NULL, 1);
    dk_set_free (out_cols);
    memzero (trr, sizeof (trans_read_t));
    trr->trr_superstep = tn->tn_superstep;
    trr->trr_is_step = 1;
    trr->trr_ext_sets = tn->tn_ext_sets;
    trr->trr_step_flags = TRR_PREV_STEP;
    if (CL_RUN_LOCAL == cl_run_local_only)
      {
	clb_init (sc->sc_cc, &ts->clb, 0);
	ts->clb.clb_itcl = stn->stn_qf->clb.clb_itcl;
      }
    ts->ts_trans_read = trr;
    if (CL_RUN_LOCAL == cl_run_local_only)
      sc->sc_qf = ((stage_node_t *) tn->tn_init)->stn_qf;
    qr_add_setp (sc, qr, ts, setp);
    sc->sc_qf = NULL;
  }
  sc->sc_cc->cc_query = save;
}


void
sqlg_bsp_trans (sql_comp_t * sc, df_elt_t * dt_dfe, trans_node_t * tn)
{
  state_slot_t *save_set_no = sc->sc_set_no_ssl;
  int save_sst = sc->sc_is_single_state;
  int is_single_state = sc->sc_is_single_state && dfe_is_1_input (dt_dfe);
  sc->sc_is_single_state = is_single_state;
  tn->tn_is_bsp = 1;
  tn->tn_is_single_state = is_single_state;
  tn->tn_ext_sets = ssl_new_variable (sc->sc_cc, "tn_ext_set", DV_LONG_INT);
  if (!is_single_state)
    tn->tn_int_set_no = ssl_new_vec (sc->sc_cc, "tn_set_no", DV_LONG_INT);
  tn_add_trans_dfg (sc, tn);
  sc->sc_is_single_state = save_sst;
  sc->sc_set_no_ssl = save_set_no;
}
