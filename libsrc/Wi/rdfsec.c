/*
 *  rdfsec.c
 *
 *  $Id$
 *
 *  RDF access scoping by in with chash
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
#include "arith.h"
#include "mhash.h"
#include "sqlparext.h"
#include "date.h"
#include "aqueue.h"
#include "sqlbif.h"
#include "datesupp.h"
#include "sqlpar.h"
#include "rdf_core.h"


id_hash_t *rdf_ctx_qrs;
dk_mutex_t rdf_ctx_qr_mtx;
dk_hash_t rdf_ctx_ht_ids;
extern user_t *user_t_dba;

int64 rdf_ctx_max_mem = 100000000;
int64 rdf_ctx_in_use;
long tc_rdf_ctx_hits;
long tc_rdf_ctx_misses;


#ifdef RDF_SECURITY_CLO

query_t *
sec_stmt_to_hash_qr (caddr_t * inst, caddr_t str)
{
  caddr_t err = NULL;
  query_t *qr, **place;
  QNCAST (QI, qi, inst);
  client_connection_t *cli = qi->qi_client;
  cl_op_t *sec = cli->cli_sec;
  int len = strlen (str);
  caddr_t stmt = dk_alloc_box (len + 200, DV_STRING);
  snprintf (stmt, box_length (stmt) - 1,
      "select dt.g from db.dba.rdf_quad rq, (%s) dt table option (hash, hash replication, hash unique, hash no drop, final) where rq.p < #i1 and dt.g = rq.g option (order)",
      str);
  mutex_enter (&rdf_ctx_qr_mtx);
  place = (query_t **) id_hash_get (rdf_ctx_qrs, (caddr_t) & str);
  if (place)
    {
      qr = *place;
      if (qr->qr_to_recompile)
	{
	  id_hash_remove (rdf_ctx_qrs, (caddr_t) & str);
	  mutex_leave (&rdf_ctx_qr_mtx);
	  qr = NULL;
	}
      else
	{
	  mutex_leave (&rdf_ctx_qr_mtx);
	  return qr;
	}
    }
  else
    mutex_leave (&rdf_ctx_qr_mtx);

  AS_DBA_C (qi, qr = sql_compile (stmt, qi->qi_client, &err, SQLC_DEFAULT));
  if (err)
    {
      if (sec)
	sec->_.sec.g_all_allowed = 0;
      sqlr_resignal (err);
    }
  mutex_enter (&rdf_ctx_qr_mtx);
  place = (query_t **) id_hash_get (rdf_ctx_qrs, (caddr_t) & str);
  if (place)
    {
      query_t *prev_qr = *place;
      mutex_leave (&rdf_ctx_qr_mtx);
      qr_free (qr);
      return prev_qr;
    }
  mutex_leave (&rdf_ctx_qr_mtx);
  return qr;
}



index_tree_t *
qi_last_hash_build (caddr_t * inst)
{
  QNCAST (QI, qi, inst);
  setp_node_t *setp;
  fun_ref_node_t *hf;
  query_t *qr = qi->qi_query;
  data_source_t *qn;
  for (qn = qr->qr_head_node; qn; qn = qn_next (qn))
    {
      if (IS_QN (qn, hash_fill_node_input))
	hf = (fun_ref_node_t *) qn;
    }
  if (!hf)
    sqlr_new_error ("CTXHF", "CTXHF", "Sec ctx hash build qr did not build any hash, wron plan");
  setp = hf->fnr_setp;
  return (index_tree_t *) QST_GET_V (inst, setp->setp_ha->ha_tree);
}

index_tree_t *
rdf_ctx_hash (query_instance_t * qi, caddr_t str, caddr_t params)
{
  /* returns a tree (chash in temp) from cache or makes a new one.  Returns with ref count incremented */
  client_connection_t *cli = qi->qi_client;
  local_cursor_t *lc;
  query_t *qr;
  caddr_t sign = list (2, str, params);
  index_tree_t **place, *tree, *prev_tree;
  IN_HIC;
  place = (index_tree_t **) id_hash_get (hash_index_cache.hic_hashes, (caddr_t) & sign);
  if (place)
    {
      tree = *place;
      tree->it_ref_count++;
      LEAVE_HIC;
      tc_rdf_ctx_hits++;
      tree->it_last_used = get_msec_real_time ();
      dk_free_box (sign);
      return tree;
    }
  LEAVE_HIC;
  tc_rdf_ctx_misses++;
  if (cli->cli_sec)
    cli->cli_sec->_.sec.g_all_allowed = 1;
  qr = sec_stmt_to_hash_qr ((caddr_t *) qi, str);
  qr_rec_exec (qr, qi->qi_client, &lc, qi, NULL, 1, ":0", box_copy_tree (params), QRP_RAW);
  if (cli->cli_sec)
    cli->cli_sec->_.sec.g_all_allowed = 0;
  tree = qi_last_hash_build (lc->lc_inst);
  if (lc->lc_error)
    {
      caddr_t err = lc->lc_error;
      lc->lc_error = NULL;
      if (tree)
	tree->it_ref_count = 1;	/* freed with the lc */
      lc_free (lc);
      sqlr_resignal (err);
    }
  lc_free (lc);
  if (tree)
    {
      tree->it_ref_count++;	/* now 2, one for this use and for being in hic */
      IN_HIC;
      place = (index_tree_t **) id_hash_get (hash_index_cache.hic_hashes, (caddr_t) & sign);
      if (place)
	{
	  prev_tree = tree;
	  tree = *place;
	  tree->it_ref_count++;
	  LEAVE_HIC;
	  prev_tree->it_ref_count = 1;
	  tree->it_last_used = get_msec_real_time ();
	  dk_free_box (sign);
	  dk_free_box ((caddr_t) prev_tree);
	  return tree;
	}
      tree->it_hi_signature = (hi_signature_t *) box_copy_tree ((caddr_t) sign);
      tree->it_hi->hi_is_rdf_ctx = 1;
      rdf_ctx_in_use += tree->it_hi->hi_chash->cha_pool->mp_bytes;
      tree->it_ref_count = 1;
      tree->it_shared = HI_OK;
      id_hash_set (hash_index_cache.hic_hashes, (caddr_t) & tree->it_hi_signature, (caddr_t) & tree);
      L2_PUSH (hash_index_cache.hic_first, hash_index_cache.hic_last, tree, it_hic_);
      LEAVE_HIC;
      tree->it_last_used = get_msec_real_time ();
      while (rdf_ctx_in_use > rdf_ctx_max_mem)
	{
	  if (!hic_pop_oldest_it (tree))
	    break;
	}
      return tree;
    }
  return NULL;
}


caddr_t
bif_rdf_ctx_changed (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  caddr_t qr = bif_arg (qst, args, 0, "rdf_ctx_changed");
  caddr_t params = bif_arg (qst, args, 1, "rdf_ctx_changed");
  dtp_t q_dtp = DV_TYPE_OF (qr);
  dtp_t p_dtp = DV_TYPE_OF (params);
  client_connection_t *cli = ((QI *) qst)->qi_client;
  if (DV_DB_NULL == q_dtp || DV_DB_NULL == p_dtp)
    {
    again:
      IN_HIC;
      DO_IDHASH (caddr_t *, key, index_tree_t *, tree, hash_index_cache.hic_hashes)
      {
	if (!tree->it_hi || !tree->it_hi->hi_is_rdf_ctx)
	  continue;
	if (DV_ARRAY_OF_POINTER != DV_TYPE_OF (key) || BOX_ELEMENTS (key) < 2)
	  continue;
	if ((DV_DB_NULL == q_dtp || box_equal (key[0], qr)) || (DV_DB_NULL == p_dtp || box_equal (key[1], params)))
	  {
	    it_hi_invalidate_in_mtx (tree);
	    LEAVE_HIC;
	    goto again;
	  }
      }
      END_DO_IDHASH;
      LEAVE_HIC;
    }
  else
    {
      caddr_t sign = list (2, qr, params);
      index_tree_t **place, *tree;
      IN_HIC;
      place = (index_tree_t **) id_hash_get (hash_index_cache.hic_hashes, (caddr_t) & sign);
      if (place)
	{
	  tree = *place;
	  it_hi_invalidate_in_mtx (tree);
	}
      LEAVE_HIC;
      dk_free_box (sign);
    }
  return NULL;
}


caddr_t
bif_rdf_set_writable (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  return NULL;
}

caddr_t
bif_rdf_set_sponge (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  return NULL;
}

void
cli_ensure_sec (query_instance_t * qi, client_connection_t * cli)
{
}


void
rdf_ctx_status ()
{
  size_t sz = 0;
  uint32 now = get_msec_real_time ();
  trset_printf ("RDF graph context cache:  %Ld hits %Ld misses, %d cached\n",
      tc_rdf_ctx_hits, tc_rdf_ctx_misses, hash_index_cache.hic_hashes->ht_count);
  IN_HIC;
  DO_IDHASH (caddr_t *, key, index_tree_t *, tree, hash_index_cache.hic_hashes)
  {
    if (!tree || !tree->it_hi || !tree->it_hi->hi_is_rdf_ctx)
      continue;
    if (DV_ARRAY_OF_POINTER != DV_TYPE_OF (key) || 2 != BOX_ELEMENTS (key))
      continue;
    sz += tree->it_hi->hi_chash->cha_pool->mp_bytes;
    trset_printf ("%d s ago %s %dK\n", (now - tree->it_last_used) / 1000, key[0], tree->it_hi->hi_chash->cha_pool->mp_bytes / 1024);
  }
  END_DO_IDHASH;
  LEAVE_HIC;
  trset_printf ("%dK total\n", sz / 1024);
}


void
rdf_sec_init ()
{
  rdf_ctx_qrs = id_str_hash_create (23);
  bif_define ("rdf_ctx_changed", bif_rdf_ctx_changed);
  bif_define ("rdf_set_writable", bif_rdf_set_writable);
  bif_define ("rdf_set_sponge", bif_rdf_set_sponge);
  dk_mutex_init (&rdf_ctx_qr_mtx, MUTEX_TYPE_SHORT);
}

#else

void
rdf_sec_init ()
{
}

void
rdf_ctx_status ()
{
}

void
cli_ensure_sec (query_instance_t * qi, client_connection_t * cli)
{
}

#endif
