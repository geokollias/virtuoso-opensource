/*
 *  clcli.c
 *
 *  $Id$
 *
 *  Cluster client end
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
#include "sqlver.h"
#include "log.h"
#ifndef WIN32
#include "sys/socket.h"
#include "netdb.h"
#include "netinet/tcp.h"
#endif
#include "sqlbif.h"

#define oddp(x) ((x) & 1)

int32 cl_msg_drop_rate;
int32 cl_con_drop_rate;
int32 drop_seed;


resource_t *clib_rc;


cll_in_box_t *
clib_allocate ()
{
  B_NEW_VARZ (cll_in_box_t, clib);
  clib->clib_req_strses = (dk_session_t *) resource_get (cl_strses_rc);
  clib->clib_req_strses->dks_cluster_flags = DKS_TO_CLUSTER;
  SESSION_SCH_DATA (&clib->clib_in_strses) = &clib->clib_in_siod;
  return clib;
}


void
clib_clear (cll_in_box_t * clib)
{
  dk_session_t *strses = clib->clib_req_strses;
  cl_queue_t clq = clib->clib_in;
  assert (!clq.rb_count);
  rb_ck_cnt (&clq);
  strses_flush (clib->clib_req_strses);
  clib->clib_req_strses->dks_cluster_flags = DKS_TO_CLUSTER;
  memzero (clib, (ptrlong) & ((cll_in_box_t *) 0)->clib_in_strses);
  clib->clib_in = clq;
  clib->clib_req_strses = strses;
  clib->clib_in_strses.dks_in_buffer = 0;
  clib->clib_in_strses.dks_in_fill = 0;
  clib->clib_in_strses.dks_in_read = 0;
  clib->clib_in_strses.dks_error = 0;
}


void
clib_free (cll_in_box_t * clib)
{
  if (clib->clib_req_strses)
    resource_store (cl_strses_rc, (void *) clib->clib_req_strses);
#ifdef CL_RBUF
  rbuf_destroy (&clib->clib_in);
#endif
  dk_free ((caddr_t) clib, sizeof (cll_in_box_t));
}


void
clib_rc_init ()
{
  clib_rc = resource_allocate (200, (rc_constr_t) clib_allocate, (rc_destr_t) clib_free, (rc_destr_t) clib_clear, 0);
}



void
id_hash_print (id_hash_t * ht)
{
  DO_IDHASH (void *, k, void *, d, ht)
  {
    printf ("%p -> %p\n", k, d);
  }
  END_DO_IDHASH;
}

void
ht_print (dk_hash_t * ht)
{
  DO_HT (void *, k, void *, d, ht)
  {
    printf ("%p -> %p\n", k, d);
  }
  END_DO_HT;
}


int64
__gethash64 (int64 i, dk_hash_t * ht)
{
  int64 r;
  gethash_64 (r, i, ht);
  return r;
}


void
__dk_hash_64_print (id_hash_t * ht)
{
  DO_IDHASH (int64, k, int64, d, ht)
  {
    printf ("%d:%d -> " BOXINT_FMTX "\n", QFID_HOST (k), (uint32) k, d);
  }
  END_DO_IDHASH;
}

uint32
cli_cl_way (client_connection_t * cli)
{
  if (cli->cli_cl_stack)
    return cli->cli_cl_stack[0].clst_req_no;
  if (!cli->cli_cl_way)
    cli->cli_cl_way = cl_way_ctr++;
  if (!cli->cli_cl_way)
    cli->cli_cl_way = CL_N_WAYS;
  return cli->cli_cl_way;
}


cl_req_group_t *
cl_req_group (lock_trx_t * lt)
{
  cl_req_group_t *clrg = (cl_req_group_t *) dk_alloc_box_zero (sizeof (cl_req_group_t), DV_CLRG);
  clrg->clrg_ref_count = 1;
  dk_mutex_init (&clrg->clrg_mtx, MUTEX_TYPE_SHORT);
  clrg->clrg_lt = lt;
  if (lt)
    {
      clrg->clrg_trx_no = lt->lt_main_trx_no ? lt->lt_main_trx_no : lt->lt_trx_no;
      clrg->clrg_cl_way = cli_cl_way (lt->lt_client);
    }
  else
    {
      clrg->clrg_cl_way = cl_way_ctr++;
      if (!clrg->clrg_cl_way)
	clrg->clrg_cl_way = CL_N_WAYS;
    }
  return clrg;
}


cl_req_group_t *
cl_req_group_qi (query_instance_t * qi)
{
  lock_trx_t *lt = qi->qi_trx;
  cl_req_group_t *clrg = (cl_req_group_t *) dk_alloc_box_zero (sizeof (cl_req_group_t), DV_CLRG);
  clrg->clrg_ref_count = 1;
  dk_mutex_init (&clrg->clrg_mtx, MUTEX_TYPE_SHORT);
  clrg->clrg_lt = lt;
  clrg->clrg_inst = (caddr_t *) qi;
  clrg->clrg_trx_no = lt->lt_main_trx_no ? lt->lt_main_trx_no : lt->lt_trx_no;
  clrg_top_check (clrg, qi);
  return clrg;
}


void
clrg_set_lt (cl_req_group_t * clrg, lock_trx_t * lt)
{
  clrg->clrg_lt = lt;
  clrg->clrg_trx_no = lt ? (lt->lt_main_trx_no ? lt->lt_main_trx_no : lt->lt_trx_no) : 0;
}

cl_req_group_t *
clrg_copy (cl_req_group_t * clrg)
{
  mutex_enter (&clrg->clrg_mtx);
  clrg->clrg_ref_count++;
  mutex_leave (&clrg->clrg_mtx);
  return clrg;
}



void
clrg_dml_free (cl_req_group_t * clrg)
{
  /* drop allocd mem for daq ins/del */
  DO_SET (cl_op_t *, clo, &clrg->clrg_vec_clos)
  {
    caddr_t *params = NULL;
    if (CLO_INSERT == clo->clo_op || CLO_DELETE == clo->clo_op)
      params = clo->_.insert.rd->rd_values;
    else if (CLO_CALL == clo->clo_op)
      params = clo->_.call.params;
    if (params)
      {
	int inx;
	DO_BOX (data_col_t *, dc, inx, params)
	{
	  if (DV_DATA == DV_TYPE_OF (dc))
	    dc_reset (dc);
	}
	END_DO_BOX;
      }
  }
  END_DO_SET ();
  clrg->clrg_vec_clos = NULL;
}




#define TA_DUP_CANCEL 1300


int
cl_is_dup_cancel (id_hash_t ** ht, int to_host, int coord, int req_no)
{
  int64 id;
  id_hash_t *dups;
  if (!*ht)
    {
      du_thread_t *self = THREAD_CURRENT_THREAD;
      dups = (id_hash_t *) THR_ATTR (self, TA_DUP_CANCEL);
      if (dups)
	*ht = dups;
      else
	{
	  *ht = dups = id_hash_allocate (123, sizeof (int64), 0, boxint_hash, boxint_hashcmp);
	  SET_THR_ATTR (self, TA_DUP_CANCEL, dups);
	}
    }
  else
    dups = *ht;
  if (boxint_hash != dups->ht_hash_func || 8 != dups->ht_ext_inx)
    GPF_T1 ("corrupt dups cancel ht");
  id = DFG_ID (((to_host << 16) | coord), req_no);
  if (id_hash_get (dups, (caddr_t) & id))
    return 1;
  id_hash_set (dups, (caddr_t) & id, NULL);
  return 0;
}

void
cl_clear_dup_cancel ()
{
}



int
clrg_destroy (cl_req_group_t * clrg)
{
  id_hash_t *dups = NULL;
  uint32 top_req_no = clrg->clrg_cl_way;
  mutex_enter (&clrg->clrg_mtx);
  clrg->clrg_ref_count--;
  if (clrg->clrg_ref_count)
    {
      mutex_leave (&clrg->clrg_mtx);
      return 1;
    }
  mutex_leave (&clrg->clrg_mtx);
  IN_CLL;
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    if (!clib->clib_req_no || clib->clib_fake_req_no)
      continue;			/* if no req no or a dfg sending clib, it is not really registered. If freed here, would remhash using a remote clib no and could collide dropping a local registration */
#if 0
    if (gethash ((void *) (ptrlong) clib->clib_req_no, &cl_ways[CL_NTH_WAY].clw_id_to_clib) != (void *) clib)
      GPF_T1 ("duplicate clib req no");
#endif
    remhash ((void *) (ptrlong) clib->clib_req_no, &cl_ways[CL_NTH_WAY].clw_id_to_clib);
  }
  END_DO_SET ();
  LEAVE_CLL;
  dk_free_tree (clrg->clrg_error);
  DO_SET (cll_in_box_t *, clib, &clrg->clrg_clibs)
  {
    cl_message_t *cm;
    cl_op_t *clo;
    /* clear the basket but do not free, the items are from the clib_local_pool */
    while ((clo = (cl_op_t *) basket_get (&clib->clib_local_clo)));
    /* clear the basket but the content is freed in freeing the params of the ts */
    dk_free_tree ((caddr_t) clib->clib_first_row);
    switch (clib->clib_first.clo_op)
      {
      case CLO_ROW:
	break;
      default:
	clo_destroy (&clib->clib_first);
      }
    if (clib->clib_local_pool)
      mp_free (clib->clib_local_pool);

    resource_store (clib_rc, (void *) clib);
  }
  END_DO_SET ();
  dk_set_free (clrg->clrg_clibs);
  clrg_dml_free (clrg);
  dk_mutex_destroy (&clrg->clrg_mtx);
  if (clrg->clrg_cu)
    cu_free (clrg->clrg_cu);
  if (clrg->clrg_pool)
    mp_free (clrg->clrg_pool);
  return 0;
}


cl_op_t *
clo_allocate (char op)
{
  cl_op_t *clo = (cl_op_t *) dk_alloc_box_zero (sizeof (cl_op_t), DV_CLOP);
  clo->clo_op = op;
  return clo;
}

cl_op_t *
clo_allocate_2 (char op)
{
  cl_op_t *clo = (cl_op_t *) dk_alloc_box_zero (sizeof (cl_op_t), DV_CLOP);
  clo->clo_op = op;
  return clo;
}

cl_op_t *
clo_allocate_3 (char op)
{
  cl_op_t *clo = (cl_op_t *) dk_alloc_box_zero (sizeof (cl_op_t), DV_CLOP);
  clo->clo_op = op;
  return clo;
}

cl_op_t *
clo_allocate_4 (char op)
{
  cl_op_t *clo = (cl_op_t *) dk_alloc_box_zero (sizeof (cl_op_t), DV_CLOP);
  clo->clo_op = op;
  return clo;
}



cl_op_t *
mp_clo_allocate (mem_pool_t * mp, char op)
{
  caddr_t box;
  cl_op_t *clo;
  switch (op)
    {
    case CLO_SET_END:
    case CLO_BATCH_END:
      MP_BYTES (box, mp, CLO_HEAD_SIZE);
      clo = (cl_op_t *) box;
      clo->clo_clibs = NULL;
      break;
    case CLO_ROW:
      MP_BYTES (box, mp, CLO_ROW_SIZE);
      clo = (cl_op_t *) box;
      clo->_.row.cols = NULL;
      clo->_.row.local_dcs = NULL;
      clo->clo_clibs = NULL;
      break;
    case CLO_CALL:
      MP_BYTES (box, mp, CLO_CALL_SIZE);
      clo = (cl_op_t *) box;
      memset (clo, 0, CLO_CALL_SIZE);
      break;
    default:
      MP_BYTES (box, mp, sizeof (cl_op_t));
      clo = (cl_op_t *) box;
      memset (clo, 0, sizeof (cl_op_t));
    }
  clo->clo_pool = mp;
  clo->clo_op = op;
  return clo;
}





int
clo_destroy (cl_op_t * clo)
{
  dk_set_free (clo->clo_clibs);
  switch (clo->clo_op)
    {
    case CLO_INSERT:
      if (clo->_.insert.rd)
	{
	  if (clo->_.insert.rd->rd_itc)
	    itc_free (clo->_.insert.rd->rd_itc);
	  rd_free (clo->_.insert.rd);
	}
      break;
    case CLO_DELETE:
      if (clo->_.delete_op.rd)
	{
	  if (clo->_.delete_op.rd->rd_itc)
	    itc_free (clo->_.delete_op.rd->rd_itc);
	  rd_free (clo->_.delete_op.rd);
	}
      break;
    case CLO_SELECT:
      if (clo->_.select.itc)
	itc_free (clo->_.select.itc);
      break;
    case CLO_SELECT_SAME:
      dk_free_tree ((caddr_t) clo->_.select_same.params);
      break;
    case CLO_ROW:
      dk_free_tree ((caddr_t) clo->_.row.cols);
      break;
    case CLO_ERROR:
      dk_free_tree ((caddr_t) clo->_.error.err);
      break;
    case CLO_ITCL:
      if (!clo->_.itcl.itcl)
	break;
      dk_free_box ((caddr_t) clo->_.itcl.itcl->itcl_clrg);
      mp_free (clo->_.itcl.itcl->itcl_pool);
      dk_free ((caddr_t) clo->_.itcl.itcl, sizeof (itc_cluster_t));
      break;
    case CLO_CALL:
      dk_free_tree (clo->_.call.func);
      dk_free_tree ((caddr_t) (clo->_.call.params));
      break;
    case CLO_QF_EXEC:
      dk_free_tree ((caddr_t) clo->_.frag.params);
      break;
    case CLO_STN_IN:
      cl_msg_string_free (clo->_.stn_in.in);
      break;
    case CLO_DFG_ARRAY:
      dk_free_tree ((caddr_t) clo->_.dfg_array.stats);
      break;
    case CLO_DFG_STATE:
      dk_free_box ((caddr_t) clo->_.dfg_stat.out_counts);
      break;
    case CLO_TOP:
      return clo_top_free (clo);
    case CLO_IEXT_CR:
      clo->_.iext.free_cb (clo->_.iext.cr);
      break;
    case CLO_SEC_TOKEN:
      dk_free_tree ((caddr_t) clo->_.sec.g_wr);
      dk_free_tree ((caddr_t) clo->_.sec.g_rd);
      break;
    }
  return 0;
}





void
clrg_top_check (cl_req_group_t * clrg, query_instance_t * top_qi)
{
  /* if this is first cluster op on a thread not invoked from cluster set up a clib for getting recursive cluster ops */
  if (0 && cl_run_local_only == CL_RUN_LOCAL)
    {
      clrg->clrg_cl_way = cl_way_ctr++;
      if (!clrg->clrg_cl_way)
	clrg->clrg_cl_way = CL_N_WAYS;
      return;
    }
  if (top_qi && top_qi->qi_client->cli_cl_stack && !clrg)
    return;
  if (!top_qi)
    top_qi = (QI *) clrg->clrg_inst;
  top_qi = qi_top_qi (top_qi);
  if (!top_qi)
    GPF_T1 ("clrg top check requires top qi");
  if (top_qi->qi_is_cl_root)
    {
      if (!top_qi->qi_client->cli_cl_stack)
	GPF_T1 ("The cli on a cl root qi must have a cl stack");
      if (clrg)
	clrg->clrg_cl_way = top_qi->qi_client->cli_cl_stack[0].clst_req_no;
      return;
    }
  if (!top_qi->qi_client->cli_cl_stack)
    {
      int64 top_id;
      uint32 top_req_no = cl_way_ctr++;
      cl_call_stack_t *clst;
      cll_in_box_t *top_clib = (cll_in_box_t *) resource_get (clib_rc);
      NEW_VARZ (cl_top_req_t, ctop);
      if (!top_req_no)
	top_req_no = CL_N_WAYS;
      ctop->ctop_n_threads = 1;
      if (clrg)
	clrg->clrg_cl_way = top_req_no;
      top_qi->qi_is_cl_root = 1;
      top_clib->clib_is_top_coord = 1;
      IN_CLL;
      clib_assign_req_no (top_clib, top_req_no);
      top_id = DFG_ID (local_cll.cll_this_host, top_clib->clib_req_no);;
      ctop->ctop_req_no = top_id;
      sethash ((void *) top_id, &cl_ways[CL_NTH_WAY].clw_top_req, (void *) ctop);
      LEAVE_CLL;
      clst = top_qi->qi_client->cli_cl_stack = (cl_call_stack_t *) dk_alloc_box (sizeof (cl_call_stack_t), DV_BIN);
      top_qi->qi_client->cli_cl_stack_ref_count = 1;
      clst->clst_host = local_cll.cll_this_host;
      clst->clst_req_no = top_clib->clib_req_no;
    }
  else
    {
      clrg->clrg_cl_way = top_qi->qi_client->cli_cl_stack[0].clst_req_no;
      if (IS_BOX_POINTER (top_qi->qi_caller))
	GPF_T1 ("top qi has a caller");
      if (!top_qi->qi_client->cli_cl_stack_ref_count)
	return;
      if (!top_qi->qi_is_cl_root)
	{
	  top_qi->qi_is_cl_root = 1;
	  top_qi->qi_client->cli_cl_stack_ref_count++;
	}
    }
}


void
cli_free_stack (client_connection_t * cli)
{
  dk_set_t replies = NULL;
  cll_in_box_t *clib = NULL;
  uint32 top_req_no = cli->cli_cl_stack[0].clst_req_no;
  int coord = cli->cli_cl_stack[0].clst_host;;
  uint64 top_id = DFG_ID (local_cll.cll_this_host, top_req_no);
  cl_top_req_t *ctop;
  if (coord != local_cll.cll_this_host)
    return;
  cli->cli_cl_stack_ref_count--;
  if (cli->cli_cl_stack_ref_count)
    return;
  IN_CLL;
  clib = (cll_in_box_t *) gethash ((void *) (ptrlong) cli->cli_cl_stack->clst_req_no, &cl_ways[CL_NTH_WAY].clw_id_to_clib);
  remhash ((void *) (ptrlong) cli->cli_cl_stack->clst_req_no, &cl_ways[CL_NTH_WAY].clw_id_to_clib);
  ctop = (cl_top_req_t *) gethash ((void *) top_id, &cl_ways[CL_NTH_WAY].clw_top_req);
  if (ctop->ctop_n_threads < 1)
    GPF_T1 ("ctop on coord must have at least 1 thread at end of its owner qi");
  ctop->ctop_n_threads--;
  if (ctop && !ctop->ctop_n_threads)
    remhash ((void *) top_id, &cl_ways[CL_NTH_WAY].clw_top_req);
  if (ctop)
    {
      DO_RBUF (clo_queue_t *, cloq, rbe, rbe_inx, &ctop->ctop_rb)
      {
	DO_RBUF (cl_message_t *, cm, rbe, rbe_inx, &cloq->cloq_rb)
	{
	  int coord;
	  int64 k;
	  coord = CMR_DFG & cm->cm_req_flags ? cm->cm_dfg_coord : cm->cm_from_host;
	  k = DFG_ID (coord, cm->cm_req_no);
	  dk_set_pushnew (&replies, (void *) k);
	  remhash_64 (k, &cl_ways[CL_NTH_WAY].clw_req);
	  cm_free (cm);
	}
	END_DO_RBUF;
	rbuf_delete_all (&cloq->cloq_rb);
	resource_store (cl_ways[CL_NTH_WAY].clw_cloqs, (void *) cloq);
      }
      END_DO_RBUF;
      rbuf_delete_all (&ctop->ctop_rb);
      if (!ctop->ctop_n_threads)
	resource_store (cl_ways[CL_NTH_WAY].clw_top_reqs, (void *) ctop);
    }
  LEAVE_CLL;
  if (clib)
    resource_store (clib_rc, (void *) clib);
  if (replies)
    {
      caddr_t err = srv_make_new_error ("S1TER", "CLDON", "recursive op rejected because root qi already terminated");
      cl_message_t t_cm;
      memzero (&t_cm, sizeof (t_cm));
      dk_free_tree (err);
    }
  dk_free_box ((caddr_t) cli->cli_cl_stack);
  cli->cli_cl_stack = NULL;
}

void
cm_set_quota (cl_req_group_t * clrg, cl_message_t * cm, int time)
{
  client_connection_t *cli = clrg->clrg_lt->lt_client;
  if (!time)
    clrg->clrg_send_time = get_msec_real_time ();
  if (cli->cli_anytime_started)
    cm->cm_anytime_quota = cli->cli_anytime_timeout - (clrg->clrg_send_time - cli->cli_anytime_started);
  else
    cm->cm_anytime_quota = 0;
}

void
clib_assign_req_no (cll_in_box_t * clib, uint32 top_req_no)
{
  uint32 req_no;
  cl_way_t *clw = &cl_ways[CL_NTH_WAY];
  ASSERT_IN_CLL;
  do
    {
      req_no = clw->clw_next_req_no += CL_N_WAYS;
    }
  while (req_no == 0 || gethash ((void *) (ptrlong) req_no, &clw->clw_id_to_clib));
  sethash ((void *) (ptrlong) req_no, &clw->clw_id_to_clib, (void *) clib);
  clib->clib_req_no = req_no;
}


int
clo_serialize (cll_in_box_t * clib, cl_op_t * clo)
{
  int bytes;
  bytes = strses_out_bytes (clib->clib_req_strses);
  if (clo->clo_pool)
    mp_set_push (clo->clo_pool, &clo->clo_clibs, (void *) clib);
  else
    dk_set_push (&clo->clo_clibs, (void *) clib);
  cl_serialize_cl_op_t (clib->clib_req_strses, clo);
  clib->clib_group->clrg_send_buffered += strses_out_bytes (clib->clib_req_strses) - bytes;
  return CLE_OK;
}



void
clrg_add_clib (cl_req_group_t * clrg, cll_in_box_t * clib)
{
  dk_set_push (&clrg->clrg_clibs, (void *) clib);
}

int
clrg_add (cl_req_group_t * clrg, cl_host_t * host, cl_op_t * clop)
{
  return 0;
}

int
clrg_add_slice (cl_req_group_t * clrg, cl_host_t * host, cl_op_t * clop, slice_id_t slid)
{
  return 0;
}


extern du_thread_t *recomp_thread;

int
clrg_wait (cl_req_group_t * clrg, int wait_all, caddr_t * qst)
{
  return CLE_OK;
}


#define CLIB_SER_CK(clib) \
  if (clib->clib_in_strses.dks_error) \
    { \
      clib->clib_first.clo_op = CLO_ERROR;					\
      log_error (" RARE - Data format error in cluster batch reply"); \
      clib->clib_first._.error.err = srv_make_new_error ("CLINT", "CLINT", "Bad data format in cluster message.  Corrupted in transmission.  If no other network problems and this predictably repeats, then contact support"); \
      return; \
   }



void
clib_read_next (cll_in_box_t * clib, caddr_t * inst, dk_set_t out_slots)
{
  /* non cluster dfg only variant */
  dtp_t op;
  dk_session_t *ses = &clib->clib_in_strses;
  if (ses->dks_in_read >= ses->dks_in_fill)
    GPF_T1 ("not supposed to be at end of cl message in single server dfg ");

  clib->clib_prev_read_to = ses->dks_in_read;
  op = ses->dks_in_buffer[ses->dks_in_read];
  {
    cl_ses_t *save = DKS_CL_DATA (ses);
    CATCH_READ_FAIL (ses)
    {
      cl_op_t *new_clo;
      cl_ses_t clses;
      clo_destroy (&clib->clib_first);
      clib->clib_first.clo_op = CLO_NONE;
      new_clo = cl_deserialize_cl_op_t (ses);
      DKS_CL_DATA (ses) = save;
      if (save)
	save->clses_stat_inst = NULL;
      clib->clib_first = *new_clo;
      box_tag_modify (new_clo, DV_CUSTOM);
      dk_free_box ((caddr_t) new_clo);
    }
    FAILED
    {
      DKS_CL_DATA (ses) = save;
      if (save)
	save->clses_stat_inst = NULL;
    }
    END_READ_FAIL (ses);
    CLIB_SER_CK (clib);
  }
}


void
cl_local_skip_to_set (cll_in_box_t * clib)
{
  while (clib->clib_in_parsed.bsk_count)
    {
      cl_op_t *clo = (cl_op_t *) basket_first (&clib->clib_in_parsed);
      if (CLO_ERROR == clo->clo_op)
	sqlr_resignal (clo->_.error.err);
      if (clo->clo_nth_param_row >= clib->clib_skip_target_set)
	return;
      basket_get (&clib->clib_in_parsed);
    }
  for (;;)
    {
      cl_op_t *clo = (cl_op_t *) basket_first (&clib->clib_local_clo);
      if (!clo)
	return;
      if (clo->clo_nth_param_row < clib->clib_skip_target_set)
	basket_get (&clib->clib_local_clo);
      else
	return;
    }
}
