/*
 *  clsrv.c
 *
 *  $Id$
 *
 *  Cluster server end
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

#ifdef unix
#include <sys/resource.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#endif

#include "datesupp.h"
#include "sqlnode.h"
#include "sqlver.h"
#include "sqlparext.h"
#include "sqlbif.h"
#include "security.h"
#include "log.h"

dk_mutex_t cll_conn_mtx;
dk_mutex_t cll_cfg_mtx;
cl_way_t cl_ways[CL_N_WAYS];
dk_mutex_t cll_qf_mtx;
cl_listener_t local_cll;
dk_mutex_t cll_thread_mtx;
uint32 cl_way_ctr;
du_thread_t *cl_listener_thr;
int32 cl_stage;
int32 cl_max_hosts = 100;
cl_host_t *cl_master;
dk_set_t cluster_hosts;
resource_t *cluster_threads;
resource_t *cl_strses_rc;	/* strses structs used for receiving and sending cluster messages */
dk_mutex_t *cluster_thread_mtx;
int64 cl_cum_messages, cl_cum_bytes, cl_cum_txn_messages;
int64 cll_entered;
int64 cll_lines[1000];
int cll_counts[1000];
resource_t *cl_buf_rc;
int64 cl_cum_wait, cl_cum_wait_msec;
char *c_cluster_listen;
int32 c_cluster_threads;
int32 cl_keep_alive_interval = 3000;
int32 cl_max_keep_alives_missed = 4;
int32 cl_batches_per_rpc;
int32 cl_req_batch_size;	/* no of request clo's per message */
int32 cl_dfg_batch_bytes = 10000000;
int32 cl_mt_read_min_bytes = 10000000;	/* if more than this much, read the stuff on the worker thread, not the dispatch thread */
uint32 cl_send_high_water;
int32 cl_res_buffer_bytes;	/* no of bytes before sending to client */
int c_cl_no_unix_domain;
char *c_cluster_local;
char *c_cluster_master;
caddr_t cl_map_file_name;
int cluster_enable = 0;
int32 cl_n_hosts;
cluster_map_t *clm_replicated;
cluster_map_t *clm_all;
cluster_map_t *clm_local_slices;
cl_thread_t *listen_clt;
int cl_n_clt_running, cl_n_clt_start, clt_n_late_cancel;
int32 cl_wait_query_delay = 20000;	/* wait 20s before requesting forced sync of cluster wait graph */
int32 cl_non_logged_write_mode;
int32 cl_batch_bytes = 10 * PAGE_SZ;
rbuf_t cll_undelivered;
int64 cll_bytes_undelivered;
int64 cll_max_undelivered = 150000000;
rbuf_t cl_alt_rbuf;
int cl_alt_threads;
int cl_max_alt_threads;
int enable_cl_alt_queue = 0;
int enable_cll_nb_read;
int enable_hduplex = 0;
char cll_stay_in_cll_mtx;
long tc_over_max_undelivered;
long tc_cll_undelivered;
long tc_cll_mt_read;
extern long tc_cl_disconnect;
extern long tc_cl_disconnect_in_clt;
int cl_no_init = 0;



typedef struct cll_line_s
{
  int64 cll_time;
  int cll_count;
  int cll_line;
} cll_line_t;


int
cll_cmp (const void *l1, const void *l2)
{
  if (((cll_line_t *) l1)->cll_time > ((cll_line_t *) l2)->cll_time)
    return 1;
  return -1;
}

void
cll_times ()
{
  cll_line_t l[1000];
  int inx, fill = 0;
  for (inx = 0; inx < 1000; inx++)
    {
      if (cll_lines[inx])
	{
	  l[fill].cll_time = cll_lines[inx];
	  l[fill].cll_count = cll_counts[inx];
	  l[fill++].cll_line = inx;
	}
    }
  qsort (l, fill, sizeof (cll_line_t), cll_cmp);
  for (inx = 0; inx < fill; inx++)
    printf ("%d:  " BOXINT_FMT " %d\n", l[inx].cll_line, l[inx].cll_time, l[inx].cll_count);
#ifdef MTX_METER
  DO_CLW (clw)
  {
    printf ("cll waits: %ld w: %Ld e: %ld\n", clw->clw_mtx.mtx_wait_clocks, clw->clw_mtx.mtx_waits, clw->clw_mtx.mtx_enters);
    clw->clw_mtx.mtx_wait_clocks = clw->clw_mtx.mtx_enters = clw->clw_mtx.mtx_waits = 0;
  }
  END_DO_CLW;
#endif
  memzero (cll_counts, sizeof (cll_counts));
  memzero (cll_lines, sizeof (cll_lines));
}



caddr_t
bif_cll_times (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  cll_times ();
  return NULL;
}


#ifdef MTX_METER
int
cll_try_enter (uint32 top_req_no)
{
  if (mutex_try_enter (&cl_ways[CL_NTH_WAY].clw_mtx))
    {
      cll_entered = rdtsc ();
      return 1;
    }
  return 0;
}
#endif


dk_session_t *
cl_strses_allocate ()
{
  dk_session_t *ses;
  /* the head and 1st buffer of strses come from common, the extension will come from the user thread */
  WITH_TLSF (dk_base_tlsf) ses = strses_allocate ();
  END_WITH_TLSF;
  ses->dks_cluster_flags = DKS_TO_CLUSTER;
  return ses;
}


void
cl_strses_free (dk_session_t * ses)
{
  dk_free_box ((caddr_t) ses);
}


void
null_serialize (caddr_t x, dk_session_t * ses)
{
  session_buffered_write_char (DV_DB_NULL, ses);
}



int64
read_boxint (dk_session_t * ses)
{
  dtp_t tag = session_buffered_read_char (ses);
  switch (tag)
    {
    case DV_LONG_INT:
      return read_long (ses);
    case DV_INT64:
      return read_int64 (ses);
    case DV_SHORT_INT:
      return (char) session_buffered_read_char (ses);
    default:
      box_read_error (ses, tag);
    }
  return 0;
}


resource_t *cl_str_1;
resource_t *cl_str_2;
resource_t *cl_str_3;
resource_t *cl_str_4;
resource_t *cl_str_5;
resource_t *cl_str_6;

#define CL_MSG_SIZE_1 256
#define CL_MSG_SIZE_2 (8192 * 2)
#define CL_MSG_SIZE_3 cl_msg_size_3
#define CL_MSG_SIZE_4 cl_msg_size_4
#define CL_MSG_SIZE_5 cl_msg_size_5
#define CL_MSG_SIZE_6 cl_msg_size_6
int cl_msg_size_3;
int cl_msg_size_4;
int cl_msg_size_5;
int cl_msg_size_6;

caddr_t
cl_msg_alloc (void *cd)
{
  return dk_alloc_box ((ptrlong) cd, DV_STRING);
}

void
cl_msg_free (caddr_t str)
{
  dk_free_box (str);
}

caddr_t
cl_msg_string (int64 bytes)
{
  if (bytes < CL_MSG_SIZE_1)
    return (caddr_t) resource_get (cl_str_1);
  if (bytes < CL_MSG_SIZE_2)
    return (caddr_t) resource_get (cl_str_2);
  if (bytes < CL_MSG_SIZE_3)
    return (caddr_t) resource_get (cl_str_3);
  if (bytes < CL_MSG_SIZE_4)
    return (caddr_t) resource_get (cl_str_4);
  if (bytes < CL_MSG_SIZE_5)
    return (caddr_t) resource_get (cl_str_5);
  if (bytes < CL_MSG_SIZE_6)
    return (caddr_t) resource_get (cl_str_6);
  return dk_alloc_box_long (bytes, DV_STRING);
}

void
cl_msg_string_free (caddr_t str)
{
  int len = box_length (str);
  if (CL_MSG_SIZE_1 == len)
    resource_store (cl_str_1, (void *) str);
  else if (CL_MSG_SIZE_2 == len)
    resource_store (cl_str_2, (void *) str);
  else if (CL_MSG_SIZE_3 == len)
    resource_store (cl_str_3, (void *) str);
  else if (CL_MSG_SIZE_4 == len)
    resource_store (cl_str_4, (void *) str);
  else if (CL_MSG_SIZE_5 == len)
    resource_store (cl_str_5, (void *) str);
  else if (CL_MSG_SIZE_6 == len)
    resource_store (cl_str_6, (void *) str);
  else
    dk_free_box (str);
}



typedef struct cmfree_s
{
  cl_message_t *cmf_cm;
  char *cmf_file;
  int cmf_line;
} cm_free_t;

#define CMF_HIST 1000
cm_free_t cmf_hist[CMF_HIST];
int cmf_ctr;

void
cm_free_1 (cl_message_t * cm, char *file, int line)
{
  int ctr = (cmf_ctr++) % CMF_HIST;
  cmf_hist[ctr].cmf_cm = cm;
  cmf_hist[ctr].cmf_line = line;
  cmf_hist[ctr].cmf_file = file;
  if (cm->cm_strses)
    resource_store (cl_strses_rc, (void *) cm->cm_strses);
  if (cm->cm_in_string)
    cl_msg_string_free (cm->cm_in_string);
  dk_free_box ((caddr_t) cm->cm_sec);
  CM_FREE_TRACE (cm, file, line);
  dk_free ((caddr_t) cm, sizeof (cl_message_t));
}


cl_slice_t *
clm_id_to_slice (cluster_map_t * clm, slice_id_t slid)
{
  return (cl_slice_t *) gethash ((void *) (ptrlong) slid, clm->clm_id_to_slice);
}

void
cluster_dummy_host ()
{
  /* when no cluster, set th the minimum structs */
  NEW_VARZ (cl_host_t, ch);
  local_cll.cll_id_to_host = hash_table_allocate (1);
  ch->ch_id = 1;
  ch->ch_name = box_dv_short_string ("Host1");
  ch->ch_is_local_host = 1;
  sethash ((void *) (ptrlong) ch->ch_id, local_cll.cll_id_to_host, (void *) ch);
  dk_set_push (&cluster_hosts, (void *) ch);
  local_cll.cll_this_host = 1;
  local_cll.cll_max_host = 1;
  local_cll.cll_local = ch;
}


extern dk_hash_t *dfg_running_queue;

void
partition_def_bif_define (void)
{
}


inline caddr_t
cl_buf_str_alloc ()
{
  return (caddr_t) dk_alloc (DKSES_OUT_BUFFER_LENGTH);
}


void
cl_buf_str_free (caddr_t str)
{
  dk_free (str, DKSES_OUT_BUFFER_LENGTH);
}



cl_top_req_t *
ctop_allocate ()
{
  NEW_VARZ (cl_top_req_t, ctop);
  return ctop;
}


caddr_t
dv_clo_deserialize (dk_session_t * ses, dtp_t dtp)
{
  return (caddr_t) cl_deserialize_cl_op_t (ses);
}


void
dv_clo_serialize (caddr_t clo, dk_session_t * out)
{
  if (CLO_SEC_TOKEN == ((cl_op_t *) clo)->clo_op)
    {
      session_buffered_write_char (DV_CLOP, out);
      cl_serialize_cl_op_t (out, (cl_op_t *) clo);
    }
  else
    session_buffered_write_char (DV_DB_NULL, out);
}


void
ctop_free (cl_top_req_t * ctop)
{
  rbuf_destroy (&ctop->ctop_rb);
  dk_free ((caddr_t) ctop, sizeof (cl_top_req_t));
}


void
ctop_clear (cl_top_req_t * ctop)
{
  rbuf_delete_all (&ctop->ctop_rb);
  memzero (ctop, (ptrlong) & (((cl_top_req_t *) 0)->ctop_rb));
}



clo_queue_t *
cloq_allocate ()
{
  NEW_VARZ (clo_queue_t, cloq);
  return cloq;
}


void
cloq_free (clo_queue_t * cloq)
{
  rbuf_destroy (&cloq->cloq_rb);
  dk_free ((caddr_t) cloq, sizeof (clo_queue_t));
}


void
cloq_clear (clo_queue_t * cloq)
{
  rbuf_delete_all (&cloq->cloq_rb);
  memzero (cloq, (ptrlong) & (((clo_queue_t *) 0)->cloq_rb));
}



void
cluster_init ()
{
  PCONFIG pconfig, pconfig_g;
  dk_set_t master_list = NULL;
  cl_host_t *local_ch;
  int inx;
  c_cluster_threads = 200;
  for (inx = 0; inx < CL_N_WAYS; inx++)
    {
      char mtx_name[20];
      cl_way_t *clw = &cl_ways[inx];
      dk_mutex_init (&clw->clw_mtx, MUTEX_TYPE_SHORT);
      snprintf (mtx_name, sizeof (mtx_name), "CLL:%d", inx);
      mutex_option (&clw->clw_mtx, mtx_name, NULL, NULL);
      hash_table_init (&clw->clw_top_req, c_cluster_threads);
      hash_table_init (&clw->clw_req, 2 * c_cluster_threads);
      hash_table_init (&clw->clw_id_to_clib, 3 * c_cluster_threads);
      hash_table_init (&clw->clw_wait_clrg, 3 * 11);
      hash_table_init (&clw->clw_id_to_cm, c_cluster_threads);
      hash_table_init (&clw->clw_closed_qfs, 1100);
      hash_table_init (&clw->clw_dfg_running_queue, 101);
      clw->clw_dfg_running_queue.ht_rehash_threshold = 2;
      clw->clw_top_req.ht_rehash_threshold = 2;
      clw->clw_req.ht_rehash_threshold = 2;
      clw->clw_id_to_cm.ht_rehash_threshold = 2;
      clw->clw_id_to_clib.ht_rehash_threshold = 2;
      clw->clw_wait_clrg.ht_rehash_threshold = 2;
      clw->clw_closed_qfs.ht_rehash_threshold = 2;
      clw->clw_dfg_running_queue.ht_rehash_threshold = 2;
      clw->clw_next_req_no = CL_N_WAYS + inx;
      clw->clw_top_reqs =
	  resource_allocate (MDBG_RC_SIZE ((c_cluster_threads / CL_N_WAYS) + 10), (rc_constr_t) ctop_allocate,
	  (rc_destr_t) ctop_free, (rc_destr_t) ctop_clear, 0);
      clw->clw_cloqs =
	  resource_allocate (MDBG_RC_SIZE (c_cluster_threads), (rc_constr_t) cloq_allocate, (rc_destr_t) cloq_free,
	  (rc_destr_t) cloq_clear, 0);
      HT_REQUIRE_MTX (&clw->clw_id_to_cm, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_id_to_clib, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_closed_qfs, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_req, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_top_req, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_wait_clrg, &clw->clw_mtx);
      HT_REQUIRE_MTX (&clw->clw_dfg_running_queue, &clw->clw_mtx);
      clw->clw_rbuf_rc = resource_allocate (600, (rc_constr_t) rbuf_allocate, (rc_destr_t) dk_free_box, NULL, 0);
      resource_no_sem (clw->clw_rbuf_rc);
    }
  dk_mutex_init (&cll_thread_mtx, MUTEX_TYPE_SHORT);
  mutex_option (&cll_thread_mtx, "cl_thr", NULL, NULL);
  dk_mutex_init (&cll_qf_mtx, MUTEX_TYPE_SHORT);
  mutex_option (&cll_qf_mtx, "cl_qf", NULL, NULL);
  dk_mutex_init (&cll_conn_mtx, MUTEX_TYPE_SHORT);
  mutex_option (&cll_conn_mtx, "cl_conn", NULL, NULL);
  dk_mutex_init (&cll_cfg_mtx, MUTEX_TYPE_SHORT);
  dk_mem_hooks (DV_CLOP, box_non_copiable, (box_destr_f) clo_destroy, 0);
  clib_rc_init ();
  cl_strses_rc =
      resource_allocate (300, (rc_constr_t) cl_strses_allocate, (rc_destr_t) cl_strses_free, (rc_destr_t) strses_flush, NULL);
  dk_mem_hooks (DV_CLRG, (box_copy_f) clrg_copy, (box_destr_f) clrg_destroy, 0);
  PrpcSetWriter (DV_CLRG, (ses_write_func) null_serialize);
  /* inited whether cluster or single */
  cl_str_1 = resource_allocate (400, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) CL_MSG_SIZE_1);
  cl_str_2 = resource_allocate (400, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) CL_MSG_SIZE_2);
  cl_msg_size_3 = mm_next_size (80000, &inx) - 16;
  cl_msg_size_4 = mm_next_size (240000, &inx) - 16;
  cl_msg_size_5 = mm_next_size (1100000, &inx) - 16;
  cl_msg_size_6 = mm_next_size (1100000, &inx) - 16;
  cl_str_3 = resource_allocate (50, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) (ptrlong) CL_MSG_SIZE_3);
  cl_str_4 = resource_allocate (30, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) (ptrlong) CL_MSG_SIZE_4);
  cl_str_5 = resource_allocate (30, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) (ptrlong) CL_MSG_SIZE_5);
  cl_str_6 = resource_allocate (30, (rc_constr_t) cl_msg_alloc, (rc_destr_t) cl_msg_free, NULL, (void *) (ptrlong) CL_MSG_SIZE_6);
  cl_buf_rc = resource_allocate (400, (rc_constr_t) cl_buf_str_alloc, (rc_destr_t) cl_buf_str_free, NULL, NULL);
  PrpcSetWriter (DV_DATA, (ses_write_func) dc_serialize);
  get_readtable ()[DV_DATA] = (macro_char_func) dc_deserialize;

  if (cfg_init (&pconfig, "cluster.ini") == -1)
    {
      cl_run_local_only = 1;
      cl_req_batch_size = 1;
      cluster_dummy_host ();
      partition_def_bif_define ();
      bif_daq_init ();
      return;
    }
}


extern int sdfg_max_slices;

void
clm_single_extra_slices (cluster_map_t * clm)
{
  /* in single server cases of dfg, it can be the partitioning is n way but there can be more than n qp threads.  So even if a sdfg stage would not split  more than n ways, make more slices becausse a ts can split more ways */
  int id;
  for (id = enable_qp; id < sdfg_max_slices; id++)
    {
      NEW_VARZ (cl_slice_t, csl);
      csl->csl_id = id;
      csl->csl_chg = clm->clm_hosts[0];
      sethash ((void *) (ptrlong) csl->csl_id, clm->clm_id_to_slice, (void *) csl);
      csl->csl_clm = clm;
      csl->csl_is_local = 1;
    }
}

cl_host_t *
cl_name_to_host (const char *name)
{
  return NULL;
}

cl_host_t *
cl_id_to_host (int id)
{
  return local_cll.cll_local;
}

#if 0				/* This is in feature/fx2 but not in feature/analytics before meking it C++ friendly */
inline caddr_t
cl_buf_str_alloc ()
{
  return (caddr_t) dk_alloc (DKSES_OUT_BUFFER_LENGTH);
}

void
cl_buf_str_free (caddr_t str)
{
  dk_free (str, DKSES_OUT_BUFFER_LENGTH);
}


void
cluster_init ()
{
  local_cll.cll_mtx = mutex_allocate ();
  dk_mem_hooks (DV_CLOP, box_non_copiable, (box_destr_f) clo_destroy, 0);
  cl_strses_rc =
      resource_allocate (30, (rc_constr_t) cl_strses_allocate, (rc_destr_t) cl_strses_free, (rc_destr_t) strses_flush, NULL);
  clib_rc_init ();
  dk_mem_hooks (DV_CLRG, (box_copy_f) clrg_copy, (box_destr_f) clrg_destroy, 0);
  PrpcSetWriter (DV_CLRG, (ses_write_func) null_serialize);
}
#endif

char *
cl_thr_stat ()
{
  return "";
}

int32 cl_ac_interval = 100000;

void
cluster_after_online ()
{
}


void
clm_init_elastic_single (cluster_map_t * clm)
{
  int n_chg = BOX_ELEMENTS (clm->clm_hosts), inx;
  clm->clm_distinct_slices = n_chg * clm->clm_init_slices;
  DO_BOX (cl_host_group_t *, chg, inx, clm->clm_hosts)
  {
    int fill = 0, hi, id;
    chg->chg_hosted_slices = (cl_slice_t **) dk_alloc_box (sizeof (caddr_t) * clm->clm_init_slices, DV_BIN);
    for (id = 0; id < clm->clm_init_slices * n_chg; id += n_chg)
      {
	NEW_VARZ (cl_slice_t, csl);
	csl->csl_id = inx + id;
	csl->csl_chg = chg;
	sethash ((void *) (ptrlong) csl->csl_id, clm->clm_id_to_slice, (void *) csl);
	csl->csl_clm = clm;
	chg->chg_hosted_slices[fill++] = csl;
	DO_BOX (cl_host_t *, ch, hi, chg->chg_hosts)
	{
	  if (local_cll.cll_this_host == ch->ch_id)
	    {
	      csl->csl_is_local = 1;
	    }
	}
	END_DO_BOX;
      }
  }
  END_DO_BOX;
  clm->clm_slices = (cl_slice_t **) dk_alloc_box (sizeof (caddr_t) * clm->clm_n_slices, DV_BIN);
  clm->clm_slice_map = (cl_host_group_t **) dk_alloc_box (sizeof (caddr_t) * clm->clm_n_slices, DV_BIN);
  for (inx = 0; inx < clm->clm_n_slices; inx++)
    {
      cl_slice_t *csl = CLM_ID_TO_CSL (clm, inx % clm->clm_distinct_slices);
      clm->clm_slices[inx] = csl;
      clm->clm_slice_map[inx] = csl->csl_chg;
    }
}


void
cluster_schema ()
{
  cl_host_group_t *chg;
  cl_host_t *host;
  cl_host_t **group;
  int inx;
  dk_set_t groups = NULL;
  NEW_VARZ (cluster_map_t, clm);
  clm->clm_name = box_dv_short_string ("__ALL");
  dk_mutex_init (&clm->clm_mtx, MUTEX_TYPE_SHORT);
  mutex_option (&clm->clm_mtx, "clm_slice", NULL, NULL);
  host = local_cll.cll_local;
  chg = dk_alloc (sizeof (cl_host_group_t));
  memset (chg, 0, sizeof (cl_host_group_t));
  group = (cl_host_t **) dk_alloc_box (sizeof (caddr_t), DV_BIN);
  group[0] = host;
  chg->chg_hosts = group;
  chg->chg_clm = clm;
  dk_set_push (&groups, (void *) chg);
  clm->clm_is_modulo = 1;
  clm->clm_hosts = (cl_host_group_t **) list_to_array (dk_set_nreverse (groups));
  dk_set_push (&local_cll.cll_cluster_maps, (void *) clm);
  clm->clm_n_slices = CLM_INITIAL_SLICES;
  clm_all = clm;

  clm_all->clm_is_elastic = 1;
  clm_all->clm_n_replicas = 1;
  clm_all->clm_init_slices = enable_qp;
  clm_all->clm_id_to_slice = hash_table_allocate (37);
  clm_all->clm_host_rank = (ushort *) dk_alloc_box_zero (2 * sizeof (ushort), DV_BIN);
  clm_init_elastic_single (clm_all);
  clm_single_extra_slices (clm_all);
}
