/*
 *  cset.c
 *
 *  $Id$
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
#include "arith.h"
#include "xmltree.h"
#include "rdf_core.h"
#include "http.h"		/* For DKS_ESC_XXX constants */
#include "date.h"		/* For DT_TYPE_DATE and the like */
#include "security.h"		/* For sec_check_dba() */
#include "mhash.h"


#ifdef CSET

dk_hash_t id_to_cset;
dk_hash_t rdfs_type_cset;
dk_hash_t int_seq_to_cset;	/* if iri pattern goes to rdf_n_iri for clustering, map the seq of the num iri string to the cset of the pattern */
dk_hash_t p_to_csetp_list;
rt_range_t rt_ranges[512];
cset_uri_t *cset_uri;
int cset_uri_fill;
extern dk_hash_t rdf_iri_always_cached;
#define SQ_N_BASE 450
square_list_t base_squares[SQ_N_BASE];
id_hash_t *cscl_funcs;
dk_mutex_t cset_p_def_mtx;

void
cscl_define (char *name, cset_cluster_t * func)
{
  id_hash_set (cscl_funcs, (caddr_t) & name, (caddr_t) & func);
}

caddr_t
cset_opt_value (caddr_t * opts, char *opt)
{
  int inx, len = opts ? BOX_ELEMENTS (opts) : 0;
  for (inx = 0; inx < len - 1; inx++)
    {
      if (DV_STRINGP (opts[inx]) && !stricmp (opts[inx], opt))
	return opts[inx + 1];
    }
  return NULL;
}


caddr_t
bif_cset_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  dbe_table_t *text_tb;
  char tmp[400];
  ptrlong id = bif_long_arg (qst, args, 0, "cset_def");
  int range = bif_long_arg (qst, args, 1, "cset_def");
  caddr_t tn = bif_string_arg (qst, args, 2, "cset_def");
  caddr_t rq_tn = bif_string_arg (qst, args, 3, "cset_def");
  ptrlong ir_id = bif_long_arg (qst, args, 4, "cset_def");
  ptrlong bn_ir_id = bif_long_arg (qst, args, 5, "cset_def");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  id_range_t *ir = (id_range_t *) gethash ((void *) ir_id, &id_to_ir);
  id_range_t *bn_ir = (id_range_t *) gethash ((void *) bn_ir_id, &id_to_ir);
  if (!bn_ir)
    sqlr_new_error ("42000", "NOIRN", "No id range object for cset %ld", id);
  if (!cset)
    {
      NEW_VARZ (cset_t, cset2);
      cset = cset2;
      hash_table_init (&cset->cset_p, 53);
      hash_table_init (&cset->cset_except_p, 53);
      cset->cset_except_p.ht_rehash_threshold = 2;
      dk_mutex_init (&cset->cset_mtx, MUTEX_TYPE_SHORT);
    }
  cset->cset_id = id;
  cset->cset_rtr_id = range;
  cset->cset_table = sch_name_to_table (wi_inst.wi_schema, tn);
  cset->cset_rq_table = sch_name_to_table (wi_inst.wi_schema, rq_tn);
  if (!cset->cset_table || !cset->cset_rq_table)
    sqlr_new_error ("42000", "CSETT", "The cset table or rq table does not exist for cset %ld", id);
  cset->cset_table->tb_cset = cset;
  sethash ((void *) (ptrlong) id, &id_to_cset, (void *) cset);
  rt_ranges[id].rtr_cset = cset;
  cset->cset_rtr = &rt_ranges[range];
  snprintf (tmp, sizeof (tmp), "%s_S", cset->cset_table->tb_name);
  cset->cset_rtr->rtr_seq = box_dv_short_string (tmp);
  cset->cset_ir = ir;
  cset->cset_bn_ir = bn_ir;
  text_tb = sch_name_to_table (wi_inst.wi_schema, "DB.DBA.RDF_OBJ_RO_FLAGS_WORDS");
  if (text_tb)
    cset->cset_table->tb_primary_key->key_text_table = text_tb;
  return NULL;
}

int
csetp_compare (const void *s1, const void *s2)
{
  cset_p_t *c1 = *(cset_p_t **) s1;
  cset_p_t *c2 = *(cset_p_t **) s2;
  return c1->csetp_nth < c2->csetp_nth ? -1 : 1;
}


cset_p_t **
cset_p_array (cset_t * cset)
{
  if (!cset->cset_p_array)
    {
      int n = cset->cset_p.ht_count, fill = 0;
      cset_p_t **arr = (cset_p_t **) dk_alloc_box (sizeof (caddr_t) * n, DV_BIN);
      DO_HT (iri_id_t, iri, cset_p_t *, csetp, &cset->cset_p)
      {
	arr[fill++] = csetp;
      }
      END_DO_HT;
      qsort (arr, n, sizeof (caddr_t), csetp_compare);
      cset->cset_p_array = arr;
    }
  return cset->cset_p_array;
}


caddr_t
bif_cset_p_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  dk_set_t lst;
  caddr_t max_inl;
  caddr_t cscl_func = NULL;
  caddr_t cscl_cd = NULL;
  cset_cluster_t *cf = NULL;
  ptrlong id = bif_long_arg (qst, args, 0, "cset_p_def");
  int nth = bif_long_arg (qst, args, 1, "cset_p_def");
  iri_id_t iri = bif_iri_id_arg (qst, args, 2, "cset_p_def");
  caddr_t cn = bif_string_arg (qst, args, 3, "cset_p_def");
  caddr_t *opts = (caddr_t *) bif_arg (qst, args, 4, "cset_p_def");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  dbe_column_t *col;
  int n_slices;
  if (!cset)
    sqlr_new_error ("42000", "NCSET", "No cset %ld", id);
  col = tb_name_to_column (cset->cset_table, cn);
  if (!col)
    sqlr_new_error ("420000", "CSENC", "cset %ld has no col %s", id, cn);
  {
    dk_set_t list;
    NEW_VARZ (cset_p_t, csetp);
    sethash ((void *) iri, &cset->cset_p, (void *) csetp);
    csetp->csetp_iri = iri;
    dk_mutex_init (&csetp->csetp_bloom_mtx, MUTEX_TYPE_SHORT);
    n_slices = CL_RUN_LOCAL == cl_run_local_only ? 1
	: cset->cset_table->tb_primary_key->key_partition->kpd_map->clm_distinct_slices;
    csetp->csetp_bloom = dk_alloc_box_zero (sizeof (caddr_t) * n_slices, DV_BIN);
    csetp->csetp_n_bloom = dk_alloc_box_zero (sizeof (int) * n_slices, DV_BIN);
    lst = (dk_set_t) gethash ((void *) iri, &p_to_csetp_list);
    sethash ((void *) iri, &p_to_csetp_list, CONS (csetp, lst));
    sethash ((void *) iri, &rdf_iri_always_cached, (void *) 1);
    csetp->csetp_col = col;
    col->col_csetp = csetp;
    csetp->csetp_nth = nth;
    csetp->csetp_cset = cset;
    csetp->csetp_index_o = inx_opt_flag (opts, "index");
    max_inl = cset_opt_value (opts, "inline");
    if (max_inl && DV_LONG_INT == DV_TYPE_OF (max_inl))
      {
	int mx = unbox (max_inl);;
	csetp->csetp_inline_max = mx < 0 ? 0 : mx > 4000 ? 4000 : mx;
      }
    cscl_func = cset_opt_value (opts, "cluster");
    cscl_cd = cset_opt_value (opts, "cluster_params");
    if (DV_STRINGP (cscl_func))
      cf = (cset_cluster_t *) id_hash_get (cscl_funcs, (caddr_t) & cscl_func);
    if (cf && cscl_cd)
      {
	csetp->csetp_cluster = *cf;
	csetp->csetp_cluster_cd = box_copy_tree (cscl_cd);
      }
  }
  return NULL;
}


void
csetp_ensure_bloom (cset_p_t * csetp, caddr_t * inst, int n_bloom, uint64 ** bf_ret, uint32 * bf_n_ret)
{
  int prev_sz = 0;
  QNCAST (QI, qi, inst);
  slice_id_t slid = qi->qi_client->cli_slice;
  uint64 **bfs = csetp->csetp_bloom;
  if (QI_NO_SLICE == slid)
    slid = 0;
  if (!bfs || BOX_ELEMENTS (bfs) <= slid)
    {
      mutex_enter (&cset_p_def_mtx);
      bfs = csetp->csetp_bloom;
      if (!bfs || (prev_sz = BOX_ELEMENTS (bfs)) < slid)
	{
	  uint64 **nbfs = (uint64 **) dk_alloc_box_zero (sizeof (caddr_t) * (slid + 100), DV_BIN);
	  uint32 *nbfs_n = (uint32 *) dk_alloc_box_zero (sizeof (uint32) * (slid + 100), DV_BIN);
	  memcpy (nbfs, csetp->csetp_bloom, prev_sz * sizeof (caddr_t));
	  memcpy (nbfs_n, csetp->csetp_n_bloom, prev_sz * sizeof (uint32));
	  csetp->csetp_bloom = nbfs;
	  csetp->csetp_n_bloom = nbfs_n;
	}
      mutex_leave (&cset_p_def_mtx);
    }
  if (n_bloom < 0)
    {
      csetp->csetp_bloom[slid] = dk_alloc (sizeof (uint64));
      memset (csetp->csetp_bloom[slid], 0xff, sizeof (uint64));
      csetp->csetp_n_bloom[slid] = 1;
    }
  else if (!csetp->csetp_n_bloom[slid])
    {
      csetp->csetp_bloom[slid] = dk_alloc (sizeof (uint64) * n_bloom);
      memzero (csetp->csetp_bloom[slid], sizeof (uint64) * n_bloom);
      csetp->csetp_n_bloom[slid] = n_bloom;
    }
  *bf_ret = csetp->csetp_bloom[slid];
  *bf_n_ret = csetp->csetp_n_bloom[slid];
}


void
csetp_bloom (cset_p_t * csetp, caddr_t * inst, uint64 ** bf_ret, uint32 * bf_n_ret)
{
  /* if null is returned this means no bf and no values that could be in one */
  QNCAST (QI, qi, inst);
  slice_id_t slid;
  if (CL_RUN_LOCAL == cl_run_local_only || QI_NO_SLICE == qi->qi_client->cli_slice)
    slid = 0;
  if (BOX_ELEMENTS (csetp->csetp_bloom) <= slid)
    {
      *bf_ret = NULL;
      *bf_n_ret = 0;
    }
  *bf_ret = csetp->csetp_bloom[slid];
  *bf_n_ret = csetp->csetp_n_bloom[slid];
}

extern float chash_bloom_bits;

caddr_t
bif_cset_p_exc (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  /* mark the existence of an exception p, set the size of the bloom filter for s if the p is a column in the cset */
  /* a bf size < 0 means a bf with all 1's where all passes */
  uint64 *bf;
  uint32 n_bf;
  int prev_sz = 0;
  QNCAST (QI, qi, qst);
  slice_id_t slid = qi->qi_client->cli_slice;
  uint64 **bfs;
  ptrlong id = bif_long_arg (qst, args, 0, "cset_p_exc");
  iri_id_t iri = bif_iri_id_arg (qst, args, 1, "cset_p_def");
  int n_bloom = bif_long_arg (qst, args, 2, "cset_p_exc");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  cset_p_t *csetp;
  if (!cset)
    sqlr_new_error ("42000", "NCSET", "No cset %ld", id);
  csetp = gethash ((void *) iri, &cset->cset_p);
  if (!csetp)
    {
      sethash ((void *) iri, &cset->cset_except_p, (void *) 1);
      return NULL;
    }
  if (QI_NO_SLICE == slid)
    slid = 0;
  n_bloom = _RNDUP_PWR2 (((uint64) (_RNDUP_PWR2 (n_bloom + 1, 32) * chash_bloom_bits / 8)), 64);
  csetp_ensure_bloom (csetp, qst, n_bloom, &bf, &n_bf);
  return (caddr_t) 1;
}


caddr_t
bif_cset_add_exc (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  sqlr_new_error ("CSEXC", "CSEXC", "cset_add_exc only vectored");
  return NULL;
}


void
bif_cset_add_exc_vec (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args, state_slot_t * ret)
{
  uint64 *bf;
  uint32 n_bf;
  int inx;
  QNCAST (QI, qi, qst);
  data_col_t *ret_dc = NULL;
  slice_id_t slid = qi->qi_client->cli_slice;
  uint64 **bfs;
  ptrlong id = bif_long_arg (qst, args, 0, "cset_add_exc");
  iri_id_t iri = bif_iri_id_arg (qst, args, 1, "cset_add_exc");
  data_col_t *s_dc;
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  cset_p_t *csetp;
  if (ret && SSL_VEC == ret->ssl_type)
    {
      ret_dc = QST_BOX (data_col_t *, qst, ret->ssl_index);
      DC_CHECK_LEN (ret_dc, qi->qi_n_sets - 1);
      if (DV_LONG_INT == ret_dc->dc_dtp)
	memzero (ret_dc->dc_values, qi->qi_n_sets * sizeof (int64));
    }
  if (!cset)
    return;
  csetp = gethash ((void *) iri, &cset->cset_p);
  if (!csetp)
    return;
  if (BOX_ELEMENTS (args) < 3 || SSL_VEC != args[2]->ssl_type)
    sqlr_new_error ("CSEXC", "CSEXC", "");
  s_dc = QST_BOX (data_col_t *, qst, args[2]->ssl_index);
  if (QI_NO_SLICE == slid)
    slid = 0;
  csetp_bloom (csetp, qst, &bf, &n_bf);
  if (!bf)
    return;
  for (inx = 0; inx < s_dc->dc_n_values; inx++)
    {
      iri_id_t s = ((iri_id_t *) s_dc->dc_values)[inx];
      uint64 h = 1;
      uint32 w;
      uint64 mask;
      MHASH_STEP_1 (h, s);
      w = BF_WORD (h, n_bf);
      mask = BF_MASK (h);
      bf[w] |= mask;
    }
}



caddr_t
bif_cset_p_no_index (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  /* declare that in a given cset a given o value for a p has no posg even if other values do */
  caddr_t err = NULL;
  ptrlong id = bif_long_arg (qst, args, 0, "cset_p_no_index");
  iri_id_t iri = bif_iri_id_arg (qst, args, 1, "cset_p_no_index");
  caddr_t exc = bif_arg (qst, args, 2, "cset_p_no_index");
  caddr_t any;
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  cset_p_t *csetp;
  if (!cset)
    sqlr_new_error ("42000", "NCSET", "No cset %ld", id);
  csetp = gethash ((void *) iri, &cset->cset_p);
  if (!csetp)
    return NULL;
  if (!csetp->csetp_non_index_o)
    {
      csetp->csetp_non_index_o = id_hash_allocate (101, sizeof (caddr_t), 0, treehash, treehashcmp);
      csetp->csetp_non_index_o->ht_rehash_threshold = 200;
    }
  caddr_t cp = box_copy_tree (exc);
  any = box_to_any (exc, &err);
  id_hash_set (csetp->csetp_non_index_o, (caddr_t) & any, NULL);
  id_hash_set (csetp->csetp_non_index_o, (caddr_t) & cp, NULL);
  return NULL;
}

caddr_t
bif_cset_type_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  ptrlong id = bif_long_arg (qst, args, 0, "cset_type_def");
  iri_id_t type = bif_iri_id_arg (qst, args, 1, "cset_type_def");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  sethash ((void *) type, &rdfs_type_cset, cset);
  return NULL;
}

caddr_t
bif_cset_uri_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  search_spec_t sp;
  state_slot_t ssl;
  ptrlong id = bif_long_arg (qst, args, 0, "cset_uri_def");
  caddr_t str = bif_string_arg (qst, args, 1, "cset_uri_def");
  int len = cset_uri ? box_length (cset_uri) / sizeof (cset_uri_t) : 0;
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  memzero (&sp, sizeof (sp));
  memzero (&ssl, sizeof (ssl));
  if (len <= cset_uri_fill)
    {
      cset_uri_t *nxt = (cset_uri_t *) dk_alloc_box ((cset_uri_fill + 500) * sizeof (cset_uri_t), DV_BIN);
      memcpy (nxt, cset_uri, sizeof (cset_uri_t) * cset_uri_fill);
      cset_uri = nxt;
    }
  ssl.ssl_type = SSL_CONSTANT;
  ssl.ssl_constant = str;
  sp.sp_min_ssl = &ssl;
  sp.sp_min_op = CMP_LIKE;
  sp_set_like_flags (&sp);
  cset_uri[cset_uri_fill].csu_substr = box_copy (str);
  cset_uri[cset_uri_fill].csu_sp = sp;
  cset_uri[cset_uri_fill].csu_cset = cset;
  cset_uri_fill++;
  return NULL;
}

#define SQ_INIT_SZ 32

double
coord_normalize (double coord, double range)
{
  int n;
  if (coord >= -range && coord < range)
    return coord;
  coord += range;
  n = floor (coord / (2 * range));
  coord -= range * n;
  return coord - range;
}


caddr_t
bif_cset_sq_clr (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  int inx;
  for (inx = 0; inx < SQ_N_BASE; inx++)
    {
      dk_free_box (base_squares[inx].sqls_sqs);
      base_squares[inx].sqls_sqs = NULL;
      base_squares[inx].sqls_fill = 0;
    }
  return NULL;
}

#define XY_PLACE(x, rng) ((coord_normalize (x, rng)+rng)/12)
#define XY_NTH(x,y) (((int)XY_PLACE((x), 180) * 15) + (int)XY_PLACE((y), 90))

caddr_t
bif_cset_sq_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  geo_t *g = bif_geo_arg (qst, args, 0, "cset_sq_def", GEO_ARG_ANY_NONNULL);
  ptrlong id = bif_long_arg (qst, args, 1, "cset_sq_def");
#if 0
  ptrlong s_n_id = bif_long_arg (qst, args, 2, "cset_sq_def");
  ptrlong s_w_id = bif_long_arg (qst, args, 3, "cset_sq_def");
#endif
  square_t *sqs;
  int nth = -1;
  square_t sq;
  double X = (g->XYbox.Xmax + g->XYbox.Xmin) / 2.0;
  double Y = (g->XYbox.Ymax + g->XYbox.Ymin) / 2.0;
  sq.sq_id = id;
#if 0
  sq.sq_s_id = s_n_id;
  sq.sq_s_w_id = s_w_id;
#endif
  sq.sq_box = g->XYbox;
  nth = (int) XY_NTH (X, Y);
  if (nth < 0 || nth >= SQ_N_BASE)
    sqlr_new_error ("22023", "CSETQ", "Can not find base square place");
  sqs = (base_squares[nth].sqls_sqs);
  if (!sqs)
    {
      sqs = dk_alloc_box (SQ_INIT_SZ * sizeof (square_t), DV_BIN);
      base_squares[nth].sqls_sqs = sqs;
      base_squares[nth].sqls_size = SQ_INIT_SZ;
      base_squares[nth].sqls_fill = 0;
    }
  else if (base_squares[nth].sqls_fill >= base_squares[nth].sqls_size)
    {
      caddr_t new;
      base_squares[nth].sqls_size *= 2;
      new = dk_alloc_box (base_squares[nth].sqls_size * sizeof (square_t), DV_BIN);
      memcpy (new, sqs, base_squares[nth].sqls_fill * sizeof (square_t));
      dk_free_box (sqs);
      base_squares[nth].sqls_sqs = sqs = (square_t *) new;
    }
  sqs[base_squares[nth].sqls_fill++] = sq;
  return NULL;
}

cset_t *
iri_cset_by_name (caddr_t str)
{
  int inx;
  dtp_t dtp = DV_TYPE_OF (str);
  if (DV_STRING != dtp)
    return NULL;
  for (inx = 0; inx < cset_uri_fill; inx++)
    {
      cset_uri_t *csu = &cset_uri[inx];
      if (strstr_sse42 (str, box_length (str) - 1, csu->csu_substr, box_length (csu->csu_substr) - 1))
	{
	  box_flags (str) = 4 * csu->csu_cset->cset_rtr_id;
	  return csu->csu_cset;
	}
    }
  return NULL;
}

square_t *
cset_set_sq_id (caddr_t box)
{
  square_t *sq = NULL;
  rdf_box_t *o_val = ((rdf_box_t *) (box));
  geo_t *g;
  int inx, nth = -1;
  int id = 0;
  double X, Y;

  if (DV_TYPE_OF (box) != DV_RDF || ((rdf_box_t *) (box))->rb_type != RDF_BOX_GEO)
    return NULL;
  g = (geo_t *) (o_val->rb_box);
  X = (g->XYbox.Xmax + g->XYbox.Xmin) / 2.0;
  Y = (g->XYbox.Ymax + g->XYbox.Ymin) / 2.0;
  nth = (int) XY_NTH (X, Y);
  /* look for smallest containing square */
  for (inx = 0; nth >= 0 && inx < base_squares[nth].sqls_fill; inx++)
    {
      geo_XYbox_t p = base_squares[nth].sqls_sqs[inx].sq_box;
      if (X >= p.Xmin && X < p.Xmax && Y >= p.Ymin && Y < p.Ymax)
	{
	  sq = &base_squares[nth].sqls_sqs[inx];
	  id = sq->sq_id;
	}
    }
  if (id > 0)
    box_flags (o_val->rb_box) = id;
  return sq;
}


void
dc_set_box_flags (data_col_t * dc, int inx, uint32 flags)
{
  /* the dc is boxes or anies.  Set the box flags */
  if (DV_ANY == dc->dc_sqt.sqt_dtp)
    {
      db_buf_t dv = ((db_buf_t *) dc->dc_values)[inx];
      if (DV_BOX_FLAGS == *dv)
	LONG_SET_NA (dv + 1, flags);
      else
	{
	  int len;
	  DB_BUF_TLEN (len, dv[0], dv);
	  int save = dc->dc_n_values;
	  dtp_t tmp[5];
	  tmp[0] = DV_BOX_FLAGS;
	  LONG_SET_NA (&tmp[1], flags);
	  dc->dc_n_values = inx;
	  dc_append_bytes (dc, dv, len, tmp, 5);
	  dc->dc_n_values = save;
	}
    }
}


uint64
cu_ro_slice (cucurbit_t * cu, cu_line_t * o_cul, cu_func_t * o_cf, value_state_t ** vs_ret, caddr_t o)
{
  /* get the vs for the o. return vs to permit affecting the params.  */
  value_state_t **place = (value_state_t **) id_hash_get (o_cul->cul_values, (caddr_t) & o);
  *vs_ret = NULL;
  if (place)
    {
      static cu_func_t *o_look_cf;
      int32 rem = 0;
      data_col_t *dc;
      uint32 hash;
      value_state_t *vs = *place;
      key_partition_def_t *kpd;
      cluster_map_t *clm;
      cl_slice_t *csl;
      *vs_ret = vs;
      if (!vs->vs_call_clo)
	return -1;
      if (!o_look_cf)
	o_look_cf = cu_func ("O_LOOK1", 1);
      dc = (data_col_t *) vs->vs_call_clo->_.call.params[0];
      kpd = o_look_cf->cf_part_key->key_partition;
      clm = kpd->kpd_map;
      hash = dc_part_hash (kpd->kpd_cols[0], dc, vs->vs_dc_inx, &rem);
      csl = clm->clm_slices[hash % clm->clm_n_slices];

      return ((uint64) csl->csl_id);
    }
  return -1;
}

id_range_t *geocl_o_seq;	/* the seq no of the top level seq for geo clustering of o's */

void
cscl_geo (void *cup, cset_t * cset, cset_p_t * csetp, caddr_t * row, int s_col, int p_col, int o_col, int g_col, caddr_t cd)
{
  value_state_t **place;
  uint32 flags;
  cu_line_t *o_cul;
  cu_line_t *s_cul;
  id_range_t *subseq;
  QNCAST (cucurbit_t, cu, cup);
  square_t *sq = cset_set_sq_id (row[o_col + 1]);
  if (sq)
    {
      caddr_t s = row[s_col];
      dtp_t s_dtp = DV_TYPE_OF (s);
      uint64 o_slice = 0;
      value_state_t *vs;
      static cu_func_t *o_cf;
      static cu_func_t *s_cf;
      if (!s_cf)
	{
	  if (CL_RUN_LOCAL == cl_run_local_only)
	    {
	      o_cf = cu_func ("L_MAKE_RO", 1);
	      s_cf = cu_func ("L_IRI_TO_ID", 1);
	    }
	  else
	    {
	      o_cf = cu_func ("MAKE_RO_1", 1);
	      s_cf = cu_func ("IRI_TO_ID_1", 1);
	    }
	}
      flags = sq->sq_id;
      o_cul = cu_line (cu, o_cf);
      o_slice = cu_ro_slice (cu, o_cul, o_cf, &vs, row[o_col + 1]);
      if (o_slice != -1)
	{
	  if (!geocl_o_seq)
	    geocl_o_seq = ir_by_name ("geo_o_init");
	  subseq = ir_sub_ir (geocl_o_seq, sq->sq_id);
	  flags = subseq->ir_id | (1L << (IR_SLICE_SHIFT - 1)) | (o_slice << IR_SLICE_SHIFT);
	  dc_set_box_flags ((data_col_t *) vs->vs_call_clo->_.call.params[0], vs->vs_dc_inx, flags);
	  if (DV_STRING == s_dtp)
	    {
	      s_cul = cu_line (cu, s_cf);
	      place = (value_state_t **) id_hash_get (s_cul->cul_values, (caddr_t) & row[s_col]);
	      if (place)
		{
		  vs = *place;
		  subseq = ir_sub_ir (cset->cset_ir, sq->sq_id);
		  flags = (box_flags (s) & BF_N_IRI) | subseq->ir_id | (1L << (IR_SLICE_SHIFT - 1)) | (o_slice << IR_SLICE_SHIFT);
		  dc_set_box_flags ((data_col_t *) vs->vs_call_clo->_.call.params[0], vs->vs_dc_inx, flags);
		}
	    }
	}
    }
}


void
cset_ld_ids (cl_req_group_t * clrg, int s_col, int p_col, int o_col, int g_col, iri_id_t fixed_g)
{
  /* clrg contains rows of psog.  For unresolved iris, set box flags to be preferred cset */
  int inx;
  cucurbit_t *cu = clrg->clrg_cu;
  for (inx = 0; inx < cu->cu_fill; inx++)
    {
      caddr_t *row = (caddr_t *) cu->cu_rows[inx];
      caddr_t p = row[p_col];
      dtp_t p_dtp = DV_TYPE_OF (p);
      caddr_t s = row[s_col];
      dtp_t s_dtp = DV_TYPE_OF (s);
      uint32 flags;
      iri_cset_by_name (row[s_col]);
      iri_cset_by_name (p);
      iri_cset_by_name (row[o_col]);
      if (-1 != g_col)
	iri_cset_by_name (row[g_col]);
      p = row[p_col];
      p_dtp = DV_TYPE_OF (p);
      s = row[s_col];
      s_dtp = DV_TYPE_OF (s);
      if (DV_IRI_ID == p_dtp && DV_STRING == s_dtp && (flags = box_flags (s)) > BF_UTF8)
	{
	  cset_t *cset;
	  flags &= ~BF_N_IRI;
	  cset = (cset_t *) gethash ((void *) (ptrlong) flags, &int_seq_to_cset);
	  if (cset)
	    {
	      iri_id_t p_id = unbox_iri_id (p);
	      cset_p_t *csetp = (cset_p_t *) gethash ((void *) p_id, &cset->cset_p);
	      if (csetp && csetp->csetp_cluster)
		csetp->csetp_cluster ((void *) cu, cset, csetp, row, s_col, p_col, o_col, g_col, csetp->csetp_cluster_cd);
	    }
	}
    }
}

void
csi_init (cset_ins_t * csi, cset_t * cset, int n_places)
{
  mem_pool_t *mp = THR_TMP_POOL;
  static state_slot_t iri_tmp;
  static state_slot_t any_tmp;
  int n_cols = cset->cset_p.ht_count + 2;
  if (!cset->cset_ins || !cset->cset_del)
    {
      caddr_t err = NULL;
      WITHOUT_TMP_POOL
      {
	cset->cset_ins = cset_ins_qr (cset, &err);
	if (err)
	  sqlr_resignal (err);
	cset->cset_del = cset_del_qr (cset, &err);
	if (err)
	  sqlr_resignal (err);
      }
      END_WITHOUT_TMP_POOL;
    }
  if (!iri_tmp.ssl_sqt.sqt_dtp)
    {
      iri_tmp.ssl_sqt.sqt_dtp = DV_IRI_ID;
      any_tmp.ssl_sqt.sqt_dtp = DV_ANY;
    }
  csi->csi_cset = cset;
  csi->csi_dcs = (data_col_t **) t_alloc_box (n_cols * sizeof (caddr_t), DV_BIN);
  csi->csi_dcs[0] = mp_data_col (mp, &iri_tmp, n_places);
  csi->csi_dcs[1] = mp_data_col (mp, &iri_tmp, n_places);
  DO_HT (iri_id_t, p_iri, cset_p_t *, csetp, &cset->cset_p)
  {
    csi->csi_dcs[csetp->csetp_nth + 2] = mp_data_col (mp, &any_tmp, n_places);
  }
  END_DO_HT;
}


void
csi_process (cset_ins_t * csi, query_instance_t * qi, int is_del)
{
  caddr_t bk = box_copy ((caddr_t) csi->csi_dcs);
  int inx;
  caddr_t err = qr_exec (qi->qi_client, csi->csi_cset->cset_ins, CALLER_LOCAL, "", NULL, NULL, (caddr_t *) bk, NULL, 0);
  dk_free_box (bk);
  if (err)
    sqlr_resignal (err);
  DO_BOX (data_col_t *, dc, inx, csi->csi_dcs) dc_reset (dc);
  END_DO_BOX;
}


int
sg_hash (sg_key_t * k)
{
  int64 h = 1;
  MHASH_STEP_1 (h, k->s);
  MHASH_STEP (h, k->g);
  return h;
}


int
sg_cmp (sg_key_t * x1, sg_key_t * x2)
{
  return x1->g == x2->g && x1->s == x2->s;
}


void
csi_add_o (cset_p_t * csetp, data_col_t * dc, int r, data_col_t * o, int inx)
{
  db_buf_t dv = ((db_buf_t *) o->dc_values)[inx];
  if (DV_RDF == dv[0])
    {
      if (csetp->csetp_inline_max)
	{
	  int l = rbs_length (dv);
	  if (l < csetp->csetp_inline_max + 12)
	    {
	      ((db_buf_t *) dc->dc_values)[r] = dv;
	      goto set_len;
	    }
	}
      {
	int l;
	dtp_t temp[10];
	int64 id = rbs_ro_id (dv);
	int save = dc->dc_n_values;
	if (id > INT32_MAX || id < INT32_MIN)
	  {
	    temp[0] = DV_RDF_ID_8;
	    INT64_SET_NA (&temp[1], id);
	    l = 9;
	  }
	else
	  {
	    temp[0] = DV_RDF_ID;
	    LONG_SET_NA (&temp[1], id);
	    l = 5;
	  }
	dc->dc_n_values = r;
	dc_append_bytes (dc, temp, l, NULL, 0);
	dc->dc_n_values = MAX (r + 1, save);
      }
    }
  else
    ((db_buf_t *) dc->dc_values)[r] = dv;
set_len:
  if (r >= dc->dc_n_values)
    dc->dc_n_values = r + 1;
}


void
csi_new_row (cset_ins_t * csi, cset_p_t * csetp, sg_key_t * k, iri_id_t pi, data_col_t * o, int nth_row)
{
  cset_t *cset = csi->csi_cset;
  static dtp_t dv_null = DV_DB_NULL;
  int64 r = csi->csi_dcs[0]->dc_n_values;
  int inx;
  t_id_hash_set (csi->csi_sg_row, (caddr_t) k, (caddr_t) & r);
  dc_append_int64 (csi->csi_dcs[0], k->s);
  dc_append_int64 (csi->csi_dcs[1], k->g);
  for (inx = 0; inx < cset->cset_p.ht_count; inx++)
    {
      data_col_t *dc = csi->csi_dcs[inx + 2];
      DC_CHECK_LEN (dc, dc->dc_n_values);
      if (inx == csetp->csetp_nth)
	{
	  csi_add_o (csetp, dc, dc->dc_n_values, o, nth_row);
	}
      else
	((db_buf_t *) dc->dc_values)[dc->dc_n_values++] = &dv_null;
    }
}


void
box_add_dv (caddr_t * box_ret, db_buf_t new_dv)
{
  db_buf_t box = *(db_buf_t *) box_ret;
  int max_len = box_length (box);
  dtp_t ign;
  int i, cnt = dv_int (box + 1, &ign), new_len;
  db_buf_t dv = DV_SHORT_INT == box[1] ? box + 2 : box + 5;
  DB_BUF_TLEN (new_len, new_dv[0], new_dv);
  for (i = 0; i < cnt; i++)
    {
      int len;
      DB_BUF_TLEN (len, *dv, dv);
      dv += len;
    }
  if (new_len + (dv - box) >= max_len)
    {
      caddr_t new_box = dk_alloc_box (max_len + new_len + 3000, DV_BIN);
      memcpy (new_box, box, box_length (box));
      dk_free_box (box);
      *box_ret = box = new_box;
      dv = new_box + (dv - box);
    }
  memcpy (dv, new_dv, new_len);
  if (DV_SHORT_INT == (dtp_t) box[1])
    {
      if (255 == (dtp_t) box[2])
	{
	  memmove (box + 6, box + 3, box_length (box) - 3);
	  box[1] = DV_LONG_INT;
	  LONG_SET_NA (box + 2, 256);
	}
      else
	box[2]++;
    }
  else
    {
      int n2 = LONG_REF_NA (box + 2) + 1;
      LONG_SET_NA (box + 2, n2);
    }
}


void
csi_temp_free (dk_hash_t * ht)
{
  DO_HT (caddr_t, box, ptrlong, ign, ht) dk_free_box (box);
  END_DO_HT;
  hash_table_free (ht);
}


void
csi_del_add (data_col_t * dc, int r, data_col_t * o, int o_row, dk_hash_t ** csi_temp_ret)
{
  /* the dc has a value, add the value in o[row] */
  caddr_t box = NULL;
  dk_hash_t *ht;
  int prev_len, new_len;
  db_buf_t prev_dv = ((db_buf_t *) dc->dc_values)[r];
  db_buf_t new_dv = ((db_buf_t *) o->dc_values)[o_row];
  DB_BUF_TLEN (prev_len, *prev_dv, prev_dv);
  DB_BUF_TLEN (new_len, *new_dv, new_dv);
  ht = *csi_temp_ret;
  if (!ht)
    {
      *csi_temp_ret = ht = hash_table_allocate (301);
    }
  if (!gethash ((void *) prev_dv, ht))
    {
      box = dk_alloc_box (prev_len + new_len + 500, DV_BIN);
      sethash ((void *) box, ht, (void *) 1);
      ((caddr_t *) dc->dc_values)[r] = box;
    }
  if (DV_ARRAY_OF_POINTER == prev_dv[0])
    {
      caddr_t prev_box = box;
      box_add_dv (&box, new_dv);
      if (prev_box != box)
	sethash ((void *) box, ht, (void *) 1);
    }
  else
    {
      box[0] = DV_ARRAY_OF_POINTER;
      box[1] = DV_SHORT_INT;
      box[2] = 2;
      memcpy (box + 3, prev_dv, prev_len);
      memcpy (box + 3 + prev_len, new_dv, new_len);
    }
}


int rq_labels_inited = 0;

int
cset_ld_indices (cl_req_group_t * clrg, data_col_t * s, data_col_t * p, data_col_t * o, data_col_t * g, int is_del)
{
  /* iris are resolved, make inserts according to csets */
  dk_hash_t *csi_temp = NULL;
  int any = 0;
  int64 *place;
  db_buf_t posg_bits, psog_bits, gs_bits, op_bits, sp_bits;
  iri_id_t si;
  int inx;
  cucurbit_t *cu = clrg->clrg_cu;
  int fill = s->dc_n_values;
  SET_THR_TMP_POOL (clrg->clrg_pool);
  if (!rq_labels_inited)
    {
      dbe_table_t *quad_tb = sch_name_to_table (wi_inst.wi_schema, "DB.DBA.RDF_QUAD");
      rdf_quad_key_labels (quad_tb);
      rq_labels_inited = 1;
    }
  if (!cu->cu_inx_bits || box_length (cu->cu_inx_bits[0]) < 10 + (cu->cu_fill / 8))
    {
      int n_bytes = ALIGN_8 (cu->cu_fill + 10000) / 8;
      cu->cu_inx_bits = (db_buf_t *) t_alloc_box (sizeof (caddr_t) * 5, DV_BIN);
      for (inx = 0; inx < 5; inx++)
	cu->cu_inx_bits[inx] = (db_buf_t) t_alloc_box (n_bytes, DV_BIN);
    }
  for (inx = 0; inx < 5; inx++)
    memzero (cu->cu_inx_bits[inx], ALIGN_8 (cu->cu_fill) / 8);
  psog_bits = cu->cu_inx_bits[I_PSOG];
  posg_bits = cu->cu_inx_bits[I_POSG];
  op_bits = cu->cu_inx_bits[I_OP];
  sp_bits = cu->cu_inx_bits[I_SP];
  gs_bits = cu->cu_inx_bits[I_GS];
  for (inx = 0; inx < fill; inx++)
    {
      int r;
      si = ((iri_id_t *) s->dc_values)[inx];
      r = IRI_RANGE (si);
      if (!r)
	{
	quad:
	  BIT_SET (posg_bits, inx);
	  BIT_SET (psog_bits, inx);
	  BIT_SET (sp_bits, inx);
	  BIT_SET (op_bits, inx);
	  BIT_SET (gs_bits, inx);
	}
      else
	{
	  cset_ins_t *csi = NULL;
	  cset_t *cset = rt_ranges[r].rtr_cset;
	  iri_id_t pi = ((iri_id_t *) p->dc_values)[inx];
	  sg_key_t k;
	  int64 tl;
	  cset_p_t *cs_p = (cset_p_t *) gethash ((void *) pi, &cset->cset_p);
	  any = 1;
	  if (!cs_p)
	    goto quad;
	  k.s = si;
	  k.g = ((iri_id_t *) g->dc_values)[inx];
	  if (!cu->cu_csi)
	    cu->cu_csi = hash_table_allocate (23);
	  csi = gethash ((void *) cset, cu->cu_csi);
	  if (!csi)
	    {
	      t_NEW_VARZ (cset_ins_t, csi2);
	      csi = csi2;
	      sethash ((void *) cset, cu->cu_csi, (void *) csi);
	      csi->csi_sg_row =
		  t_id_hash_allocate (cu->cu_fill / 2, 2 * sizeof (sg_key_t), sizeof (int64), (hash_func_t) sg_hash,
		  (cmp_func_t) sg_cmp);
	      tl = inx;
	      csi_init (csi, cset, 1 + (cu->cu_fill / cset->cset_p.ht_count));
	    }
	  place = (int64 *) id_hash_get (csi->csi_sg_row, (caddr_t) & k);
	  if (!place)
	    {
	      csi_new_row (csi, cs_p, &k, pi, o, inx);
	      BIT_SET (gs_bits, inx);
	    }
	  else
	    {
	      int r = *place;
	      data_col_t *dc = csi->csi_dcs[cs_p->csetp_nth + 2];
	      int n = dc->dc_n_values;
	      if (r >= n)
		GPF_T1 ("cset fill, row above fill");
	      if (DV_DB_NULL != ((db_buf_t *) dc->dc_values)[r][0])
		{
		  if (is_del)
		    csi_del_add (dc, r, o, inx, &csi_temp);
		  else
		    goto quad;
		}
	      DC_CHECK_LEN (dc, r);
	      csi_add_o (cs_p, dc, r, o, inx);
	      if (cs_p->csetp_index_o)
		{
		  BIT_SET (posg_bits, inx);
		  BIT_SET (op_bits, inx);
		}
	    }
	}
    }
  SET_THR_TMP_POOL (NULL);
  if (!cu->cu_csi || !cu->cu_csi->ht_count)
    return any;
  QR_RESET_CTX
  {
    DO_HT (ptrlong, id, cset_ins_t *, csi, cu->cu_csi)
    {
      if (csi->csi_dcs[0]->dc_n_values)
	csi_process (csi, (QI *) clrg->clrg_inst, is_del);
    }
    END_DO_HT;
  }
  QR_RESET_CODE
  {
    POP_QR_RESET;
    csi_temp_free (csi_temp);
    longjmp_splice (__self->thr_reset_ctx, reset_code);
  }
  END_QR_RESET;
  return any;
}


void
rdf_quad_key_labels (dbe_table_t * tb)
{
  DO_SET (dbe_key_t *, key, &tb->tb_keys)
  {
    int inx;
    char name[20];
    for (inx = 0; inx < MIN (4, key->key_n_significant); inx++)
      {
	dbe_column_t *col = (dbe_column_t *) dk_set_nth (key->key_parts, inx);
	name[inx] = col->col_name[0];
      }
    name[inx] = 0;
    if (0 == stricmp (name, "psog"))
      key->key_rdf_order = I_PSOG;
    else if (0 == stricmp (name, "posg"))
      key->key_rdf_order = I_POSG;
    else if (0 == stricmp (name, "op"))
      key->key_rdf_order = I_OP;
    else if (0 == stricmp (name, "sp"))
      key->key_rdf_order = I_SP;
    else if (0 == stricmp (name, "gs"))
      key->key_rdf_order = I_GS;
    else
      key->key_rdf_order = I_OTHER;
  }
  END_DO_SET ();
}


void csg_init ();

void
cset_init ()
{
  int save;
  csg_init ();
  cscl_funcs = id_str_hash_create (11);
  hash_table_init (&p_to_csetp_list, 211);
  hash_table_init (&id_to_cset, 223);
  hash_table_init (&rdfs_type_cset, 223);
  hash_table_init (&int_seq_to_cset, 51);
  bif_define ("cset_def", bif_cset_def);
  bif_define ("cset_p_def", bif_cset_p_def);
  bif_define ("cset_p_exc", bif_cset_p_exc);
  bif_define_ex ("cset_add_exc", bif_cset_add_exc, BMD_VECTOR_IMPL, bif_cset_add_exc_vec, BMD_RET_TYPE, &bt_integer, BMD_DONE);
  bif_define ("cset_p_no_index", bif_cset_p_no_index);
  bif_define ("cset_type_def", bif_cset_type_def);
  bif_define ("cset_uri_def", bif_cset_uri_def);
  bif_define ("cset_sq_def", bif_cset_sq_def);
  bif_define ("cset_sq_clear", bif_cset_sq_clr);
  save = enable_qp;
  enable_qp = 1;
  cscl_define ("cscl_geo", cscl_geo);
  ddl_ensure_table ("do this always",
      "select count (cset_def (cset_id, cset_range, cset_table, cset_rq_table, cset_id_range, cset_bn_id_range)) from rdf_cset");
  ddl_ensure_table ("do this always",
      "select count (cset_p_def (csetp_cset, csetp_nth, csetp_iid, csetp_col, csetp_options)) from rdf_cset_p");
  ddl_ensure_table ("do this always", "select count (cset_type_def (cst_cset, cst_type)) from rdf_cset_type");
  cset_uri = NULL;
  ddl_ensure_table ("do this always", "select count (cset_uri_def (csu_cset, csu_pattern)) from rdf_cset_uri");
  ddl_ensure_table ("do this always",
      "select count (iri_pattern_def (rip_pattern, rip_start, rip_fields, rip_cset, rip_int_range, rip_exc_range))  from rdf_iri_pattern");
  ddl_ensure_table ("do this always", "iri_pattern_changed ()");
  enable_qp = save;
  dk_mutex_init (&cset_p_def_mtx, MUTEX_TYPE_SHORT);
  {
  }
  {
    dbe_table_t *text_tb;
    char tmp[400];
    NEW_VARZ (cset_t, cset);
    hash_table_init (&cset->cset_p, 53);
    hash_table_init (&cset->cset_except_p, 53);
    cset->cset_except_p.ht_rehash_threshold = 2;
    cset->cset_id = 0;
    cset->cset_rtr_id = 0;
    cset->cset_rq_table = sch_name_to_table (wi_inst.wi_schema, "DB.DBA.RDF_QUAD");
    sethash ((void *) (ptrlong) 0, &id_to_cset, (void *) cset);
    rt_ranges[0].rtr_cset = cset;
    cset->cset_rtr = &rt_ranges[0];
  }
  ddl_ensure_table ("do this always", "cset_bf_init ()");
  ddl_ensure_table ("do this always", "select count (cset_p_no_index (csni_cset, csni_iid, csni_o)) from RDF_CSET_P_NI ");
}

#else

void
cset_init ()
{
}

#endif
