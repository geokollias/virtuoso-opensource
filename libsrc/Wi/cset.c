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
dk_hash_t p_to_csetp_list;
rt_range_t rt_ranges[512];
cset_uri_t *cset_uri;
int cset_uri_fill;
extern dk_hash_t rdf_iri_always_cached;


caddr_t
bif_cset_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  char tmp[400];
  ptrlong id = bif_long_arg (qst, args, 0, "cset_def");
  int range = bif_long_arg (qst, args, 1, "cset_def");
  caddr_t tn = bif_string_arg (qst, args, 2, "cset_def");
  caddr_t rq_tn = bif_string_arg (qst, args, 3, "cset_def");
  ptrlong ir_id = bif_long_arg (qst, args, 4, "cset_def");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  id_range_t *ir = (id_range_t *) gethash ((void *) ir_id, &id_to_ir);
  if (!ir)
    sqlr_new_error ("42000", "NOIRN", "No id range object for cset %ld", id);
  if (!cset)
    {
      NEW_VARZ (cset_t, cset2);
      cset = cset2;
      hash_table_init (&cset->cset_p, 53);
      hash_table_init (&cset->cset_except_p, 53);
      cset->cset_except_p.ht_rehash_threshold = 2;
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
  return NULL;
}


caddr_t
bif_cset_p_def (caddr_t * qst, caddr_t * err_ret, state_slot_t ** args)
{
  ptrlong id = bif_long_arg (qst, args, 0, "cset_p_def");
  int nth = bif_long_arg (qst, args, 1, "cset_p_def");
  iri_id_t iri = bif_iri_id_arg (qst, args, 2, "cset_p_def");
  caddr_t cn = bif_string_arg (qst, args, 3, "cset_p_def");
  caddr_t opts = bif_arg (qst, args, 4, "cset_p_def");
  cset_t *cset = (cset_t *) gethash ((void *) id, &id_to_cset);
  dbe_column_t *col;
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
    list = (dk_set_t) gethash ((void *) iri, &p_to_csetp_list);
    sethash ((void *) iri, &p_to_csetp_list, CONS (csetp, list));
    sethash ((void *) iri, &rdf_iri_always_cached, (void *) 1);
    csetp->csetp_col = col;
    col->col_csetp = csetp;
    csetp->csetp_nth = nth;
    csetp->csetp_cset = cset;
    csetp->csetp_index_o = inx_opt_flag ((caddr_t *) opts, "index");
  }
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

void
cset_ld_ids (cl_req_group_t * clrg, int s_col, int p_col, int o_col, int g_col, iri_id_t fixed_g)
{
  /* clrg contains rows of psog.  For unresolved iris, set box flags to be preferred cset */
  int inx;
  cucurbit_t *cu = clrg->clrg_cu;
  for (inx = 0; inx < cu->cu_fill; inx++)
    {
      caddr_t *row = (caddr_t *) cu->cu_rows[inx];
      iri_cset_by_name (row[s_col]);
      iri_cset_by_name (row[p_col]);
      iri_cset_by_name (row[o_col]);
      if (-1 != g_col)
	iri_cset_by_name (row[g_col]);
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
      cset->cset_ins = cset_ins_qr (cset, &err);
      if (err)
	sqlr_resignal (err);
      cset->cset_del = cset_del_qr (cset, &err);
      if (err)
	sqlr_resignal (err);
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
	((db_buf_t *) dc->dc_values)[dc->dc_n_values++] = ((db_buf_t *) o->dc_values)[nth_row];
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
  if (!cu->cu_inx_bits || box_length (cu->cu_inx_bits[0]) < 10 + (cu->cu_fill / 8))
    {
      int n_bytes = ALIGN_8 (cu->cu_fill + 10000) / 8;
      cu->cu_inx_bits = t_alloc_box (sizeof (caddr_t) * 5, DV_BIN);
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
	      ((db_buf_t *) dc->dc_values)[r] = ((db_buf_t *) o->dc_values)[inx];
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
  hash_table_init (&p_to_csetp_list, 211);
  hash_table_init (&id_to_cset, 223);
  hash_table_init (&rdfs_type_cset, 223);
  bif_define ("cset_def", bif_cset_def);
  bif_define ("cset_p_def", bif_cset_p_def);
  bif_define ("cset_type_def", bif_cset_type_def);
  bif_define ("cset_uri_def", bif_cset_uri_def);
  save = enable_qp;
  enable_qp = 1;
  ddl_ensure_table ("do this always",
      "select count (cset_def (cset_id, cset_range, cset_table, cset_rq_table, cset_id_range)) from rdf_cset");
  ddl_ensure_table ("do this always",
      "select count (cset_p_def (csetp_cset, csetp_nth, csetp_iid, csetp_col, csetp_options)) from rdf_cset_p");
  ddl_ensure_table ("do this always", "select count (cset_type_def (cst_cset, cst_type)) from rdf_cset_type");
  cset_uri = NULL;
  ddl_ensure_table ("do this always", "select count (cset_uri_def (csu_cset, csu_pattern)) from rdf_cset_uri");

  enable_qp = save;
}

#else

void
cset_init ()
{
}

#endif
