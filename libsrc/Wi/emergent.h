/*
 *  emergent.h
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



#define CSET_MAX 512

typedef unsigned short rtr_id_t;

#define CSET_RNG_SHIFT 53
#define IRI_RANGE(i) ((((iri_id_t)(i)) >> CSET_RNG_SHIFT) & 0x1FF)


typedef struct rt_range_s
{
  rtr_id_t rtr_id;
  dbe_table_t *rtr_table;
  struct cset_s *rtr_cset;
  caddr_t rtr_seq;
} rt_range_t;

extern rt_range_t rt_ranges[512];

extern id_hash_t cset_p_by_string;
extern dk_hash_t cset_p_by_id;
extern dk_hash_t int_seq_to_cset;


typedef struct cset_s
{
  int cset_id;			/*unique, used for sorting csets when matching */
  rtr_id_t cset_rtr_id;
  rt_range_t *cset_rtr;
  dk_hash_t cset_p;
  dk_hash_t cset_except_p;
  struct cset_p_s **cset_p_array;	/* cols left to right  in def order */
  dbe_table_t *cset_table;
  dbe_table_t *cset_rq_table;
  struct query_s *cset_ins;
  struct query_s *cset_del;
  id_range_t *cset_ir;
  id_range_t *cset_bn_ir;
  dk_mutex_t cset_mtx;
} cset_t;

typedef struct cset_p_s cset_p_t;

typedef void (*cset_cluster_t) (void *cu, cset_t * cset, cset_p_t * csetp, caddr_t * row, int s_col, int p_col, int o_col,
    int g_col, caddr_t cd);


struct cset_p_s
{
  iri_id_t csetp_iri;
  cset_t *csetp_cset;
  dbe_key_t *csetp_key;
  dbe_column_t *csetp_col;
  int csetp_nth;
  int csetp_inline_max;
  char csetp_index_o;
  uint32 *csetp_n_bloom;
  uint64 **csetp_bloom;
  dk_mutex_t csetp_bloom_mtx;	/* serialize writing to bloom */
  id_hash_t *csetp_non_index_o;
  cset_cluster_t csetp_cluster;
  caddr_t csetp_cluster_cd;
};




typedef struct cset_ins_s
{
  cset_t *csi_cset;
  dbe_key_t *csi_key;
  data_col_t **csi_dcs;
  id_hash_t *csi_sg_row;
} cset_ins_t;

typedef struct sg_key_s
{
  iri_id_t s;
  iri_id_t g;
} sg_key_t;

extern dk_hash_t id_to_cset;
extern int rt_n_csets;
extern dk_hash_t rdfs_type_cset;
extern dk_hash_t p_to_csetp_list;
extern int enable_cset;


/* key_rdf_order */
#define I_PSOG 0
#define I_POSG 1
#define I_OP 2
#define I_SP 3
#define I_GS 4
#define I_OTHER -1


typedef struct dv_arr_iter_s
{
  db_buf_t dvai_dv;
  int dvai_n;
  int dvai_pos;
} dv_array_iter_t;

#define cset_by_id(id) \
  (cset_t*)gethash ((void*)(ptrlong)(id), &id_to_cset)


#define CSQ_TABLE 0
#define CSQ_EXC_S CSET_MAX	/* look for s values in quad where no s in the table */
#define CSQ_G_MIX (2 * CSET_MAX)
#define CSQ_LOOKUP (3 * CSET_MAX)
#define CSQ_RUN (CSET_MAX * 16)	/* bit set if in the middle of a mode, continue in mode until at end */

caddr_t cset_opt_value (caddr_t * opts, char *opt);
id_range_t *ir_by_name (char *name);
id_range_t *ir_sub_ir (id_range_t * super, int nth);


void csetp_ensure_bloom (cset_p_t * csetp, caddr_t * inst, int create_sz, uint64 ** bf_ret, uint32 * bf_n_ret);
void csetp_bloom (cset_p_t * csetp, caddr_t * inst, uint64 ** bf_ret, uint32 * bf_n_ret);


#define CSQ_PS 3
#define CSQ_P 2
#define CSQ_S 1
#define CSQ_ 0
