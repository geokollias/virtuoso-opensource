


#define CSET_MAX 512

typedef unsigned short rtr_id_t;

#define IRI_RANGE(i) ((((iri_id_t)(i)) >>  53) & 0x1FF)


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

typedef struct cset_s
{
  int cset_id;			/*unique, used for sorting csets when matching */
  rtr_id_t cset_rtr_id;
  rt_range_t *cset_rtr;
  dk_hash_t cset_p;
  dk_hash_t cset_except_p;
  dbe_table_t *cset_table;
  dbe_table_t *cset_rq_table;
  struct query_s *cset_ins;
  struct query_s *cset_del;
  id_range_t *cset_ir;
} cset_t;

typedef struct cset_p_s
{
  iri_id_t csetp_iri;
  cset_t *csetp_cset;
  dbe_key_t *csetp_key;
  dbe_column_t *csetp_col;
  int csetp_nth;
  char csetp_index_o;
  uint32 csetp_n_bloom;
  uint64 *csetp_bloom;
  dk_mutex_t csetp_bloom_mtx;	/* serialize writing to bloom */
  id_hash_t *csetp_non_index_o;
} cset_p_t;




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
