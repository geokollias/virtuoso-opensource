
[Parameters]
MaxQueryMem = 30G
HashJoinSpace = 10G
AdjustVectorSize = 0
VectorSize = 100000
MaxVectorSize = 100000 
;NumberOfBuffers = 8000000 
;MaxDirtyBuffers = 10000

[Flags]
hash_join_enable = 2
dfg_max_empty_mores = 10000 
dfg_empty_more_pause_msec = 5
qp_thread_min_usec = 100
cl_dfg_batch_bytes = 100000000 
enable_mt_txn = 1
enable_mt_transact = 1
enable_high_card_part = 1
enable_vec_reuse = 1
mp_local_rc_sz = 0
dbf_explain_level = 0
enable_feed_other_dfg = 0
;enable_cll_nb_read = 1
dbf_no_sample_timeout = 1
;enable_cl_compress = 1
enable_conn_fifo = 0
qrc_tolerance = 40
