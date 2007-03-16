/*
 *  sort.c
 *
 *  $Id$
 *
 *  SQL ORDER BY sort and DISTINCT
 *  
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *  
 *  Copyright (C) 1998-2006 OpenLink Software
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
 *  
*/

#include "sqlnode.h"
#include "sqlopcod.h"
#include "sqlopcod.h"
#include "sqlpar.h"
#include "sqlcmps.h"
#include "sqlintrp.h"
#include "arith.h"


int
setp_comp_array (setp_node_t * setp, caddr_t * qst, caddr_t * left, state_slot_t ** right)
{
  int inx;
  dk_set_t is_rev = setp->setp_key_is_desc;
  _DO_BOX (inx, right)
    {
      collation_t * coll = setp->setp_keys_box[inx]->ssl_sqt.sqt_collation;
      caddr_t right_v;
      int rc;

      right_v = qst_get (qst, right[inx]);
      rc = cmp_boxes (left[inx], right_v, coll, coll);
      if (rc == DVC_UNKNOWN)
	{ /* GK: the NULLs sort high */
	  dtp_t dtp1 = DV_TYPE_OF (left[inx]);
	  dtp_t dtp2 = DV_TYPE_OF (right_v);

	  if (dtp1 == DV_DB_NULL)
	    {
	      if (dtp2 == DV_DB_NULL)
		rc = DVC_MATCH;
	      else
		rc = DVC_LESS;
	    }
	  else
	    rc = DVC_GREATER;
	}
      if (is_rev && ORDER_DESC == (ptrlong) is_rev->data)
	rc = DVC_INVERSE (rc);
      if (rc != DVC_MATCH)
	return rc;
      is_rev = is_rev ? is_rev->next : NULL;

    }
  END_DO_BOX;
  return DVC_MATCH;
}


int
setp_key_comp (setp_node_t * setp, caddr_t * qst, state_slot_t ** left, state_slot_t ** right)
{
  int inx;
  dk_set_t is_rev = setp->setp_key_is_desc;
  DO_BOX (state_slot_t *, l, inx, left)
    {
      collation_t * coll = setp->setp_keys_box[inx]->ssl_sqt.sqt_collation;
      int rc = cmp_boxes (qst_get (qst, l), qst_get (qst, right[inx]), coll, coll);
      if (is_rev && ORDER_DESC == (ptrlong) is_rev->data)
	rc = DVC_INVERSE (rc);
      if (rc != DVC_MATCH)
	return rc;
      is_rev = is_rev ? is_rev->next : NULL;

    }
  END_DO_BOX;
  return DVC_MATCH;
}


void
setp_mem_insert (setp_node_t * setp, caddr_t * qst, int pos, caddr_t ** arr, int fill)
{
  int n_keys = BOX_ELEMENTS (setp->setp_keys_box), inx;
  int top = BOX_ELEMENTS (arr);
  if (fill == top)
    dk_free_tree ((caddr_t) arr[fill - 1]);
  else
    qst_set_long (qst, setp->setp_row_ctr, fill + 1);
  if (pos < fill)
    memmove (&arr[pos + 1], &arr[pos], sizeof (caddr_t) * (fill - pos - (fill == top ? 1 : 0)));
  arr[pos] = (caddr_t *) dk_alloc_box ((n_keys + BOX_ELEMENTS (setp->setp_dependent_box)) * sizeof (caddr_t),
			   DV_ARRAY_OF_POINTER);
  for (inx = 0; inx < n_keys; inx++)
    arr[pos][inx] = box_copy_tree (qst_get (qst, setp->setp_keys_box[inx]));
  for (inx = 0; ((uint32) inx) < BOX_ELEMENTS (setp->setp_dependent_box); inx++)
    arr[pos][inx + n_keys] = box_copy_tree (qst_get (qst, setp->setp_dependent_box[inx]));
}


long setp_top_row_limit = 10000;

void
setp_mem_sort (setp_node_t * setp, caddr_t * qst)
{
  caddr_t ** arr = (caddr_t **) qst_get (qst, setp->setp_sorted);
  ptrlong top = unbox (qst_get (qst, setp->setp_top));
  ptrlong skip = setp->setp_top_skip ? unbox (qst_get (qst, setp->setp_top_skip)) : 0;
  ptrlong fill = unbox (qst_get (qst, setp->setp_row_ctr));
  ptrlong rc, guess, at_or_above = 0, below = fill;

  if (!arr)
    {
      if (setp_top_row_limit < top + skip)
	sqlr_new_error ("22023", "SR353",
	     "Sorted TOP clause specifies more then %ld rows to sort. "
	     "Only %ld are allowed. "
	     "Either decrease the offset and/or row count or use a scrollable cursor",
	     (long) (top + skip), setp_top_row_limit);
      arr = (caddr_t **) dk_alloc_box (sizeof (caddr_t) * (top + skip), DV_ARRAY_OF_POINTER);
      memset (arr, 0, (top + skip) * sizeof (caddr_t));
      qst_set (qst, setp->setp_sorted, (caddr_t) arr);
    }
  if (skip < 0)
    sqlr_new_error ("22023", "SR351", "SKIP parameter < 0");
  if (top < 0)
    sqlr_new_error ("22023", "SR352", "TOP parameter < 0");
  if (top + skip == 0)
    return;
  if (fill == (top + skip) && DVC_GREATER != setp_comp_array (setp, qst, arr[fill - 1], setp->setp_keys_box))
    return;
  if (!fill || DVC_GREATER == setp_comp_array (setp, qst, arr[0], setp->setp_keys_box))
    {
      setp_mem_insert (setp, qst, 0, arr, (int) fill);
      return;
    }
  guess = fill / 2;
  for (;;)
    {
      rc = setp_comp_array (setp, qst, arr[guess], setp->setp_keys_box);
      if (below - guess <= 1)
	{
	  if (DVC_MATCH == rc || DVC_LESS == rc)
	    guess++;
	  setp_mem_insert (setp, qst, (int) guess, arr, (int) fill);
	  return;
	}
      if (DVC_LESS == rc || DVC_MATCH == rc)
	at_or_above = guess;
      else
	below = guess;
      guess = at_or_above + ((below - at_or_above) / 2);
    }
}


int
setp_top_pre (setp_node_t * setp, caddr_t * qst, int * is_ties_edge)
{
  int rc;
  if (!setp->setp_top)
    return 0;
  if (!setp->setp_keys_box)
    {
      setp->setp_keys_box = (state_slot_t **) dk_set_to_array (setp->setp_keys);
      setp->setp_dependent_box = (state_slot_t **) dk_set_to_array (setp->setp_dependent);
    }
  if (unbox (qst_get (qst, setp->setp_flushing_mem_sort)))
    return 0;
  if (!setp->setp_ties)
    {
      setp_mem_sort (setp, qst);
      return 1;
    }
  if (unbox (qst_get (qst, setp->setp_row_ctr)) <= unbox (qst_get (qst, setp->setp_top)))
    return 0;
  rc = setp_key_comp (setp, qst, setp->setp_keys_box, setp->setp_last_vals);
  if (DVC_GREATER == rc)
    return 1;
  if (!setp->setp_ties && DVC_MATCH == rc)
    {
      *is_ties_edge = 1;
      return 1;
    }
  return 0;
}


int
setp_node_run (setp_node_t * setp, caddr_t * inst, caddr_t * state, int print_blobs)
{
  int is_ties_edge = 0;
  if (HA_FILL == setp->setp_ha->ha_op)
    {
      itc_ha_feed_ret_t ihfr;
      itc_ha_feed (&ihfr, setp->setp_ha, inst, 0);
	return DVC_MATCH;
    }
  if (setp->setp_distinct)
    {
      itc_ha_feed_ret_t ihfr;
      if (DVC_MATCH == itc_ha_feed (&ihfr, setp->setp_ha, inst, 0))
	return 0;
      else
	return 1;
    }
  if (setp_top_pre (setp, state, &is_ties_edge))
    return 0;

  if (setp->setp_ha->ha_op != HA_GROUP)
    setp_order_row (setp, inst);
  else
    setp_group_row (setp, inst);
  return 1;
}


void
setp_filled (setp_node_t * setp, caddr_t * qst)
{
  hash_area_t * ha = setp->setp_ha;
  if (HA_GROUP == ha->ha_op
      || HA_FILL == ha->ha_op)
    {
      it_cursor_t * ins_itc, * ref_itc;
#ifdef NEW_HASH
      it_cursor_t *bp_ref_itc;
#endif
      index_tree_t * it = (index_tree_t *) QST_GET (qst, ha->ha_tree);
      if (!it)
	return;
      ins_itc = (it_cursor_t *) QST_GET_V (qst, ha->ha_insert_itc);
      ref_itc = (it_cursor_t *) QST_GET_V (qst, ha->ha_ref_itc);
#ifdef NEW_HASH
      bp_ref_itc = (it_cursor_t *) QST_GET_V (qst, ha->ha_bp_ref_itc);
#endif
      if (ins_itc && ins_itc->itc_hash_buf)
	{
	  page_leave_outside_map (ins_itc->itc_hash_buf);
	  ins_itc->itc_hash_buf = NULL;
	}
      if (ref_itc && ref_itc->itc_buf)
	{
	  page_leave_outside_map (ref_itc->itc_buf);
	  ref_itc->itc_buf = NULL;
	}
#ifdef NEW_HASH
      if (bp_ref_itc && bp_ref_itc->itc_buf)
	{
	  page_leave_outside_map (bp_ref_itc->itc_buf);
	  bp_ref_itc->itc_buf = NULL;
	}
#endif

    }
}


void
setp_temp_clear (setp_node_t * setp, hash_area_t * ha, caddr_t * qst)
{
  it_cursor_t * ins_itc = (it_cursor_t*) QST_GET_V (qst, ha->ha_insert_itc);
  it_cursor_t * ref_itc = (it_cursor_t*) QST_GET_V (qst, ha->ha_ref_itc);
#ifdef NEW_HASH
  it_cursor_t * bp_ref_itc = (it_cursor_t*) QST_GET_V (qst, ha->ha_bp_ref_itc);
#endif
  index_tree_t * it = (index_tree_t*) QST_GET_V (qst, ha->ha_tree);
  qst[ha->ha_insert_itc->ssl_index] = NULL;
  qst[ha->ha_ref_itc->ssl_index] = NULL;
#ifdef NEW_HASH
  qst[ha->ha_bp_ref_itc->ssl_index] = NULL;
#endif
  qst[ha->ha_tree->ssl_index] = NULL;
  if (ins_itc)
    itc_free (ins_itc);
  if (ref_itc)
    itc_free (ref_itc);
#ifdef NEW_HASH
  if (bp_ref_itc)
    itc_free (bp_ref_itc);
#endif
  if (it)
    it_temp_free (it);
}


void
setp_mem_sort_flush (setp_node_t * setp, caddr_t * qst)
{
  caddr_t ** arr;
  int fill;
  if (!setp->setp_sorted)
    return;
#ifndef O12
  if (setp->setp_ordered_gb_out)
    {
      data_source_t * gb_read_ts = (data_source_t *) setp->setp_ordered_gb_fref->src_gen.src_continuations->data;
      setp_set_gb_out (setp, qst, 1);
      qn_record_in_state ((data_source_t *) setp, qst, NULL);
      qn_input ((data_source_t *) gb_read_ts->src_continuations->data, qst, qst);
      return;
    }
#endif
  arr = (caddr_t **) qst_get (qst, setp->setp_sorted);
  fill = (int)  unbox (qst_get (qst, setp->setp_row_ctr));
  if (fill && arr)
    {
      int col;
      ptrlong row;
      int n_keys = BOX_ELEMENTS (setp->setp_keys_box);
      int n_deps = BOX_ELEMENTS (setp->setp_dependent_box);
      ptrlong skip = setp->setp_top_skip ? unbox (qst_get (qst, setp->setp_top_skip)) : 0;
      qst_set_long (qst, setp->setp_flushing_mem_sort, 1);
      for (row = 0; row < fill; row++)
	{
	  for (col = 0; col < n_keys; col++)
	    {
	      qst_set (qst, setp->setp_keys_box[col], arr[row][col]);
	      arr[row][col] = NULL;
	    }
	  for (col = 0; col < n_deps; col++)
	    {
	      qst_set (qst, setp->setp_dependent_box[col], arr[row][col + n_keys]);
	      arr[row][col + n_keys] = NULL;
	    }
	  if (skip)
	    {
	      skip--;
	      continue;
	    }
	  setp_node_run (setp, qst, qst, 0);
	}
      qst_set_long (qst, setp->setp_flushing_mem_sort, 0);
      qst_set_long (qst, setp->setp_row_ctr, 0);
    }
}


void
setp_node_input (setp_node_t * setp, caddr_t * inst, caddr_t * state)
{
  int cont = setp_node_run (setp, inst, state, 0);
  if (setp->setp_set_op == INTERSECT_ST ||
      setp->setp_set_op == INTERSECT_ALL_ST)
    cont = !cont;
  if (cont)
    qn_send_output ((data_source_t *) setp, state);
}


void
union_node_input (union_node_t * un, caddr_t * inst, caddr_t * state)
{
  int inx;
  for (;;)
    {
      dk_set_t out_list = un->uni_successors;
      int nth;
      if (!state)
	{
	  state = qn_get_in_state ((data_source_t *) un, inst);
	  nth = (int) unbox (qst_get (state, un->uni_nth_output));
	}
      else
	{
	  qst_set (inst, un->uni_nth_output, box_num (0));
	  nth = 0;
	}
      for (inx = 0; inx < nth; inx++)
	{
	  if (out_list)
	    out_list = out_list->next;
	  if (!out_list)
	    break;
	}
      if (!out_list)
	{
	  qn_record_in_state ((data_source_t *) un, inst, NULL);
	  qst_set (inst, un->uni_nth_output, box_num (0));
	  return;
	}
      qst_set (inst, un->uni_nth_output, box_num (nth + 1));
      qn_record_in_state ((data_source_t *) un, inst, inst);
      qn_input (((query_t *) out_list->data)->qr_head_node, inst, inst);
      qr_resume_pending_nodes ((query_t*) out_list->data, inst);
      state = NULL;
    }
}


void
subq_node_input (subq_source_t * sqs, caddr_t * inst, caddr_t * state)
{
  int any_passed = 0;
  int inx;
  caddr_t err;
  int flag;
  for (;;)
    {
      if (!state)
	{
	  state = qn_get_in_state ((data_source_t *) sqs, inst);
	  flag = CR_OPEN;
	  any_passed = 1;
	}
      else
	{
	  subq_init (sqs->sqs_query, state);
	  flag = CR_INITIAL;
	}
      err = subq_next (sqs->sqs_query, inst, flag);

      if (err == (caddr_t) SQL_NO_DATA_FOUND
	  && !any_passed && sqs->sqs_is_outer)
	{
	  /* no data on first call and outer node. Set to null and continue */
	  qn_record_in_state ((data_source_t *) sqs, inst, NULL);
	  DO_BOX (state_slot_t *, out, inx, sqs->sqs_out_slots)
	  {
	    qst_set_bin_string (inst, out, (db_buf_t) "", 0, DV_DB_NULL);
	  }
	  END_DO_BOX;
	  qn_ts_send_output ((data_source_t *) sqs, inst,
	      sqs->sqs_after_join_test);
	  return;
	}

      if (err == SQL_SUCCESS)
	{
	  if (!sqs->src_gen.src_after_test
	      || code_vec_run (sqs->src_gen.src_after_test, inst))
	    {
	      qn_record_in_state ((data_source_t *) sqs, inst, inst);
	      any_passed = 1;
	      qn_ts_send_output ((data_source_t *) sqs, inst,
	          sqs->sqs_after_join_test);
	    }
	}
      else
	{
	  qn_record_in_state ((data_source_t *) sqs, inst, NULL);
	  if (err != (caddr_t) SQL_NO_DATA_FOUND)
	    sqlr_resignal (err);
	  return;
	}
      state = NULL;
    }
}
