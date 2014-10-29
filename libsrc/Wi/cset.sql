



create function new_cset_id () returns int
{
  declare n int;
  select max (cset_id) into n from rdf_cset;
  if (n is null)
    return 1;
  return n + 1;
}
;

create procedure cset_changed (in cs_id int)
{
  declare ign int;
  select count (cset_def (cset_id, cset_range, cset_table, cset_rq_table, cset_id_range)) into ign from rdf_cset where cset_id = cs_id;
  select count (cset_p_def (csetp_cset, csetp_nth, csetp_iid, csetp_col, csetp_options)) into ign from rdf_cset_p where csetp_cset = cs_id;
  select count (cset_type_def (cst_cset, cst_type)) into ign from rdf_cset_type where cst_cset = cs_id;
  select count (cset_uri_def (csu_cset, csu_pattern)) into ign from rdf_cset_uri where csu_cset = cs_id;
  log_text ('cset_changed (?)', cs_id);
}
;

create procedure rdf_cset (in name varchar, in properties any, in types any)
{
  declare inx, cs_id, g_id, s_id, k_id, rid, ign int;
  declare tn varchar;
  declare opt, indexed any;
  tn := 'DB.DBA.' || name || '_cset';
  if (exists (select 1 from sys_keys where upper (key_table) = upper (tn)))
    signal ('42000', 'cset exists already');
 indexed := make_array (length (properties), 'any');
  for (inx := 0; inx < length (properties); inx := inx + 1)
    {
      declare pn any;
    pn := properties[inx];
      if (isvector (pn))
	{
	  indexed[inx] := pn;
	pn := pn[0];
	}
      properties[inx] := iri_to_id (pn);
    }
  for (inx := 0; inx < length (types); inx := inx + 1)
    types[inx] := iri_to_id (types[inx]);
  k_id := new_key_id (sys_stat ('__internal_first_id'));
  insert into sys_keys (key_table, key_name, key_id, key_is_main, key_super_id, key_options, key_version, key_n_significant)
  values (tn, tn, k_id, 1, k_id, vector ('column'), 1, 2);
 s_id := new_col_id (sys_stat ('__internal_first_id'));
  insert into sys_cols ("TABLE", "COLUMN", col_id, col_dtp, col_prec, col_scale, col_nullable, col_options)
    values (tn, 'S', s_id, 244, 20, 0, 0, vector ());
 cs_id := new_cset_id ();  sequence_set (tn || '_S', bit_shift (cs_id, 53), 0);
 rid := ir_init (tn || '_rng', tn || '_S', ir_max => bit_shift (cs_id + 1, 53));
  insert into  rdf_cset (cset_id, cset_name, cset_table, cset_rq_table, cset_range, cset_id_range, cset_options)
    values (cs_id, name, tn, 'DB.DBA.RDF_QUAD', cs_id, rid, vector ());

 g_id := new_col_id (sys_stat ('__internal_first_id'));
  insert into sys_cols ("TABLE", "COLUMN", col_id, col_dtp, col_prec, col_scale, col_nullable, col_options)
    values (tn, 'G', g_id, 244, 20, 0, 0, vector ());
  insert into sys_key_parts  values (k_id, 0, s_id);
  insert into sys_key_parts  values (k_id, 1, g_id);
  for (inx := 0; inx < length (properties); inx := inx + 1)
    {
      declare cn varchar;
      declare c_id, sl, ha int;
    cn := id_to_iri (properties[inx]);
    ha := strrchr (cn, '#');
    sl := strrchr (cn, '/');
      if (ha is not null)
      cn := subseq (cn, ha + 1);
      else if (sl is not null)
      cn := subseq (cn, sl + 1);
      if (exists (select 1 from sys_cols where "TABLE" = tn and "COLUMN" = cn))
      cn := id_to_iri (properties[inx]);
    c_id :=  new_col_id (sys_stat ('__internal_first_id'));
      insert into sys_cols ("TABLE", "COLUMN", col_id, col_dtp, col_prec, col_scale, col_options)
	values (tn, cn, c_id, 242, 0, 0, vector ());
      insert into sys_key_parts values (k_id, inx + 2, c_id);
      insert into rdf_cset_p (csetp_cset, csetp_nth, csetp_iid, csetp_col, csetp_options)
	values (cs_id, inx, properties[inx], cn, indexed[inx]);
    }
  for (inx := 0; inx < length (types); inx  := inx + 1)
    {
      insert soft rdf_cset_type values (types[inx], cs_id);
    }
  declare pc, pd any;
  whenever not found goto np;
  select part_cluster, part_data into pc, pd from sys_partition where part_table = 'DB.DBA.RDF_QUAD' and part_key = 'RDF_QUAD';
  pd[1] := tn;
  pd[2] := name_part (tn, 2);
  pd[4][0][1] := 'S';
  insert into sys_partition (part_table, part_key, part_version, part_cluster, part_data) values (tn, tn, 0, pc, pd);
 np:
  __ddl_changed (tn);
  cl_exec ('db..cset_changed (?)', params => vector (cs_id));
}
;


create procedure cset_iri_pattern (in name varchar, in pattern varchar, in fields any)
{
  declare sz, inx, start, cset, rid, ign int;
  declare tb, seq varchar;
  sz := 1;
  for (inx := 0; inx < length (fields); inx := inx + 1)
    {
      if (fields[inx][1] - fields[inx][0] > 1)
      sz := sz * fields[inx][1] - fields[inx][0];
    }
  whenever not found goto nocs;
  select cset_table, cset_id, cset_id_range into tb, cset, rid from rdf_cset where cset_name = name;
 seq := tb || '_S';
 start := sequence_next (seq);
  sequence_set (seq, start + sz, 0);
  insert into rdf_iri_pattern (rip_pattern, rip_cset, rip_start, rip_fields, rip_int_range, rip_exc_range)
    values (pattern, cset, iri_id_from_num (start), fields, 0, rid);
  select count (iri_pattern_def (rip_pattern, rip_start, rip_fields, rip_cset, rip_int_range, rip_exc_range)) into ign from rdf_iri_pattern where rip_pattern = pattern;
  iri_pattern_changed ();
  log_text ('  select count (iri_pattern_def (rip_pattern, rip_start, rip_fields, rip_cset, rip_int_range, rip_exc_range)) into ign from rdf_iri_pattern where rip_pattern = ?', pattern);
  log_text ('iri_pattern_changed ()');

  return;
 nocs:
  signal ('CSETN', 'bad cset name');
}
;




