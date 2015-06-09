



create function c (in name varchar) returns any
{
  declare f any;
 f := connection_get (name);
  if (f is null)
    signal ('NOCFG', sprintf ('no clcfg environment setting %s', name));
  return f;
}

create function dbfile (in name varchar)
{
  declare fs any;
 fs := connection_get ('files');
  if (fs is null)
  fs := vector (name);
  else 
  fs := vector_concat (fs, vector (name));
  connection_set ('files', fs);
  return name;
}


create function str_eval (in str varchar) returns any
{
  declare res, st, msg, md any;
 str := sprintf ('select %s', str);
 st := '00000';
  exec (str, st, msg, vector (), 0, md, res);
  if (st <> '00000')
    signal (st, msg);
  return res[0][0];
}

create procedure subst_exp (in str varchar)
{
  declare inx, level, len, start, d int;
  declare repl varchar;
 len := length (str);
 start := -1;
  for (inx := 0; inx < len; inx := inx + 1)
    {
      if ('{'[0] = str[inx])
	{
	  if (0 = level)
	  start := inx;
	level := level  + 1;
	}
      if ('}'[0] = str[inx])
	{
	  if (-1 = start)
	    signal ('37000', 'unbalanced {} in template file');
	level := level - 1;
	  if (0 = level)
	    {
	    repl := cast (str_eval (subseq (str, start + 1, inx)) as varchar);
	    str := replace (str, subseq (str, start, inx + 1), repl);
	    d := length (repl) - (1 + inx - start);
	    len := len + d;
	    inx := inx + d;
	    start := -1;
	    }
	}
    }
  return str;
}


create procedure dir_part (in f varchar)
{
  declare s int;
 s := strrchr (f, '/'[0]);
  if (s is null)
    return null;
  return subseq (f, 0, s);
}


create procedure cl_setup (in hosts any, in n_per_host int, in run_dir varchar)
{
  declare ctr, nth_in_host, host_ctr, n_hosts, cl_port, proc_ctr int;
  declare del_str, init_str, start_str, cpex_str, clgl_str, files  any;
  declare h_dir, run_dir_exp, host varchar;
 cl_port := 12200;
 n_hosts := length (hosts);
  del_str := string_output ();
  init_str := string_output ();
 start_str := string_output ();
 del_str := string_output ();
 clgl_str := string_output ();
 cpex_str := string_output ();
  http (file_to_string ('cluster.global.ini.tpl'), clgl_str);
 nth_in_host := 0;
 host_ctr := 0;
  connection_set ('n_per_host', n_per_host); 
  string_to_file ('virtuoso.global.ini.1', subst_exp (file_to_string ('virtuoso.global.ini.tpl')), -2);
 ctr := 0;
  for (host_ctr := 0; host_ctr < n_hosts; host_ctr := host_ctr + 1)
    {
    host := hosts[host_ctr];
      connection_set ('host', host_ctr + 1);
    run_dir_exp := subst_exp (run_dir);
      http (sprintf ('ssh %s mkdir %s\n', host, run_dir_exp), init_str);
      http (sprintf ('ssh %s killall -9 virtuoso\n', host), start_str);
      http (sprintf ('ssh %s rm -rf %s\n', host, subst_exp (run_dir)), del_str);
      http (sprintf ('scp virtuoso %s:%s\n', host, run_dir_exp), cpex_str); 

      for (proc_ctr := 0; proc_ctr < n_per_host; proc_ctr := proc_ctr + 1)
	{
	  connection_set ('proc_in_host', proc_ctr);
	  connection_set ('proc', ctr + 1);
	  connection_set ('files', null);
	  string_to_file (sprintf ('cluster.ini.%d', ctr + 1), subst_exp (file_to_string ('cluster.ini.tpl')), -2);
	  string_to_file (sprintf ('virtuoso.ini.%d', ctr + 1), subst_exp (file_to_string ('virtuoso.ini.tpl')), -2);
	  http (sprintf ('Host%d = %s:%d\n', ctr + 1, host, cl_port + ctr + 1), clgl_str);
	h_dir := sprintf ('%s/%d', run_dir_exp, ctr + 1);
	  http (sprintf ('ssh %s mkdir %s\n', host, h_dir), init_str);

	files := connection_get ('files');
	  if (files is not null)
	    {
	      declare n_files, inx int;
	    n_files := length (files);
	      for (inx :=0; inx < n_files; inx := inx + 1)
		{
		  http (sprintf ('ssh %s mkdir %s\n', host, dir_part (files[inx])), init_str);
		  http (sprintf ('ssh %s "rm %s*"\n', host, files[inx]), del_str);
		}
	    }
	  http (sprintf ('scp cluster.ini.%d %s:%s/cluster.ini\n', ctr + 1, host, h_dir), init_str);
	  http (sprintf ('scp virtuoso.ini.%d %s:%s/virtuoso.ini\n', ctr + 1, host, h_dir), init_str);
	  http (sprintf ('scp cluster.global.ini.1 %s:%s/cluster.global.ini\n', host, h_dir), init_str);
	  http (sprintf ('scp virtuoso.global.ini.1 %s:%s/virtuoso.global.ini\n', host, h_dir), init_str);
	  http (sprintf ('ssh %s "(cd %s; ../virtuoso)"\n', host, h_dir), start_str);
	ctr := ctr + 1;
	}
    }
  string_to_file ('start.sh', start_str, -2);
  string_to_file ('init.sh', init_str, -2);
  string_to_file ('delete.sh', del_str, -2);
  string_to_file ('cpexe.sh', cpex_str, -2);
  string_to_file ('cluster.global.ini.1', clgl_str, -2);

}

create procedure dbgen_rf (inout str any)
{
  declare tpl, files varchar;
 files := string_output_string (str);
 tpl := file_to_string ('clrefresh.sql.tpl');
 tpl := replace (tpl, 'XXX', subseq (files, 2, length (files)));
  string_to_file ('clrefresh.sql', tpl, -2);
}


create procedure dbgen (in hosts any, in data_dir varchar, in scale int, in files_per_host int, in procs_per_host int, in n_refresh int)
{
  declare host, data_dir_exp, data_dir_1 varchar;
  declare host_ctr, n_hosts, slice_ctr, ctr int;
  declare dbgen_str, ucp_str, refr_str any;
 dbgen_str := string_output ();
 ucp_str := string_output ();
 refr_str := string_output ();
 ctr := 1;
 n_hosts := length (hosts);
  for (host_ctr := 0; host_ctr < n_hosts; host_ctr := host_ctr + 1)
    {
    host := hosts[host_ctr];
      connection_set ('host', host_ctr + 1);
    data_dir_exp := subst_exp (data_dir);
      http (sprintf ('ssh %s mkdir %s\n', host, data_dir_exp), dbgen_str);
      http (sprintf ('scp dbgen %s:%s\n', host, data_dir_exp), dbgen_str);
      http (sprintf ('scp dists.dss %s:%s\n', host, data_dir_exp), dbgen_str);
      if (0 = host_ctr)
	{
	  http (sprintf ('ssh %s "(cd %s; ./dbgen -q -f -s %d -U %d -i %d -d %d)" &\n', host, data_dir_exp, scale, n_refresh, n_hosts * procs_per_host, n_hosts * procs_per_host), dbgen_str);
	data_dir_1 := data_dir_exp;
	}
      for (slice_ctr := 0; slice_ctr < files_per_host; slice_ctr := slice_ctr + 1)
	{
	  http (sprintf ('sleep 1\nssh %s "(cd %s; ./dbgen -z -q -f -s %d -C %d -S %d)" &\n', host, data_dir_exp, scale, n_hosts * files_per_host, host_ctr * files_per_host + slice_ctr + 1), dbgen_str);
	}
      for (slice_ctr := 0; slice_ctr < procs_per_host; slice_ctr := slice_ctr + 1)
	{
	  if (ctr > procs_per_host)
	    http (sprintf ('ssh %s \"scp %s/*.u*.%d %s:%s \"\n', hosts[0], data_dir_1, ctr, host, data_dir_exp), ucp_str);
	  http (sprintf (', ''%s/FILE.uNTH.%d'', %d', data_dir_exp, ctr, ctr), refr_str);
	ctr := ctr + 1;
	}
    }
  http ('wait\n', dbgen_str);
  for (host_ctr := 0; host_ctr < n_hosts; host_ctr := host_ctr + 1)
    {
    host := hosts[host_ctr];
      connection_set ('host', host_ctr + 1);
    data_dir_exp := subst_exp (data_dir);
      http (sprintf ('ssh %s chmod -R 777  %s\n', host, data_dir_exp), dbgen_str);
      if (host_ctr > 0)
	http (sprintf ('ssh %s rm  %s/nation.tbl %s/region.tbl\n', host, data_dir_exp, data_dir_exp), dbgen_str);
    }

  string_to_file ('dbgen.sh', dbgen_str, -2);
  string_to_file ('dbgen.sh', ucp_str, -1);
  dbgen_rf (refr_str);
}



-- cl_setup (vector ('madras-i', 'masala-i'), 2, '/{c (''host'') }d1/tpch100c');

--dbgen (vector ('madras-i', 'masala-i'), '/{c (''host'')}d1/tpch100data', 100, 24, 2, 12);
