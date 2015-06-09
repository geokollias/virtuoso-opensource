




create procedure tpch_cl_load (in f varchar)
{
  declare fname varchar;
 fname := f;
  if (0 = sys_stat ('cl_run_local_only'))
    {
      declare host, slice int;
    host :=        sys_stat ('cl_this_host');
    slice := cl_hosted_slices ('ELASTIC', host)[0];
    f := vector (f, vector (slice));
    }

  log_enable (2,1);
  if (fname like '%lineitem.tbl.%')
    {
      insert into lineitem select * from lineitem_f table option (from f);
      return;
    }
  if (fname like '%orders.tbl.%')
    {
      insert into orders select * from orders_f table option (from f);
      return;
    }
  if (fname like '%part.tbl.%')
    {
      insert into part select * from part_f table option (from f);
      return;
    }
  if (fname like '%partsupp.tbl.%')
    {
      insert into partsupp select * from partsupp_f table option (from f);
      return;
    }
  if (fname like '%customer.tbl.%')
    {
      insert into customer select * from customer_f table option (from f);
      return;
    }
  if (fname like '%nation.tbl%')
    {
      insert into nation select * from nation_f table option (from f);
      return;
    }
  if (fname like '%region.tbl%')
    {
      insert into region select * from region_f table option (from f);
      return;
    }
  if (fname like '%supplier.tbl.%')
    {
      insert into supplier select * from supplier_f table option (from f);
      return;
    }
  log_message (sprintf ('unrecognized file %s', f));
  signal ('BADFI', 'snb load does not know file type');
}


-- ld_dir ('/1d1/tpch100data', '%.tbl%', 'sql:tpch_cl_load (?)');
-- ld_dir ('/2d1/tpch100data', '%.tbl%', 'sql:tpch_cl_load (?)');

create procedure fill_load_list () {
       declare i int;       
       for (i := 1; i < length(cl_control(1, 'cl_host_map')); i := i + 2)
       	   cl_exec ('ld_dir (''/1s1/tpch100data'', ''%.tbl%'', ''sql:tpch_cl_load (?)'', txn => 0)', txn => 1, hosts => vector (i));
}

fill_load_list();



--cl_exec ('ld_dir (''/1s1/tpch1000data'', ''%.tbl%'', ''sql:tpch_cl_load (?)'', txn => 0)', txn => 1, hosts => vector (1));
--cl_exec ('ld_dir (''/home/tpch1000data'', ''%.tbl%'', ''sql:tpch_cl_load (?)'', txn => 0)', txn => 1, hosts => vector (3));
delete from load_list where ll_file like '%.u%';

