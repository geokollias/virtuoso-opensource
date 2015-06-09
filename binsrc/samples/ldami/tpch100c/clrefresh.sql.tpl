

create procedure rf_files (in file varchar, in nth int)
{
  declare files, name any;
  declare n_files, inx int;
  files := vector (XXX);
 n_files := length (files);
  for (inx := 0; inx < n_files; inx := inx + 2)
    {
    name := files[inx];
      files[inx] := replace (replace (name, 'FILE', file), 'NTH', cast (nth as varchar));
    }
  return files;
}




create procedure cl_rf1 (in dir varchar, in nth int, in no_pk int := 0, in rb int := 0, in qp int := null)
{
  declare _b_time any;
  _b_time := msec_time();
  insert into orders select * from orders_f table option (from rf_files ('orders.tbl', nth));
  insert into lineitem select * from lineitem_f table option (from rf_files ('lineitem.tbl', nth));
  if (rb)
    rollback work;
  else
    commit work;
  return msec_time() - _b_time;
}
;


create procedure del_batch (in d_orderkey int)
{
  vectored;
  delete from lineitem where l_orderkey = d_orderkey;
  delete from orders where o_orderkey = d_orderkey;
}
;

create procedure cl_rf2 (in dir varchar, in nth int, in rb int := 0)
{
  declare cnt int;
  declare _b_time any;
  _b_time := msec_time();
  cnt := (select count (del_batch (d_orderkey)) from delete_f table option (from rf_files ('delete', nth)));
  if (rb)
    rollback work;
  else 
    commit work;
  return msec_time() - _b_time;
}
;


