select ps_partkey,
       value
from (
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
      ps_partkey
) tmp
where value > (
      	    select
		sum(ps_supplycost * ps_availqty) * 0.0001000000e-2
	    from
	    partsupp,
	    supplier,
	    nation
	    where
	         ps_suppkey = s_suppkey
   		 and s_nationkey = n_nationkey
      		 and n_name = 'GERMANY'

)
order by
      value desc
limit 10
;

