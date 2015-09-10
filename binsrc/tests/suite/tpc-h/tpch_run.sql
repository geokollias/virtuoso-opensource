#line 3 "tpc-h/15.sql"
create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= cast ('1996-01-01' as date)
		and l_shipdate < dateadd ('month', 3, cast ('1996-01-01' as date))
	group by
		l_suppkey
;
echo both $if $equ $state "OK" "PASSED" "***FAILED";
echo both ": #line 3 tpch/15.sql
create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= cast (1996-01-01 as date)
		and l_shipdate < dateadd (month, 3, cast (1996-01-01 as date))
	group by
		l_suppkey
\n";

qt_check_dir ('tpc-h');

#line 38 "tpch/15.sql"
drop view revenue0
;
echo both $if $equ $state "OK" "PASSED" "***FAILED";
echo both ": #line 38 tpc-h/15.sql
drop view revenue0
\n";
