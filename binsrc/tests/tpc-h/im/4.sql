
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1993-07-01'
	and o_orderdate < cast ('1993-07-01' as timestamp) + INTERVAL 3 MONTHS
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
;
