select sum (case when r_start - r_sched > 1000 then 1 else 0 end) as n_late, count (*) as cnt, sum (r_start - r_sched) as sum_delay, (r_start - (select min (r_start) from result_f)) / 10000 as wnd
from result_f group by wnd order by wnd;

select n_late * 100.0 / cnt, n_late, cnt from (select sum (case when r_start - r_sched > 1000 then 1 else 0 end) as n_late, count (*) as cnt from result_f) f;
