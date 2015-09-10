-- run tpc-h queries
prof_enable(1);
select 'tpc-h start';
load tpc-h/1.sql;
load tpc-h/2.sql;
load tpc-h/3.sql;
load tpc-h/4.sql;
load tpc-h/5.sql;
load tpc-h/6.sql;
load tpc-h/7.sql;
load tpc-h/8.sql;
load tpc-h/9.sql;
load tpc-h/10.sql;
load tpc-h/11.sql;
load tpc-h/12.sql;
load tpc-h/13.sql;
load tpc-h/14.sql;
load tpc-h/15.sql;
load tpc-h/16.sql;
load tpc-h/17.sql;
load tpc-h/18.sql;
load tpc-h/19.sql;
load tpc-h/20.sql;
load tpc-h/21.sql;
load tpc-h/22.sql;
prof_enable(0);

-- generate test
load testgen.sql;
testgen('tpc-h', '%tpc-h start%');
