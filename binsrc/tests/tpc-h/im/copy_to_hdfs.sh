hdfs dfs -mkdir /user/impala/tpch100data
hdfs dfs -mkdir /user/impala/tpch100data/region
hdfs dfs -put /data0/tpch100data/region.tbl /user/impala/tpch100data/region
hdfs dfs -mkdir /user/impala/tpch100data/supplier
hdfs dfs -put /data0/tpch100data/supplier.tbl.* /user/impala/tpch100data/supplier
hdfs dfs -mkdir /user/impala/tpch100data/partsupp
hdfs dfs -put /data0/tpch100data/partsupp.tbl.* /user/impala/tpch100data/partsupp
hdfs dfs -mkdir /user/impala/tpch100data/part
hdfs dfs -put /data0/tpch100data/part.tbl.* /user/impala/tpch100data/part
hdfs dfs -mkdir /user/impala/tpch100data/orders
hdfs dfs -put /data0/tpch100data/orders.tbl.* /user/impala/tpch100data/orders
hdfs dfs -mkdir /user/impala/tpch100data/nation
hdfs dfs -put /data0/tpch100data/nation.tbl /user/impala/tpch100data/nation
hdfs dfs -mkdir /user/impala/tpch100data/customer
hdfs dfs -put /data0/tpch100data/customer.tbl.* /user/impala/tpch100data/customer
hdfs dfs -mkdir /user/impala/tpch100data/lineitem
hdfs dfs -put /data0/tpch100data/lineitem.tbl.* /user/impala/tpch100data/lineitem
