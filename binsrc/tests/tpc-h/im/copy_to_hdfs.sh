hdfs dfs -mkdir /user/impala/tpch6data
hdfs dfs -mkdir /user/impala/tpch6data/region
hdfs dfs -put region.csv /user/impala/tpch6data/region
hdfs dfs -mkdir /user/impala/tpch6data/supplier
hdfs dfs -put supplier.csv /user/impala/tpch6data/supplier
hdfs dfs -mkdir /user/impala/tpch6data/partsupp
hdfs dfs -put partsupp.csv /user/impala/tpch6data/partsupp
hdfs dfs -mkdir /user/impala/tpch6data/part
hdfs dfs -put part.csv /user/impala/tpch6data/part
hdfs dfs -mkdir /user/impala/tpch6data/orders
hdfs dfs -put orders.csv /user/impala/tpch6data/orders
hdfs dfs -mkdir /user/impala/tpch6data/nation
hdfs dfs -put nation.csv /user/impala/tpch6data/nation
hdfs dfs -mkdir /user/impala/tpch6data/customer
hdfs dfs -put customer.csv /user/impala/tpch6data/customer
hdfs dfs -mkdir /user/impala/tpch6data/lineitem
hdfs dfs -put lineitem.csv /user/impala/tpch6data/lineitem
