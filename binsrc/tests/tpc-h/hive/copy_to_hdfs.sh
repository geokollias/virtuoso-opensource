hdfs dfs -mkdir -p /mirko/tpch100data
hdfs dfs -mkdir /mirko/tpch100data/region
hdfs dfs -put region.tbl /mirko/tpch100data/region
hdfs dfs -mkdir /mirko/tpch100data/supplier
hdfs dfs -put supplier.tbl.* /mirko/tpch100data/supplier
hdfs dfs -mkdir /mirko/tpch100data/partsupp
hdfs dfs -put partsupp.tbl.* /mirko/tpch100data/partsupp
hdfs dfs -mkdir /mirko/tpch100data/part
hdfs dfs -put part.tbl.* /mirko/tpch100data/part
hdfs dfs -mkdir /mirko/tpch100data/orders
hdfs dfs -put orders.tbl.* /mirko/tpch100data/orders
hdfs dfs -mkdir /mirko/tpch100data/nation
hdfs dfs -put nation.tbl /mirko/tpch100data/nation
hdfs dfs -mkdir /mirko/tpch100data/customer
hdfs dfs -put customer.tbl.* /mirko/tpch100data/customer
hdfs dfs -mkdir /mirko/tpch100data/lineitem
hdfs dfs -put lineitem.tbl.* /mirko/tpch100data/lineitem
