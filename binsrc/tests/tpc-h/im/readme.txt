TPC-H 100 on Impala

In order to install Impala on Amazon cluster with 2 nodes (of type
r3.8xlarge), I have followed this instruction:
https://s3.amazonaws.com/quickstart-reference/cloudera/hadoop/latest/doc/Cloudera_EDH_on_AWS.pdf

You can find the screenshots of WebUI of EC2 Management Console, and
of Cloudera Manager, at the end of the installation. The critical
issue with HDFS-1 is Under-Replicated Blocks. (1,164 under replicated
blocks in the cluster. 1,166 total blocks in the cluster. Percentage
under replicated blocks: 99.83%. Critical threshold: 40.00%.)

I have impala deamon on both boxes, but when I execute any query from
impala-shell on any of these two boxes, I can notice that only impala
deamon on the first box is running, but the one on the other box is
not. This happens no matter from which machine I execute the query.

In this folder you can find a script for generating the dataset. It is
called dbgen.sh. You can regenerate the TPC-H dataset of scale factor
100. It will be generated in parallel, on both boxes. In this script,
you should change 'mspasic.pem' with your permission file, and
internal IP addresses (10.0.1.251 and 10.0.1.250) to yours. After
about 3 minutes, the datasets will be prepared in the
/data0/tpch100data folder on both machines.

I executed copy_to_hdfs.sh script as user 'impala' to copy the
generated csv files to hdfs. This has to be done on both machines,
while on the other one you shoud get some errors while copying and
making new directories (ignore them). This step will take less than 3
minutes on the first machine, and 4.5 on the second one.

I have created external tables from these csv files with ldschema.sql
file (impala-shell -f ldschema.sql), created parquet tables in the
database TPCH (impala-shell -f schema.sql), and loaded data into them
(impala-shell -f ld.sql). The execution of the last script took 47
minutes, while the majority of times went to COMPUTE STATS statements
(~28 minutes).

Everything is loaded, and that can be checked with count.sql and
count1.sql scripts. The output of them is in count.out and count1.out.


Then, I executed from impala-shell 22 of TPC-H queries (1.sql, 2.sql,
..., 22.sql). The standard output and error are redirected for each
query are redirected to number_of_query.out and number_of_query.err
(respectively). In the err files you can find the total execution
times of queries, while in the out files you can find the results of
the queries. Also, you can find number of returned rows, execution
time and CPU load per query in the file execution.txt. There should be
no query returing 0 rows, while this is the case with query XXX, XXX,
XXX and XXX.

The query plans of all of these queries can be found in .expl files.
