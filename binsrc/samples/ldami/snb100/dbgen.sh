rm -rf /1s1/snb100data
mkdir /1s1/snb100data

rm -rf /1s1/hdfs/*

/home/ec2-user/hadoop-2.6.0/bin/hdfs namenode -format
/home/ec2-user/hadoop-2.6.0/sbin/start-dfs.sh
/home/ec2-user/hadoop-2.6.0/sbin/start-yarn.sh

rm -rf /home/ldbc_snb_datagen/params.ini

cp /home/snb100/params.ini /home/ldbc_snb_datagen/

/home/ldbc_snb_datagen/run.sh

/home/ec2-user/hadoop-2.6.0/bin/hdfs dfs -copyToLocal /user/ec2-user/social_network/* /1s1/snb100data

/home/ec2-user/hadoop-2.6.0/sbin/stop-yarn.sh
/home/ec2-user/hadoop-2.6.0/sbin/stop-dfs.sh

rm -rf /1s1/hdfs/*

chmod -R 777 /1s1/snb100data

cp -r /home/ldbc_snb_datagen/substitution_parameters/ /1s1/snb100data

mkdir /1s1/snb100data/updates

mv /1s1/snb100data/updateS* /1s1/snb100data/updates

#gunzip /1s1/snb100data/updates/updateStream_0_* &
#gunzip /1s1/snb100data/updates/updateStream_1_* &
#gunzip /1s1/snb100data/updates/updateStream_2_* &
#gunzip /1s1/snb100data/updates/updateStream_3_* &
#gunzip /1s1/snb100data/updates/updateStream_4_* &
#gunzip /1s1/snb100data/updates/updateStream_5_* &
#gunzip /1s1/snb100data/updates/updateStream_6_* &
#gunzip /1s1/snb100data/updates/updateStream_7_* &
#gunzip /1s1/snb100data/updates/updateStream_8_* &
#gunzip /1s1/snb100data/updates/updateStream_9_* &
#gunzip /1s1/snb100data/updates/updateStream_10_* &
#gunzip /1s1/snb100data/updates/updateStream_11_* &
#gunzip /1s1/snb100data/updates/updateStream_12_* &
#gunzip /1s1/snb100data/updates/updateStream_13_* &
#gunzip /1s1/snb100data/updates/updateStream_14_* &
#gunzip /1s1/snb100data/updates/updateStream_15_* &
#wait

chmod -R 777 /1s1/snb100data
