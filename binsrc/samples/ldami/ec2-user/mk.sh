cd /1s1/
aws s3 cp s3://opl-spb256dataset/spb256db.tar .
cd /
tar xf /1s1/spb256db.tar 
gunzip /1s1/dbs/spb256.db.gz &
gunzip /1s2/dbs/spb256.db.gz &
wait
rm /1s1/spb256db.tar 
cd /home/spb256/
cp -R /home/ec2-user/ldbc_spb_bm_2.0/dist /1s2/spb256
cp *.properties /1s2/spb256
cd /1s2/spb256/
aws s3 cp s3://opl-spb256dataset/spb256data.tar .
tar xf spb256data.tar 
cd /home/spb256
