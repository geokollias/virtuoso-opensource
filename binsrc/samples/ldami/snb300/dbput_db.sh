gzip -c /1s1/dbs/snb300-1.db > /1s1/dbs/snb300-1.db.gz &
gzip -c /1s1/dbs/snb300-2.db > /1s1/dbs/snb300-2.db.gz &
gzip -c /1s1/dbs/snb300-3.db > /1s1/dbs/snb300-3.db.gz &
gzip -c /1s1/dbs/snb300-4.db > /1s1/dbs/snb300-4.db.gz &
gzip -c /1s2/dbs/snb300-5.db > /1s2/dbs/snb300-5.db.gz &
gzip -c /1s2/dbs/snb300-6.db > /1s2/dbs/snb300-6.db.gz &
gzip -c /1s2/dbs/snb300-7.db > /1s2/dbs/snb300-7.db.gz &
gzip -c /1s2/dbs/snb300-8.db > /1s2/dbs/snb300-8.db.gz &

wait

aws s3 cp /1s1/dbs/snb300-1.db.gz s3://opl-snb300database/snb300-1.db.gz 
aws s3 cp /1s1/dbs/snb300-2.db.gz s3://opl-snb300database/snb300-2.db.gz 
aws s3 cp /1s1/dbs/snb300-3.db.gz s3://opl-snb300database/snb300-3.db.gz 
aws s3 cp /1s1/dbs/snb300-4.db.gz s3://opl-snb300database/snb300-4.db.gz 
aws s3 cp /1s2/dbs/snb300-5.db.gz s3://opl-snb300database/snb300-5.db.gz 
aws s3 cp /1s2/dbs/snb300-6.db.gz s3://opl-snb300database/snb300-6.db.gz 
aws s3 cp /1s2/dbs/snb300-7.db.gz s3://opl-snb300database/snb300-7.db.gz 
aws s3 cp /1s2/dbs/snb300-8.db.gz s3://opl-snb300database/snb300-8.db.gz 

rm /1s1/dbs/snb300-1.db.gz 
rm /1s1/dbs/snb300-2.db.gz 
rm /1s1/dbs/snb300-3.db.gz 
rm /1s1/dbs/snb300-4.db.gz 
rm /1s2/dbs/snb300-5.db.gz 
rm /1s2/dbs/snb300-6.db.gz 
rm /1s2/dbs/snb300-7.db.gz 
rm /1s2/dbs/snb300-8.db.gz
