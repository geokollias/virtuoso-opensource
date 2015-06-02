aws s3 cp s3://opl-snb30database/snb30-1.db.gz /1s1/dbs/snb30-1.db.gz
aws s3 cp s3://opl-snb30database/snb30-2.db.gz /1s1/dbs/snb30-2.db.gz
aws s3 cp s3://opl-snb30database/snb30-3.db.gz /1s1/dbs/snb30-3.db.gz
aws s3 cp s3://opl-snb30database/snb30-4.db.gz /1s1/dbs/snb30-4.db.gz
aws s3 cp s3://opl-snb30database/snb30-5.db.gz /1s2/dbs/snb30-5.db.gz
aws s3 cp s3://opl-snb30database/snb30-6.db.gz /1s2/dbs/snb30-6.db.gz
aws s3 cp s3://opl-snb30database/snb30-7.db.gz /1s2/dbs/snb30-7.db.gz
aws s3 cp s3://opl-snb30database/snb30-8.db.gz /1s2/dbs/snb30-8.db.gz

gunzip /1s1/dbs/snb30-1.db.gz &
gunzip /1s1/dbs/snb30-2.db.gz &
gunzip /1s1/dbs/snb30-3.db.gz &
gunzip /1s1/dbs/snb30-4.db.gz &
gunzip /1s2/dbs/snb30-5.db.gz &
gunzip /1s2/dbs/snb30-6.db.gz &
gunzip /1s2/dbs/snb30-7.db.gz &
gunzip /1s2/dbs/snb30-8.db.gz &

wait

