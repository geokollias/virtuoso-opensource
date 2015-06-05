aws s3 cp s3://opl-snb100database/snb100-1.db.gz /1s1/dbs/snb100-1.db.gz
aws s3 cp s3://opl-snb100database/snb100-2.db.gz /1s1/dbs/snb100-2.db.gz
aws s3 cp s3://opl-snb100database/snb100-3.db.gz /1s1/dbs/snb100-3.db.gz
aws s3 cp s3://opl-snb100database/snb100-4.db.gz /1s1/dbs/snb100-4.db.gz
aws s3 cp s3://opl-snb100database/snb100-5.db.gz /1s2/dbs/snb100-5.db.gz
aws s3 cp s3://opl-snb100database/snb100-6.db.gz /1s2/dbs/snb100-6.db.gz
aws s3 cp s3://opl-snb100database/snb100-7.db.gz /1s2/dbs/snb100-7.db.gz
aws s3 cp s3://opl-snb100database/snb100-8.db.gz /1s2/dbs/snb100-8.db.gz

gunzip /1s1/dbs/snb100-1.db.gz &
gunzip /1s1/dbs/snb100-2.db.gz &
gunzip /1s1/dbs/snb100-3.db.gz &
gunzip /1s1/dbs/snb100-4.db.gz &
gunzip /1s2/dbs/snb100-5.db.gz &
gunzip /1s2/dbs/snb100-6.db.gz &
gunzip /1s2/dbs/snb100-7.db.gz &
gunzip /1s2/dbs/snb100-8.db.gz &

wait

