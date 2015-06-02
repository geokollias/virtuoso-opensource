aws s3 cp s3://opl-snb300database/snb300-1.db.gz /1s1/dbs/snb300-1.db.gz
aws s3 cp s3://opl-snb300database/snb300-2.db.gz /1s1/dbs/snb300-2.db.gz
aws s3 cp s3://opl-snb300database/snb300-3.db.gz /1s1/dbs/snb300-3.db.gz
aws s3 cp s3://opl-snb300database/snb300-4.db.gz /1s1/dbs/snb300-4.db.gz
aws s3 cp s3://opl-snb300database/snb300-5.db.gz /1s2/dbs/snb300-5.db.gz
aws s3 cp s3://opl-snb300database/snb300-6.db.gz /1s2/dbs/snb300-6.db.gz
aws s3 cp s3://opl-snb300database/snb300-7.db.gz /1s2/dbs/snb300-7.db.gz
aws s3 cp s3://opl-snb300database/snb300-8.db.gz /1s2/dbs/snb300-8.db.gz

gunzip /1s1/dbs/snb300-1.db.gz &
gunzip /1s1/dbs/snb300-2.db.gz &
gunzip /1s1/dbs/snb300-3.db.gz &
gunzip /1s1/dbs/snb300-4.db.gz &
gunzip /1s2/dbs/snb300-5.db.gz &
gunzip /1s2/dbs/snb300-6.db.gz &
gunzip /1s2/dbs/snb300-7.db.gz &
gunzip /1s2/dbs/snb300-8.db.gz &

wait

