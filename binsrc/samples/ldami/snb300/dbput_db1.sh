tar -zcvf /1s1/dbs/snb300-1.db.tar.gz /1s1/dbs/snb300-1.db
aws s3 cp /1s1/dbs/snb300-1.db s3://opl-snb300database/snb300-1.db
rm /1s1/dbs/snb300-1.db.tar.gz
