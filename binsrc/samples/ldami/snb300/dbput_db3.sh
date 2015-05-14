tar -zcvf /1s1/dbs/snb300-3.db.tar.gz /1s1/dbs/snb300-3.db
aws s3 cp /1s1/dbs/snb300-3.db s3://opl-snb300database/snb300-3.db
rm /1s1/dbs/snb300-3.db.tar.gz
