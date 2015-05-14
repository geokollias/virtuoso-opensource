tar -zcvf /1s1/dbs/snb300-2.db.tar.gz /1s1/dbs/snb300-2.db
aws s3 cp /1s1/dbs/snb300-2.db s3://opl-snb300database/snb300-2.db
rm /1s1/dbs/snb300-2.db.tar.gz
