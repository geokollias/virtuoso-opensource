tar -zcvf /1s2/dbs/snb300-5.db.tar.gz /1s2/dbs/snb300-5.db
aws s3 cp /1s2/dbs/snb300-5.db s3://opl-snb300database/snb300-5.db
rm /1s2/dbs/snb300-5.db.tar.gz
