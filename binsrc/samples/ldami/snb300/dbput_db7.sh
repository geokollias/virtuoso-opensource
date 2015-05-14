tar -zcvf /1s2/dbs/snb300-7.db.tar.gz /1s2/dbs/snb300-7.db
aws s3 cp /1s2/dbs/snb300-7.db s3://opl-snb300database/snb300-7.db
rm /1s2/dbs/snb300-7.db.tar.gz
