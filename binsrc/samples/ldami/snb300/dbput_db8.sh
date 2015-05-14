tar -zcvf /1s2/dbs/snb300-8.db.tar.gz /1s2/dbs/snb300-8.db
aws s3 cp /1s2/dbs/snb300-8.db s3://opl-snb300database/snb300-8.db
rm /1s2/dbs/snb300-8.db.tar.gz
