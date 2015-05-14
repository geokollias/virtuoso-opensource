tar -cvf /1s2/snb300data.tar /1s1/snb300data

aws s3 cp /1s2/snb300data.tar s3://opl-snb300dataset/snb300data.tar

rm -rf /1s2/snb300data.tar
