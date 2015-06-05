tar -cvf /1s2/snb100data.tar /1s1/snb100data

aws s3 cp /1s2/snb100data.tar s3://opl-snb100dataset/snb100data.tar

rm -rf /1s2/snb100data.tar
