tar -cvf /1s2/snb30data.tar /1s1/snb30data

aws s3 cp /1s2/snb30data.tar s3://opl-snb30dataset/snb30data.tar

rm -rf /1s2/snb30data.tar
