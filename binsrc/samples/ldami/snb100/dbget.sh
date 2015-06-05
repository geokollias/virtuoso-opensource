aws s3 cp s3://opl-snb100dataset/snb100data.tar /1s2/snb100data.tar

tar -xvf /1s2/snb100data.tar -C /1s1
mv /1s1/1s1/snb100data /1s1
rmdir /1s1/1s1

rm -rf /1s2/snb100data.tar
