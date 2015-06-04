aws s3 cp s3://opl-snb300dataset/snb300data.tar /1s2/snb300data.tar

tar -xvf /1s2/snb300data.tar -C /1s1
mv /1s1/1s1/snb300data /1s1
rmdir /1s1/1s1

rm -rf /1s2/snb300data.tar
