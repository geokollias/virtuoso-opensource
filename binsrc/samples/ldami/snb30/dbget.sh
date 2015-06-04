aws s3 cp s3://opl-snb30dataset/snb30data.tar /1s2/snb30data.tar

tar -xvf /1s2/snb30data.tar -C /1s1
mv /1s1/1s1/snb30data /1s1
rmdir /1s1/1s1

rm -rf /1s2/snb30data.tar
