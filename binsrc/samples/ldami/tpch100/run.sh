cd /home/ec2-user/virtuoso-opensource
. ./.profile
cd /home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h
sh run.sh 100 5 2
cp report?.txt /home/tpch100
