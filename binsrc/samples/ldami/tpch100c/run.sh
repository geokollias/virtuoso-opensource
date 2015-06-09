cd /home/ec2-user/virtuoso-opensource
. ./.profile
cd /home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h
export WO_START=1
export DSN=1201
export PORT=1201
./run.sh 100 5 2
cp report?.txt /home/tpch1000c
