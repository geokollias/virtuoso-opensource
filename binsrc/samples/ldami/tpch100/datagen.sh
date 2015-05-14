cp -R /home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h/dbgen /1s1/
ln -s /1s1/dbgen .
export HOME=/home/ec2-user/virtuoso-opensource
/home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h/gen.sh 100 5 2
