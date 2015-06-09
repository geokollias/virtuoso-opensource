cd /home
for f in snb100 snb300 snbval spb1g spb256 spbval tpch100 tpch1000c tpch100c tpch300
do
    if [ ! -d /home/virtuoso-opensource/binsrc/samples/ldami/$f ]
    then
	echo "$f does not exists in vos checkout" 
	exit 1
    fi
    echo $f
    cp -R /home/virtuoso-opensource/binsrc/samples/ldami/$f .
    cd $f
    ln -s /home/virtuoso-opensource/binsrc/virtuoso/virtuoso-t .
    cd ..
done
for f in tpch100 tpch1000c tpch100c tpch300
do
    cd $f
    ln -s /home/ec2-user/exe virtuoso
    cd ..
done
cp -R /home/virtuoso-opensource/binsrc/samples/ldami/ec2-user/* /home/ec2-user/
sudo cp /home/virtuoso-opensource/binsrc/samples/ldami/motd /etc/motd
cp /home/virtuoso-opensource/binsrc/tests/isql /home/virtuoso-opensource/binsrc/tests/tpc-h/
