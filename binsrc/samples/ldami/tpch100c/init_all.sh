./virtuoso +wait
isql 1111 < /home/virtuoso-opensource/binsrc/clcfg/clcfg.sql
isql 1111 < init.sql
chmod +x *.sh
./init.sh
./cpexe.sh
cp /home/virtuoso-opensource/binsrc/tests/tpc-h/dbgen/dists.dss .
cp /home/virtuoso-opensource/binsrc/tests/tpc-h/dbgen/dbgen .
