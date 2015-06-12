ssh -i mspasic.pem a1 mkdir /data0/tpch100data
scp -i mspasic.pem dbgen a1:/data0/tpch100data
scp -i mspasic.pem dists.dss a1:/data0/tpch100data
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -U 18 -i 4 -d 4)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 1)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 2)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 3)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 4)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 5)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 6)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 7)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 8)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 9)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 10)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 11)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 12)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 13)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 14)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 15)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 16)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 17)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 18)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 19)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 20)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 21)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 22)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 23)" &
sleep 1
ssh -i mspasic.pem a1 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 24)" &
ssh -i mspasic.pem a2 mkdir /data0/tpch100data
scp -i mspasic.pem dbgen a2:/data0/tpch100data
scp -i mspasic.pem dists.dss a2:/data0/tpch100data
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 25)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 26)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 27)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 28)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 29)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 30)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 31)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 32)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 33)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 34)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 35)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 36)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 37)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 38)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 39)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 40)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 41)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 42)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 43)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 44)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 45)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 46)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 47)" &
sleep 1
ssh -i mspasic.pem a2 "(cd /data0/tpch100data; ./dbgen -q -f -s 100 -C 48 -S 48)" &
wait
ssh -i mspasic.pem a1 chmod -R 777  /data0/tpch100data
ssh -i mspasic.pem a2 chmod -R 777  /data0/tpch100data
ssh -i mspasic.pem a2 rm  /data0/tpch100data/nation.tbl /data0/tpch100data/region.tbl
ssh -i mspasic.pem a1 "scp -i mspasic.pem /data0/tpch100data/*.u*.3 a2:/data0/tpch100data "
ssh -i mspasic.pem a1 "scp -i mspasic.pem /data0/tpch100data/*.u*.4 a2:/data0/tpch100data "
