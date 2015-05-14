ssh localhost mkdir /1s2/tpch300data
scp dbgen localhost:/1s2/tpch300data
scp dists.dss localhost:/1s2/tpch300data
ssh localhost "(cd /1s2/tpch300data; ./dbgen -q -f -s 300 -U 14 
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 1)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 2)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 3)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 4)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 5)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 6)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 7)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 8)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 9)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 10)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 11)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 12)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 13)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 14)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 15)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 16)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 17)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 18)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 19)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 20)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 21)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 22)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 23)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 24)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 25)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 26)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 27)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 28)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 29)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 30)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 31)" &
sleep 1
ssh localhost "(cd /1s2/tpch300data; ./dbgen -z -q -f -s 300 -C 32 -S 32)" &
wait
ssh localhost chmod -R 777  /1s2/tpch300data
