./start.sh

for ((i=0; i<12; i++ ))
do
    diff <(echo "status('cluster');" | isql 1201 2> /dev/null | head -n 9 | tail -n 1 | awk '{print $1, $3;}') <(echo "Cluster nodes,") > /dev/null 2> /dev/null
    if [ $? -eq 0 ]; then
	break
    fi
    echo "Waiting for servers to start..."
    sleep 10
done

echo "cl_wait_start(0);" | isql 1201

if [ $i -eq 12 ];
then echo "Servers are not online"; exit
fi

today=$(date +"%m/%d/%y %H:%M:%S")
echo "  LOADING TPC-H DATA  $today"
echo "  LOADING TPC-H DATA  $today" > load.output

isql 1201 < ldschema.sql >> load.output 2>> load.err
isql 1201 < ldfile.sql >> load.output 2>> load.err
isql 1201 < schema.sql >> load.output 2>> load.err
isql 1201 < clrefresh.sql >> load.output 2>> load.err
isql 1201 < clld.sql >> load.output 2>> load.err
isql 1201 < tpc-h.sql >> load.output 2>> load.err

number_of_machines=$(echo "select length(cl_control(1, 'cl_host_map'));" | isql 1201 | head -n 9 | tail -n 1)
number_of_machines=$(((number_of_machines-1)/2))
number_of_loaders=0
if [ $number_of_machines -eq 2 ];
then number_of_loaders=8;
fi
if [ $number_of_machines -eq 3 ];
then number_of_loaders=7;
fi
if [ $number_of_machines -eq 4 ];
then number_of_loaders=6;
fi
if [ $number_of_machines -eq 5 ];
then number_of_loaders=6;
fi
for ((i=0; i<$number_of_loaders; i++ ))
do
    echo "cl_exec('rdf_ld_srv()');" | isql 1201  >> load.output 2>> load.err &
    sleep 2
done

wait

echo "cl_exec('checkpoint');" | isql 1201  >> load.output  2>> load.err

today=$(date +"%m/%d/%y %H:%M:%S")
echo " FINISHED LOADING TPC-H DATA  $today"
echo " FINISHED LOADING TPC-H DATA  $today" >> load.output

cp load.output /home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h/
cp load.output /home/tpch1000r/1/