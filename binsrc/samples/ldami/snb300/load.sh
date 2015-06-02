/home/virtuoso-opensource/binsrc/virtuoso/virtuoso-t +wait

today=$(date)
starttime=$(date +%s)

echo "Start of Database Load: $today" > load.out

isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldschema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldfile.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/schema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ld.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/procedures.sql

echo "checkpoint;" | isql 1210

endtime=$(date +%s)
today=$(date)

let secs=endtime-starttime

h=$(( secs / 3600 ))
m=$(( ( secs / 60 ) % 60 ))
s=$(( secs % 60 ))

echo "End of Database Load: $today" >> load.out
echo "Database Load Time: $h hours $m minutes $s seconds" >> load.out

