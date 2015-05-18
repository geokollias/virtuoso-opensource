echo "LDBC SNB Interactive Results"
echo ""

today=$(date +"%B %d, %Y")
echo -e "Report Date: \t\t\t$today"
echo ""

scalefactor=$(grep 'ldbc.snb.datagen.generator.scaleFactor:snb.interactive.' params.ini | sed 's/ldbc.snb.datagen.generator.scaleFactor:snb.interactive.//')
echo -e "Database Scale: \t\t$scalefactor"
echo ""

totalsize=$(grep Segment virtuoso.ini | egrep -o [^[:space:]]*[.]db | xargs du -ch | tail -n 1 | sed 's/total//')
echo -e "Total Data Storage/Database Size: \t$totalsize"
echo ""

cat load.out

egrep  'Operation Count' run.out | tail -n 1 | sed -r 's/\s+/ /g' | sed 's/:/:\t\t/'
egrep  'Operation Count' run.out | head -n 2 | tail -n 1 | sed 's/Operation/Warmup/' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t\t/'
egrep  'Worker Threads|Time Compression Ratio|Spinner Sleep Duration' run.out | head -n 3 | sed -r 's/^\s+//g' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t/'
egrep  '^Duration' run.out | head -n 1 | sed 's/^/Warmup /' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t/'
egrep  '^Duration' run.out | head -n 2 | tail -n 1 | sed 's/^/Run /' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t\t/'
egrep  '^Throughput' run.out | head -n 1 | sed 's/^/Warmup /' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t/'
egrep  '^Throughput' run.out | head -n 2 | tail -n 1 | sed 's/^/Run /' | sed -r 's/\s+/ /g' | sed 's/:/:\t\t\t/'
egrep  '^Start Time' run.out
egrep  '^Finish Time' run.out
echo ""
echo ""

cat LDBC-results.json | sed -e 's/[{}"]/''/g' | awk -v k="text" '{n=split($0,a,","); for (i=1; i<=n; i++) print a[i]}' | sed 's/]//' | egrep 'Ldbc|^count|mean|min|max|50th|90th|95th|99th' | sed -e 's/.*://' | tr '\n' '\t' | sed 's/Ldbc/\nLdbc/g' | tail -n +2 | awk 'BEGIN{printf("%-30s%12s%12s%12s%12s%12s%12s%12s%12s\n", "Query", "Count", "Mean", "Min", "Max", "50th_perc", "90th_perc", "95th_perc", "99th_perc");}{printf("%-30s%12d%12g%12d%12d%12d%12d%12d%12d\n", $1, $3, $4, $5, $6, $7, $8, $9, $10);}'
echo ""
echo ""

latequeries=$(awk -f run_validator.awk driver/results/LDBC-results_log.csv | head -n 1 | awk '{print $4;}')
echo "Percentage of late queries: $latequeries"
if [ $(bc  <<<  "$latequeries <= 5.0") == 1 ]
then echo "VALID RUN"
else echo "INVALID RUN"
fi
echo ""


