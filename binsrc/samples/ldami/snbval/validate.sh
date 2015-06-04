/home/virtuoso-opensource/binsrc/virtuoso/virtuoso-t +wait

isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldschema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldfile.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/schema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ld.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/procedures.sql

echo 'checkpoint;' | isql 1210
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/procedures.sql

echo 'checkpoint;' | isql 1210

java -Xmx8g -cp /home/ldbc_driver/target/jeeves-0.2-SNAPSHOT.jar:/home/ldbc_snb_implementations/interactive/virtuoso/java/virtuoso/target/virtuoso-0.0.1-SNAPSHOT.jar:/home/ldbc_snb_implementations/interactive/virtuoso/java/virtuoso/virtjdbc4_lib/virtjdbc4.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.ldbc.snb.interactive.db.VirtuosoDb  -P ./ldbc_snb_interactive.properties -P ./ldbc_driver_default.properties -P virtuoso_configuration.properties -vdb ./validation_params.csv

echo 'shutdown();' | isql 1210

rm -f /1s1/dbs/snbval* /1s2/dbs/snbval*

