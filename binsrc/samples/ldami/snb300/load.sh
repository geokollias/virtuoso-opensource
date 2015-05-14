/home/virtuoso-opensource/binsrc/virtuoso/virtuoso-t +wait

isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldschema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ldfile.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/schema.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/ld.sql
isql 1210 < /home/ldbc_snb_implementations/interactive/virtuoso/scripts/procedures.sql

echo 'checkpoint;' | isql 1210

