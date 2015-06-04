#!/bin/sh

PORT=1112
SPB=/home/ec2-user/ldbc_spb_bm_2.0
TDIR=`pwd`

cp -R /home/ec2-user/ldbc_spb_bm_2.0/dist /1s2/spb1g
cp *.properties /1s2/spb1g

isql $PORT dba dba ns.sql
#isql $PORT dba dba spbcset.sql #enable when using feature/emergent branch
isql $PORT dba dba exec="ld_dir_all ('$SPB/dist/data/ontologies', '*.ttl*', 'ldbc-onto')"
isql $PORT dba dba exec="ld_dir_all ('$SPB/dist/data/datasets', '*.ttl*', 'ldbc')"
for i in {1..8}; do isql $PORT dba dba exec="rdf_loader_run()" & done
wait
isql $PORT dba dba exec="rdfs_rule_set ('ldbc', 'ldbc-onto')"
cd /1s2/spb1g
java -jar semantic_publishing_benchmark-basic-virtuoso.jar spb1ggen.properties 
isql $PORT dba dba exec="ld_dir_all ('/1s2/spb1g/generated', '*.nq*', 'ldbc')"
for i in {1..32}; do isql $PORT dba dba exec="rdf_loader_run()" & done
sleep 10
isql $PORT dba dba exec="VT_INC_INDEX_DB_DBA_RDF_OBJ()" &
wait
isql $PORT dba dba exec="rdf_geo_fill()"
isql $PORT dba dba exec='grant "SPARQL_UPDATE" to "SPARQL"'
isql $PORT dba dba exec="checkpoint"
java -jar semantic_publishing_benchmark-basic-virtuoso.jar spb1gqgen.properties 
