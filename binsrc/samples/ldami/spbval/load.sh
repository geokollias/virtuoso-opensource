#!/bin/sh

PORT=${1-1605}
SPB=${2-/home/ec2-user/ldbc_spb_bm_2.0}
TDIR=`pwd`

isql $PORT dba dba ns.sql
isql $PORT dba dba exec="ld_dir_all ('$SPB/dist/data/ontologies', '*.ttl', 'ldbc-onto')"
isql $PORT dba dba exec="ld_dir_all ('$SPB/dist/data/datasets', '*.ttl', 'ldbc')"
for i in {1..8}; do isql $PORT dba dba exec="rdf_loader_run()" & done
wait
isql $PORT dba dba exec="rdfs_rule_set ('ldbc', 'ldbc-onto')"
isql $PORT dba dba exec="ld_dir_all ('$SPB/dist/data/validation', '*.nq', 'ldbc')"
for i in {1..16}; do isql $PORT dba dba exec="rdf_loader_run()" & done
isql $PORT dba dba exec="VT_INC_INDEX_DB_DBA_RDF_OBJ()" &
wait
isql $PORT dba dba exec="rdf_geo_fill()"
isql $PORT dba dba exec='grant "SPARQL_UPDATE" to "SPARQL"'
isql $PORT dba dba exec="checkpoint"
