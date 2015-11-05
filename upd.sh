#
#

export SRC=/3d3/fx2
cd libsrc/Wi 
cp $SRC/libsrc/Wi/*.[chyl] .
cp $SRC/libsrc/Wi/*.awk .
cp $SRC/libsrc/Wi/*.jso .
cp $SRC/libsrc/Wi/*.sql .
cd ../Dk
cp $SRC/libsrc/Dk/*.[chyl] .
cd ../../binsrc/virtuoso 
cp $SRC/binsrc/virtuoso/*.[ch] .
cd ../..
cd binsrc/cached_resources/openlinksw-sparql
cp $SRC/binsrc/cached_resources/openlinksw-sparql/*.ttl .
cd ../../..
cd binsrc/cached_resources
cp $SRC/binsrc/cached_resources/*.c .
cd ../..
cd binsrc/dav 
#cp $SRC/binsrc/dav/* .
cd ../yacutia
#cp $SRC/binsrc/yacutia/* .
cd ../virtuoso
cp $SRC/binsrc/virtuoso/*.[ch] .
cd ../..
git diff -u > diff.txt

