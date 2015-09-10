#  
#  $Id$
#
#  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
#  project.
#  
#  Copyright (C) 1998-2014 OpenLink Software
#  
#  This project is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; only version 2 of the License, dated June 1991.
#  
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#  General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#  
# 

# Note: dataset for TPC-H is generated and then cached in $VIRTUOSO_TEST/tpc-h/
# This dataset is not regenerated each time the tests are run.
# If you have some problems with these data or need to change TPC-H scale,
# you need to do "make cleann" in this directory for the datasets to be regenerated.

DSN=$PORT
. $VIRTUOSO_TEST/testlib.sh

testdir=`pwd`
LOGFILE=$testdir/ttpch.output
tpch_scale=1
#0.25
#rdfhgraph="http://example.com/tpcd"
rdfhgraph="urn:example.com:tpcd"
nrdfloaders=6
nsqlloaders=6
tpchdir=$VIRTUOSO_TEST/../tpc-h
dbgendir=$tpchdir/dbgen

BANNER "STARTED TPC-H/RDF-H functional tests"
NOLITE
if [ "z$NO_TPCH" != "z" ]
then
   exit
fi 

STOP_SERVER $PORT
mem=`sysctl hw.memsize | cut -f 2 -d ' '`
if [ -f /proc/meminfo ]
then
mv virtuoso-1111.ini virtuoso-tmp.ini
NumberOfBuffers=`cat /proc/meminfo | grep "MemTotal" | awk '{ MLIM = 524288; m = int($2 / 4 / 8 / 2); if(m > MLIM) { m = MLIM; } print m; }'`
sed "s/NumberOfBuffers\s*=\s*2000/NumberOfBuffers         = $NumberOfBuffers/g" virtuoso-tmp.ini > virtuoso-1111.ini 
elif [ "z$mem" != "z" ]
then
mv virtuoso-1111.ini virtuoso-tmp.ini
NumberOfBuffers=`sysctl hw.memsize | awk '{ MLIM = 524288; m = int($2 / 4 / 8 / 1024 / 4); if(m > MLIM) { m = MLIM; } print m; }'`
sed -e "s/NumberOfBuffers.*=.*2000/NumberOfBuffers         = $NumberOfBuffers/g" virtuoso-tmp.ini > virtuoso-1111.ini 
fi
MAKECFG_FILE $TESTCFGFILE $PORT $CFGFILE
ln -sf $VIRTUOSO_TEST/tpc-h .
cd $testdir

# generate TPC-H
if [ ! -f $VIRTUOSO_TEST/tpc-h/lineitem.tbl ]; then
    RUN make -C $VIRTUOSO_TEST/tpc-h data
    if [ $STATUS -ne 0 ]; then
        LOG "***ABORTED: Generating TPC-H data"
        exit 1
    fi
    LOG "SQL data for TPC-H generated"
fi

START_SERVER $PORT 1000
cd $testdir

# load TPC-H data
ln -sf $VIRTUOSO_TEST/tpc-h dbgen
for i in cl*; do
	[ -d $i ] && ln -sf $VIRTUOSO_TEST/tpc-h $i/dbgen
done
RUN 
RUN $tpchdir/load.sh $tpch_scale
if [ $STATUS -ne 0 ]; then
    LOG "***ABORTED: Loading TPC-H data"
    exit 1
fi
LOG "SQL data for TPC-H loaded."

# run TPC-H qualification
ln -sf $VIRTUOSO_TEST/tpc-h .
RUN $ISQL $DSN PROMPT=OFF VERBOSE=OFF ERRORS=STDOUT <tpc-h/tpch_run.sql
if [ $STATUS -ne 0 ]; then
    LOG "***ABORTED: tpc-h/tpch_run.sql"
    exit 1
fi

SHUTDOWN_SERVER $PORT

CHECK_LOG
BANNER "COMPLETED SERIES OF TPC-H/RDF-H TESTS"
