#!/bin/bash
#
#  $Id$
#
#  choose a server to run with
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

CLUSTER_RUN="NO"
export CLUSTER_RUN
WO_START=${WO_START-0}

SERVER=virtuoso-t
echo Using virtuoso configuration | tee -a $LOGFILE
CFGFILE=virtuoso.ini
DBFILE=virtuoso.db
DBLOGFILE=virtuoso.trx
DELETEMASK="virtuoso.lck $DBLOGFILE $DBFILE virtuoso.tdb virtuoso.ttr"
SRVMSGLOGFILE="virtuoso.log"
TESTCFGFILE=virtuoso-1111.ini
BACKUP_DUMP_OPTION=+backup-dump
CRASH_DUMP_OPTION=+crash-dump
FOREGROUND_OPTION=+foreground
LOCKFILE=virtuoso.lck
export FOREGROUND_OPTION LOCKFILE
export CFGFILE DBFILE DBLOGFILE DELETEMASK SRVMSGLOGFILE
export BACKUP_DUMP_OPTION CRASH_DUMP_OPTION TESTCFGFILE SERVER

. $HOME/binsrc/tests/tpc-h/test_fn.sh
LOGFILE=`pwd`/run.output
export LOGFILE

STATUS_FILE=status_server.output
export STATUS_FILE

TESTDIR=`pwd`
export TESTDIR

STATUS=0
export STATUS

TEMP_LOG=tmp.log
export TEMP_LOG

CLUSTER_CONF=cluster.conf
export CLUSTER_CONF

rm -rf $STATUS_FILE
rm -rf $TEMP_LOG
rm -rf $LOGFILE
rm -f $LOGFILE.*

SCALE=${1-1}
THREADS=${2-1}
LOOPS=${3-2}

if [ "$#" -gt 2 ]
then
    echo
    echo "TPC-H: RUN" 
    echo "SCALE:" $SCALE
    echo "STREAMS:" $THREADS
    echo "LOOPS:" $LOOPS
    echo
else
    echo
    echo "     run.sh scale streams loops"
    echo
    echo "     export WO_START=1 in order to run against existing instance " 
    echo
    echo "     Single server:"
    echo "     run.sh 1 5 2"
    exit
fi

PARSE_CONF ()
{
   LINE
   clusters=0
   for line in `cat $1` 
   do
       clusters=`expr $clusters + 1`
       DSN[$clusters]=$line
       echo "DSN"$clusters" = "$line 
   done
   export clusters
   export DSN
   LINE
}

waitAll ()
{
    cl=1
    while [ "$cl" -gt "0" ]
      do
        sleep 1
        cl=`ps -e | grep $1 | grep -v grep | wc -l`
      done
}

umask 000

# decide witch server options set to use based on the server's name

    DBGEN_PROG=dbgen
    QGEN_PROG=qgen
    LOAD_TBL_PROG=load_tbl



    BANNER "STARTED run.sh"

    BANNER "PREPARE TPC-H DATA"
    # XXX
    RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < tpc-h.sql

    cd dbgen
    make

    if [ ! -x $QGEN_PROG ]
    then
	LOG "No "$QGEN_PROG" executable compiled. Exiting"
	CHECK_LOG
	BANNER "COMPLETED (run.sh)"
	exit 0
    else
	LOG "The "$QGEN_PROG" is OK."
    fi

    seq=0
    while [ "$seq" -lt "$LOOPS" ]
    do
    seq=`expr $seq + 1`	

    BANNER "GENERATING TPC-H QUERIES"
    # Queryes for POWER test
    q_num=0
    while [ "$q_num" -lt "22" ]
    do
	q_num=`expr $q_num + 1`
	./$QGEN_PROG -s $SCALE $q_num > qset_0_"$q_num".sql
	chmod 777 qset_0_"$q_num".sql
	$ISQL $DSN dba dba EXEC="prepare_qs ('$TESTDIR/', $seq, $q_num, 0)" > /dev/null
	if [ "x$CLUSTER_RUN" = "xYES" ]
	then
	    ./$LOAD_TBL_PROG $DSN[1] dba dba -C qset_0_"$q_num".sql
	fi
    done    

    # Queryes for clients 
    q_count=0
    while [ "$q_count" -lt "$THREADS" ]
    do
	q_count=`expr $q_count + 1`
	q_num=0
	while [ "$q_num" -lt "22" ]
	do
	    q_num=`expr $q_num + 1`
	    ./$QGEN_PROG -s $SCALE -p $q_count $q_num > qset_"$q_count"_"$q_num".sql
	    chmod 777 qset_"$q_count"_"$q_num".sql
	    $ISQL $DSN dba dba EXEC="prepare_qs ('$TESTDIR/', $seq, $q_num, $q_count)" > /dev/null
	done    
    done    

    chmod +r qset*.sql
    BANNER "RUN TPC-H POWER TEST #$seq"

    ECHO "PASSED: START STREAM 0 RUN #$seq AT "$DSN "`date \"+%m/%d/%Y %H:%M:%S\"`"
    #$TESTDIR/run_rf.sh $ISQL $DSN 0 $seq 1 $LOGFILE 1 $THREADS #POWer flag ON
    $ISQL $DSN dba dba EXEC="rf_metric ($seq, 23, 0, $THREADS)" >> $LOGFILE
    $TESTDIR/run_one_client.sh $ISQL $DSN 0 $seq 1 $LOGFILE 1 $THREADS
    $ISQL $DSN dba dba EXEC="rf_metric ($seq, 24, 0, $THREADS)" >> $LOGFILE
    ECHO "PASSED: FINISH STREAM 0 RUN #$seq AT "$DSN "`date \"+%m/%d/%Y %H:%M:%S\"`"

    RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT 'EXEC="POWER_SIZE ($SCALE, $seq);"'  >> $LOGFILE 
    if test $STATUS -ne 0
    then
	LOG "***ABORTED: POWER_SIZE"
	exit 3
    else
	LOG "PASSED: POWER SIZE"
    fi

    isql $DSN PROMPT=OFF ERRORS=STDOUT "EXEC=print_power_test_results ('$TESTDIR/', $SCALE, $seq)" 
    if test $STATUS -ne 0
    then
	LOG "***ABORTED: PRINT_POWER_TEST_RESULTS"
	exit 3
    else
	LOG "PASSED: PRINT RESULTS"
    fi

    BANNER "END TPC-H POWER TEST"
    BANNER "TPC-H POWER TEST RESULTS"
    
    ECHO `cat $TESTDIR/power_test_results.txt | grep "Scale:"`
    ECHO
    ECHO "RF1 -" `cat $TESTDIR/power_test_results.txt | grep "Q23" | cut -f 3 -d " "` "sec."
    q_num=0
    while [ "$q_num" -lt "22" ]
    do
	q_num=`expr $q_num + 1`
	ECHO `cat $TESTDIR/power_test_results.txt | grep "Q$q_num "`
    done    
    ECHO "RF2 -" `cat $TESTDIR/power_test_results.txt | grep "Q24" | cut -f 3 -d " "` "sec."
    ECHO
    ECHO `cat $TESTDIR/power_test_results.txt | grep "TPC-H POWER:"`

    cat $TESTDIR/power_test_results.txt >> $LOGFILE
    
    BANNER "RUN TPC-H"

    cli_no=0
    while [ "$cli_no" -lt "$THREADS" ]
    do
	$TESTDIR/run_one_client.sh $ISQL $DSN $cli_no $seq $THREADS $LOGFILE 0 $THREADS &
	cli_no=`expr $cli_no + 1`
    done    
    $TESTDIR/run_rf.sh isql $DSN 0 $seq $THREADS $LOGFILE 0 $THREADS &

    wait
    waitAll run_one_client
    waitAll run_rf
    
    BANNER "FINISH TPC-H QUERYES"
    done # END LOOP

    RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT 'EXEC="check_results ($LOOPS, $THREADS);"' 
    if test $STATUS -ne 0
    then
	LOG "***ABORTED: CHECK RESULTS"
	exit 3
    else
	LOG "PASSED: CHECK RESULTS"
    fi

    cd ..

    seq=0
    while [ "$seq" -lt "$LOOPS" ]
    do
    seq=`expr $seq + 1`	

    isql $DSN PROMPT=OFF ERRORS=STDOUT "EXEC=print_stream_results ('$TESTDIR/', $SCALE, $THREADS, $seq)" 
    if test $STATUS -ne 0
    then
	LOG "***ABORTED: PRINT STREAM RESULTS"
	exit 3
    else
	LOG "PASSED: PRINT STREAM RESULTS"
    fi
    done
   
    TEMP_LOG='virtuoso.log'

    cat $STATUS_FILE >> $LOGFILE
    cat $TEMP_LOG | grep 'PL LOG: PASSED: ' | cut -b 18-120 >> $LOGFILE
    cat $TEMP_LOG | grep 'PL LOG: \*\*\*FAILED' | cut -b 18-120 >> $LOGFILE
    cat $TEMP_LOG | grep 'PL LOG: \*\*\*ABORTED' | cut -b 18-120 >> $LOGFILE
    grep "START STREAM " $LOGFILE >> $STATUS_FILE
    grep "FINISH STREAM " $LOGFILE >> $STATUS_FILE
#   rm -rf $TEMP_LOG

    CHECK_LOG
    BANNER "COMPLETED TPC-H RUN."
    #cat report.txt
