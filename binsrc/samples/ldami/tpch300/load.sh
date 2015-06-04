export HOME=/home/ec2-user/virtuoso-opensource
export PATH=$HOME/binsrc/tests:$PATH
LOGFILE=/home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h/load.output
tpchdir=/home/ec2-user/virtuoso-opensource/binsrc/tests/tpc-h
. $HOME/binsrc/tests/tpc-h/test_fn.sh
LOG "=  LOADING TPC-H DATA " "`date \"+%m/%d/%Y %H:%M:%S\"`"
RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < $tpchdir/schema.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: schema.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < $tpchdir/ldschema.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ldschema.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < $tpchdir/tpc-h.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: tpc-h.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < $tpchdir/ldfile.sql
if test $STATUS -ne 0
then
    LOG "***ABORTED: ldschema.sql"
    exit 3
fi

RUN $ISQL $DSN PROMPT=OFF ERRORS=STDOUT < clld.sql
LOG "=  FINISHED LOADING TPC-H DATA " "`date \"+%m/%d/%Y %H:%M:%S\"`"
