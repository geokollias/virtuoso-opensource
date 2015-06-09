; virtuoso.ini
;
; Configuration file for the OpenLink Virtuoso VDBMS Server
;
;
; Database setup
;
[Database]
DatabaseFile    = virtuoso.db
TransactionFile = { sprintf ('/1s2/dbs/virtuoso-%d.trx', c ('proc')) };
ErrorLogFile    = virtuoso.log
ErrorLogLevel   = 7
Syslog          = 0
TempStorage     = TempDatabase
FileExtend      = 200
Striping        = 0

[TempDatabase]
DatabaseFile    = virtuoso.tdb
TransactionFile = virtuoso.ttr
FileExtend      = 200

;
; Server parameters
;
[Parameters]
Affinity                   = { case when mod (c ('proc'), 2) = 1 then '0-7, 16-23' else '8-15, 24-31' end}
;Affinity                   = { case when mod (c ('proc'), 2) = 1 then '1-5, 12-17' else '7-11, 18-23' end}
;ListenerAffinity           = { case when mod (c ('proc'), 2) = 1 then '0' else '6' end}
ColumnStore = 1
ServerPort                 = {1200 + c('proc')}
ServerThreads              = 100
CheckpointSyncMode         = 0
CheckpointInterval         = 0
NumberOfBuffers            = 10000000
MaxDirtyBuffers            = 4000000
MaxCheckpointRemap         = 2500000
DefaultIsolation           = 2
UnremapQuota               = 0
AtomicDive                 = 1
PrefixResultNames          = 0
CaseMode                   = 2
DisableMtWrite             = 0
;MinAutoCheckpointSize	= 4000000
;CheckpointAuditTrail	= 1
DirsAllowed                = /
PLDebug                    = 0
TestCoverage               = cov.xml
;Charset=ISO-8859-1
ResourcesCleanupInterval   = 1
ThreadCleanupInterval      = 1
TransactionAfterImageLimit = 1500000000
FDsPerFile                 = 4
;StopCompilerWhenXOverRunTime = 1
MaxMemPoolSize             = 40000000
ThreadsPerQuery            = 16
AsyncQueueMaxThreads       = 32
IndexTreeMaps              = 64

[VDB]
VDBDisconnectTimeout = 1000
ArrayOptimization    = 2
NumArrayParameters   = 10

[Client]
SQL_QUERY_TIMEOUT  = 0
SQL_TXN_TIMEOUT    = 0
SQL_ROWSET_SIZE    = 10
SQL_PREFETCH_BYTES = 12000

[AutoRepair]
BadParentLinks = 0
BadDTP         = 0

[Replication]
ServerName   = HOSTNAME
ServerEnable = 1

[HTTPServer]
ServerPort                  = { 8600 +  c ('proc')}
ServerRoot                  = vsp
ServerThreads               = 40
MaxKeepAlives               = 10
KeepAliveTimeout            = 10
MaxCachedProxyConnections   = 10
ProxyConnectionCacheTimeout = 10
DavRoot                     = DAV
HTTPLogFile                 = logs/http.log

[URIQA]
DefaultHost = HOSTNAME

[SPARQL]
;ExternalQuerySource = 1
;ExternalXsltSource = 1
ResultSetMaxRows   = 100000
LabelInferenceName = facets
ImmutableGraphs    = inference-graphs, *
ShortenLongURIs    = 1
;EnablePstats = 0
