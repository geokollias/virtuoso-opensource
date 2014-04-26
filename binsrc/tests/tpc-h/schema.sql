DROP TABLE LINEITEM;
DROP TABLE ORDERS;
DROP TABLE PARTSUPP;
DROP TABLE CUSTOMER;
DROP TABLE SUPPLIER;
DROP TABLE NATION;
DROP TABLE REGION;
DROP TABLE PART;

CREATE TABLE REGION (
    R_REGIONKEY  INTEGER NOT NULL,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152),
    PRIMARY KEY  (R_REGIONKEY)
    )
alter index REGION  on REGION partition cluster replicated
;


CREATE TABLE NATION (
    N_NATIONKEY  INTEGER NOT NULL,
    N_NAME 	 CHAR(25) NOT NULL,
    N_REGIONKEY  INTEGER NOT NULL,
    N_COMMENT    VARCHAR(152),
    PRIMARY KEY  (N_NATIONKEY)
    )
alter index NATION on NATION partition cluster replicated
;


CREATE TABLE PART (
    P_PARTKEY     INTEGER NOT NULL,
    P_NAME        VARCHAR(55) NOT NULL,
    P_MFGR        CHAR(25) NOT NULL,
    P_BRAND       CHAR(10) NOT NULL,
    P_TYPE        VARCHAR(25) NOT NULL,
    P_SIZE        INTEGER NOT NULL,
    P_CONTAINER   CHAR(10) NOT NULL,
    P_RETAILPRICE double precision NOT NULL,
    P_COMMENT     VARCHAR(23) NOT NULL,
    PRIMARY KEY   (P_PARTKEY)
    )
alter index PART on PART partition (P_PARTKEY int (0hexffff00))
;

CREATE TABLE SUPPLIER (
    S_SUPPKEY     INTEGER NOT NULL,
    S_NAME        CHAR(25) NOT NULL,
    S_ADDRESS     VARCHAR(40) NOT NULL,
    S_NATIONKEY   INTEGER NOT NULL,
    S_PHONE       CHAR(15) NOT NULL,
    S_ACCTBAL     double precision NOT NULL,
    S_COMMENT     VARCHAR(101) NOT NULL,
    PRIMARY KEY   (S_SUPPKEY)
    )
alter index SUPPLIER on SUPPLIER partition (S_SUPPKEY int (0hexffff00))
;

CREATE TABLE PARTSUPP (
    PS_PARTKEY     INTEGER NOT NULL,
    PS_SUPPKEY     INTEGER NOT NULL,
    PS_AVAILQTY    INTEGER NOT NULL,
    PS_SUPPLYCOST  double precision NOT NULL,
    PS_COMMENT     VARCHAR (199) NOT NULL,
    PRIMARY KEY    (PS_PARTKEY, PS_SUPPKEY)
    )
alter index PARTSUPP on PARTSUPP partition (PS_PARTKEY int (0hexffff00))
;

CREATE TABLE CUSTOMER (
    C_CUSTKEY     INTEGER NOT NULL,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       CHAR(15) NOT NULL,
    C_ACCTBAL     double precision   NOT NULL,
    C_MKTSEGMENT  CHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL,
    PRIMARY KEY   (C_CUSTKEY)
    )
alter index CUSTOMER on CUSTOMER partition (C_CUSTKEY int (0hexffff00))
;

CREATE TABLE ORDERS (
    O_ORDERKEY       INTEGER NOT NULL,
    O_CUSTKEY        INTEGER NOT NULL,
    O_ORDERSTATUS    CHAR(1) NOT NULL,
    O_TOTALPRICE     double precision NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  CHAR(15) NOT NULL,
    O_CLERK          CHAR(15) NOT NULL,
    O_SHIPPRIORITY   INTEGER NOT NULL,
    O_COMMENT        VARCHAR(79) NOT NULL,
    PRIMARY KEY      (O_ORDERKEY)
    )
alter index ORDERS on ORDERS partition (O_ORDERKEY int (0hexffff00))
;

CREATE TABLE LINEITEM (
    L_ORDERKEY    INTEGER NOT NULL,
    L_PARTKEY     INTEGER NOT NULL,
    L_SUPPKEY     INTEGER NOT NULL,
    L_LINENUMBER  INTEGER NOT NULL,
    L_QUANTITY    double precision NOT NULL,
    L_EXTENDEDPRICE  double precision NOT NULL,
    L_DISCOUNT    double precision NOT NULL,
    L_TAX         double precision NOT NULL,
    L_RETURNFLAG  CHAR(1) NOT NULL,
    L_LINESTATUS  CHAR(1) NOT NULL,
    L_SHIPDATE    DATE NOT NULL,
    L_COMMITDATE  DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT CHAR(25) NOT NULL,
    L_SHIPMODE     CHAR(10) NOT NULL,
    L_COMMENT      VARCHAR(44) NOT NULL,
    PRIMARY KEY    (L_ORDERKEY, L_LINENUMBER)
    )
alter index LINEITEM on LINEITEM partition (L_ORDERKEY int (0hexffff00))
;





CREATE INDEX O_CK ON ORDERS (O_CUSTKEY) partition (O_custkey int (0hexffff00));







