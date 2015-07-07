DROP TABLE IF EXISTS ORDERS;
DROP TABLE IF EXISTS PARTSUPP;
DROP TABLE IF EXISTS CUSTOMER;
DROP TABLE IF EXISTS SUPPLIER;
DROP TABLE IF EXISTS NATION;
DROP TABLE IF EXISTS REGION;
DROP TABLE IF EXISTS PART;
DROP TABLE IF EXISTS LINEITEM;

CREATE TABLE REGION (
 R_REGIONKEY INT,
 R_NAME CHAR(25),
 R_COMMENT VARCHAR(152)
 )
STORED AS ORC;


CREATE TABLE NATION (
 N_NATIONKEY INT,
 N_NAME CHAR(25),
 N_REGIONKEY INT,
 N_COMMENT VARCHAR(152)
 )
STORED AS ORC;


CREATE TABLE PART (
 P_PARTKEY INT,
 P_NAME VARCHAR(55),
 P_MFGR CHAR(25),
 P_BRAND CHAR(10),
 P_TYPE VARCHAR(25),
 P_SIZE INT,
 P_CONTAINER CHAR(10),
 P_RETAILPRICE DOUBLE,
 P_COMMENT VARCHAR(23)
 )
STORED AS ORC;

CREATE TABLE SUPPLIER (
 S_SUPPKEY INT,
 S_NAME CHAR(25),
 S_ADDRESS VARCHAR(40),
 S_NATIONKEY INT,
 S_PHONE CHAR(15),
 S_ACCTBAL DOUBLE,
 S_COMMENT VARCHAR(101)
 )
STORED AS ORC;

CREATE TABLE PARTSUPP (
 PS_PARTKEY INT,
 PS_SUPPKEY INT,
 PS_AVAILQTY INT,
 PS_SUPPLYCOST DOUBLE,
 PS_COMMENT VARCHAR (199)
 )
STORED AS ORC;

CREATE TABLE CUSTOMER (
 C_CUSTKEY INT,
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY INT,
 C_PHONE CHAR(15),
 C_ACCTBAL DOUBLE ,
 C_MKTSEGMENT CHAR(10),
 C_COMMENT VARCHAR(117)
 )
STORED AS ORC;

CREATE TABLE ORDERS (
 O_ORDERKEY BIGINT,
 O_CUSTKEY INT,
 O_ORDERSTATUS CHAR(1),
 O_TOTALPRICE DOUBLE,
 O_ORDERDATE DATE,
 O_ORDERPRIORITY CHAR(15),
 O_CLERK CHAR(15),
 O_SHIPPRIORITY INT,
 O_COMMENT VARCHAR(79)
 )
STORED AS ORC;

CREATE TABLE LINEITEM (
 L_ORDERKEY BIGINT,
 L_PARTKEY INT,
 L_SUPPKEY INT,
 L_LINENUMBER INT,
 L_QUANTITY DOUBLE,
 L_EXTENDEDPRICE DOUBLE,
 L_DISCOUNT DOUBLE,
 L_TAX DOUBLE,
 L_RETURNFLAG CHAR(1),
 L_LINESTATUS CHAR(1),
 L_SHIPDATE DATE,
 L_COMMITDATE DATE,
 L_RECEIPTDATE DATE,
 L_SHIPINSTRUCT CHAR(25),
 L_SHIPMODE CHAR(10),
 L_COMMENT VARCHAR(44)
 )
STORED AS ORC;


