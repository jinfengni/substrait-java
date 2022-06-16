
CREATE TABLE LINEITEM (
  "row_number" BIGINT,
  ORDERKEY BIGINT,
  PARTKEY BIGINT,
  SUPPKEY BIGINT,
  LINENUMBER INTEGER,
  QUANTITY double,
  EXTENDEDPRICE double,
  DISCOUNT DOUBLE,
  TAX DOUBLE,
  RETURNFLAG VARCHAR(1),
  LINESTATUS VARCHAR(1),
  SHIPDATE DATE,
  COMMITDATE DATE,
  RECEIPTDATE DATE,
  SHIPINSTRUCT VARCHAR(25),
  SHIPMODE VARCHAR(10),
  COMMENT VARCHAR(44)
);
