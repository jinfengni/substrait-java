#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "${parent_path}"
CMD=../../../build/graal/isthmus

TPCH="../resources/tpch/"
DDL=`cat ${TPCH}/schema.sql`
QUERY_FOLDER="${TPCH}/queries"

# tpc-h Q1
Q1=`cat $QUERY_FOLDER/01.sql`
Q3=`cat $QUERY_FOLDER/03.sql`
Q5=`cat $QUERY_FOLDER/05.sql`
Q6=`cat $QUERY_FOLDER/06.sql`
Q10=`cat $QUERY_FOLDER/10.sql`
Q14=`cat $QUERY_FOLDER/14.sql`
Q19=`cat $QUERY_FOLDER/19.sql`

###
echo "QACK Run 7 tpc-h queries together. Three rounds."
$CMD -q "${Q1}" -q "${Q3}"  -q "${Q5}"  -q "${Q6}" -q "${Q10}" -q "${Q14}" -q "${Q19}" -q "${Q1}" -q "${Q3}"  -q "${Q5}"  -q "${Q6}" -q "${Q10}" -q "${Q14}" -q "${Q19}" -q "${Q1}" -q "${Q3}"  -q "${Q5}"  -q "${Q6}" -q "${Q10}" -q "${Q14}" -q "${Q19}"  --create "${DDL}"

echo "QACK run one tpc-h query separately"
$CMD -q "${Q1}" --create "${DDL}"
$CMD -q "${Q3}" --create "${DDL}"
$CMD -q "${Q5}" --create "${DDL}"
$CMD -q "${Q6}" --create "${DDL}"
$CMD -q "${Q10}" --create "${DDL}"
$CMD -q "${Q14}" --create "${DDL}"
$CMD -q "${Q19}" --create "${DDL}"

