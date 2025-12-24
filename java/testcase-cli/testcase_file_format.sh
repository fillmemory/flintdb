#!/bin/bash


SCRIPT="${BASH_SOURCE[0]}"
SCRIPTDIR=$(dirname $SCRIPT)
cd ${SCRIPTDIR}
HOMEDIR=`pwd`
if [[ "$HOMEDIR" =~ "/testcase" ]]; then
	cd ..
	HOMEDIR=`pwd`
fi

# clear & reset terminal
# clear && printf '\e[3J'

# https://github.com/electrum/tpch-dbgen
# wget http://www.trentu.ca/~dbrobinson/tpch-dbgen.tar.gz
# tar -xvzf tpch-dbgen.tar.gz
# cd tpch-dbgen
# make
# ./dbgen -s 1 -T L

FILENAME="tpch_lineitem.tsv.gz"
#FILENAME="tpch_lineitem.jsonl.gz"
FILENAME="tpch_lineitem.parquet"

rm -f temp/${FILENAME}*

if [ ! -f temp/tpch/lineitem.tbl.gz ]; then
	echo "lineitem.tbl.gz not found"
	exit 1
fi

cat > temp/tpch/lineitem.tbl.gz.desc <<- EOF
CREATE TABLE tpch_lineitem.tbl (
	l_orderkey    UINT,
	l_partkey     UINT,
	l_suppkey     UINT16,
	l_linenumber  UINT8,
	l_quantity    DECIMAL(4,2),
	l_extendedprice  DECIMAL(4,2),
	l_discount    DECIMAL(4,2),
	l_tax         DECIMAL(4,2),
	l_returnflag  STRING(1),
	l_linestatus  STRING(1),
	l_shipDATE    DATE,
	l_commitDATE  DATE,
	l_receiptDATE DATE,
	l_shipinstruct STRING(25),
	l_shipmode     STRING(10),
	l_comment      STRING(44)
) HEADER=SKIP, DELIMITER=|
EOF

cat > temp/${FILENAME}.desc <<- EOF
CREATE TABLE ${FILENAME} (
	l_orderkey    UINT,
	l_partkey     UINT,
	l_suppkey     UINT16,
	l_linenumber  UINT8,
	l_quantity    DECIMAL(4,2),
	l_extendedprice  DECIMAL(4,2),
	l_discount    DECIMAL(4,2),
	l_tax         DECIMAL(4,2),
	l_returnflag  STRING(1),
	l_linestatus  STRING(1),
	
	PRIMARY KEY (l_orderkey, l_linenumber)
)
EOF

echo "INSERT FROM"
./bin/flintdb.sh "INSERT INTO temp/${FILENAME}.desc FROM temp/tpch/lineitem.tbl.gz" -log
#  LIMIT 300000

ls -l temp/tpch_lineitem*
