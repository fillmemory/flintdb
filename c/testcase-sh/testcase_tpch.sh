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

# Parse command line arguments
LIMIT=""
WAL_OPTION="NONE"
while [[ $# -gt 0 ]]; do
	case $1 in
		-limit)
			LIMIT="LIMIT $2"
			shift 2
			;;
		-limit=*)
			LIMIT="LIMIT ${1#-limit=}"
			shift
			;;
		-wal)
			WAL_OPTION="$2"
			shift 2
			;;
		-wal=*)
			WAL_OPTION="${1#-wal=}"
			shift
			;;
		*)
			shift
			;;
	esac
done

# https://github.com/electrum/tpch-dbgen

rm -f ../temp/tpch_lineitem.flintdb*

if [ ! -f ../temp/tpch/lineitem.tbl.gz ]; then
	echo "lineitem.tbl.gz not found"
	exit 1
fi

cat > ../temp/tpch/lineitem.tbl.gz.desc <<- EOF
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
) HEADER=ABSENT, DELIMITER=|
EOF


SQL_CREATE_TABLE=$(cat <<- EOF
CREATE TABLE ../temp/c/tpch_lineitem.flintdb (
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
	l_comment      STRING(44),
	
	PRIMARY KEY (l_orderkey, l_linenumber)
) CACHE=256K, WAL=${WAL_OPTION}, WAL_CHECKPOINT_INTERVAL=10000000, WAL_BATCH_SIZE=50000, WAL_COMPRESSION_THRESHOLD=1024
EOF
)
# WAL=NONE|TRUNCATE|LOG
echo "CREATE TABLE"
./bin/flintdb "$SQL_CREATE_TABLE" -status


# ) CACHE=1M, STORAGE=MEMORY, COMPACT=140

echo "INSERT FROM"
./bin/flintdb "INSERT INTO ../temp/c/tpch_lineitem.flintdb FROM ../temp/tpch/lineitem.tbl.gz ${LIMIT}" -status

#  LIMIT 300000

ls -l ../temp/c/tpch_lineitem*
