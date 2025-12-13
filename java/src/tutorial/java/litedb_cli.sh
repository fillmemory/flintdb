#!/bin/bash
# Tutorial command line script

SCRIPT="${BASH_SOURCE[0]}"
SCRIPTDIR=$(dirname $SCRIPT)
SCRIPTDIR=$(realpath ${SCRIPTDIR})
SCRIPTDIR=$(dirname ${SCRIPTDIR})


if [ ! -f "data/customers.tsv.gz" ] || [ ! -f "data/orders.tsv.gz" ] || [ ! -f "data/order_items.tsv.gz" ]; then
  echo
  echo "--------------------------------" 
  echo "Make tutorial data files"
  ./tutorial.sh 
fi

echo
echo "--------------------------------" 
echo "TSV files into FlintDB files"

# clean up previous runs
rm -f tutorial/data/*.flintdb*

if [ ! -f "tutorial/data/order_items.flintdb" ]; then
  cat > tutorial/data/order_items.flintdb.desc <<- EOF
CREATE TABLE order_items.flintdb (
  orderkey UINT,
  partkey UINT,
  suppkey UINT16,
  linenumber UINT8,
  quantity DECIMAL(8,2),
  extendedprice DECIMAL(14,2),
  discount DECIMAL(4,2),
  tax DECIMAL(4,2),
  returnflag STRING(1),
  linestatus STRING(1),
  ship_at TIME,
  commit_at TIME,
  receipt_at TIME,
  shipinstruct STRING(25),
  shipmode STRING(10),
  comment STRING(64),

  PRIMARY KEY (orderkey, linenumber)
)
EOF

  echo
  echo "--------------------------------" 
  echo "Creating order_items.flintdb"
  ./bin/flintdb "INSERT INTO tutorial/data/order_items.flintdb.desc FROM tutorial/data/order_items.tsv.gz" -log
fi

if [ ! -f "tutorial/data/orders.flintdb" ]; then
  cat > tutorial/data/orders.flintdb.desc <<- EOF
CREATE TABLE orders.flintdb (
  orderkey UINT,
  customer_id UINT,
  amount DECIMAL(12,2),
  order_at TIME,
  status INT8,

  PRIMARY KEY (orderkey, customer_id)
)
EOF

  echo
  echo "--------------------------------" 
  echo "Creating orders.flintdb"
  ./bin/flintdb "INSERT INTO tutorial/data/orders.flintdb.desc FROM tutorial/data/orders.tsv.gz" -log
fi

if [ ! -f "tutorial/data/customers.flintdb" ]; then
  cat > tutorial/data/customers.flintdb.desc <<- EOF
CREATE TABLE customers.flintdb (
	customer_id UINT,
	name STRING(100),
	email STRING(120),
	created_at TIME,

  PRIMARY KEY (customer_id)
)
EOF

  echo
  echo "--------------------------------" 
  echo "Creating customers.flintdb"
  ./bin/flintdb "INSERT INTO tutorial/data/customers.flintdb.desc FROM tutorial/data/customers.tsv.gz" -log
fi


if [ ! -f "tutorial/data/customers.csv.gz" ]; then
  cat > tutorial/data/customers.csv.gz.desc <<- EOF
CREATE TABLE customers.csv.gz (
	customer_id UINT,
	name STRING(100),
	email STRING(120),
	created_at TIME
)
EOF

  echo
  echo "--------------------------------" 
  echo "TSV to CSV"
  ./bin/flintdb "INSERT INTO tutorial/data/customers.csv.gz.desc FROM tutorial/data/customers.tsv.gz" -log
  #  LIMIT 300000
fi