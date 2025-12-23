#!/bin/bash
# 

# Testcase for SQL queries on TPCH lineitem table
echo "SELECT l_shipmode FROM TPCH lineitem table in FlintDB format"
./bin/flintdb "SELECT l_shipmode FROM ../temp/c/tpch_lineitem.flintdb LIMIT 10" -pretty

echo "SELECT l_shipmode FROM TPCH lineitem table in TSV.GZ format"
./bin/flintdb "SELECT l_shipmode FROM ../temp/tpch/lineitem.tbl.gz LIMIT 10" -pretty

echo "GROUP BY queries on TPCH lineitem table"
./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM ../temp/c/tpch_lineitem.flintdb GROUP BY l_shipmode LIMIT 10" -pretty

echo "GROUP BY queries on TPCH lineitem table in TSV.GZ format"
./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM ../temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10" -pretty

echo "DISTINCT and COUNT queries on TPCH lineitem table"
./bin/flintdb "SELECT DISTINCT l_shipmode FROM ../temp/c/tpch_lineitem.flintdb LIMIT 10" -pretty

echo "SELECT USE INDEX queries on TPCH lineitem table"
./bin/flintdb "SELECT * FROM ../temp/c/tpch_lineitem.flintdb USE INDEX(primary DESC) LIMIT 10" -pretty

echo "COUNT queries on TPCH lineitem table"
./bin/flintdb "SELECT COUNT(*) v FROM ../temp/c/tpch_lineitem.flintdb LIMIT 10" -pretty


# performance test vs Java
echo "Performance test vs Java implementation"
echo "C: SELECT * FROM TPCH lineitem table:"
time ./bin/flintdb "SELECT * FROM ../temp/c/tpch_lineitem.flintdb " > /dev/null
echo "Java: SELECT * FROM TPCH lineitem table:"
time ../java/bin/flintdb "SELECT * FROM ../temp/c/tpch_lineitem.flintdb " > /dev/null

echo "C: SELECT * FROM TPCH lineitem table with gzip input:"
time ./bin/flintdb "SELECT * FROM ../temp/tpch/lineitem.tbl.gz " > /dev/null
echo "Java: SELECT * FROM TPCH lineitem table with gzip input:"
time ../java/bin/flintdb "SELECT * FROM ../temp/tpch/lineitem.tbl.gz " > /dev/null

echo "C: GROUP BY:"
time ./bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM ../temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10 "

echo "Java: GROUP BY:"
time ../java/bin/flintdb "SELECT l_shipmode, COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM ../temp/tpch/lineitem.tbl.gz GROUP BY l_shipmode LIMIT 10 "