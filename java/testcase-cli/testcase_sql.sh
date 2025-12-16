#!/bin/bash
# 

SQL_CREATE_TABLE=$(cat <<- EOF
CREATE TABLE temp/testcase.flintdb (
    id UINT NOT NULL,
    name STRING(100) NOT NULL,
    age UINT8,
    salary DECIMAL(10,2) NOT NULL,

    PRIMARY KEY (id)
) WAL=TRUNCATE
EOF
)

echo "DROP TABLE"
./bin/flintdb "DROP TABLE temp/testcase.flintdb" -log
if [ -f temp/testcase.flintdb ]; then
    echo "Removing existing temp/testcase.flintdb"
    rm temp/testcase.flintdb
fi


echo "CREATE TABLE"
./bin/flintdb "$SQL_CREATE_TABLE" -log -status


echo "INSERT INTO (with column specification)"
./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, name, age, salary) VALUES (1, 'Alice', 30, 60000.00)" -log
./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, name, age, salary) VALUES (2, 'Bob', 25, 50000.00)" -log

echo "INSERT INTO (without column specification - all columns in order)"
./bin/flintdb "INSERT INTO temp/testcase.flintdb VALUES (3, 'Charlie', 35, 70000.00)" -log

echo "INSERT INTO (duplicate primary key - should fail)"
./bin/flintdb "INSERT INTO temp/testcase.flintdb VALUES (3, 'Charlie', 35, 70001.99)" -log

echo "REPLACE INTO (without column specification - all columns in order)"
./bin/flintdb "REPLACE INTO temp/testcase.flintdb VALUES (3, 'Charlie', 35, 70001.99)" -log


echo "UPDATE"
./bin/flintdb "UPDATE temp/testcase.flintdb SET salary = 65000.00 WHERE id = 2" -log

echo "DELETE"
./bin/flintdb "DELETE FROM temp/testcase.flintdb WHERE id = 2" -log


echo "INSERT ... FROM FILE (without column specification)"
cat > temp/testcase_data.csv <<- EOF
id,name,age,salary
4,Dave,28,55000.00
5,Eve,32,72000.00
6,Frank,29,58000.00
EOF

./bin/flintdb "INSERT INTO temp/testcase.flintdb FROM temp/testcase_data.csv" -log

echo "INSERT ... FROM FILE (with column specification - partial columns)"
cat > temp/testcase_data2.csv <<- EOF
id,name,salary
7,Grace,53000.00
8,Heidi,69000.00
9,Ivan,71000.00
EOF

./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, name, salary) FROM temp/testcase_data2.csv" -log

echo "INSERT ... FROM FILE (with column specification - different order)"
cat > temp/testcase_data3.csv <<- EOF
id,salary,name,age
10,52000.00,Judy,26
11,75000.00,Kevin,34
EOF

./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, salary, name, age) FROM temp/testcase_data3.csv" -log



echo "SELECT *"
./bin/flintdb "SELECT * FROM temp/testcase.flintdb" -log



echo "INSERT INTO tsv.gz FROM FILE"

rm temp/testcase.tsv.gz* 2>/dev/null
cat > temp/testcase.tsv.gz.desc <<- EOF
CREATE TABLE temp/testcase.tsv.gz (
    id UINT,
    name STRING(100),
    age UINT8
)
EOF

./bin/flintdb "INSERT INTO temp/testcase.tsv.gz (id, name, age) FROM temp/testcase_data3.csv" -log
echo "SELECT * FROM tsv.gz"
./bin/flintdb "SELECT * FROM temp/testcase.tsv.gz" -log


echo ""
echo "========================================"
echo "SELECT INTO Tests"
echo "========================================"

echo "SELECT INTO TSV"
./bin/flintdb "SELECT * FROM temp/testcase.flintdb INTO temp/output_all.tsv" -log -status
./bin/flintdb "SELECT * FROM temp/output_all.tsv LIMIT 3" -pretty

echo "SELECT INTO CSV with WHERE"
./bin/flintdb "SELECT id, name, salary FROM temp/testcase.flintdb WHERE age > 30 INTO temp/output_filtered.csv" -log -status
./bin/flintdb "SELECT * FROM temp/output_filtered.csv" -pretty

echo "SELECT INTO compressed TSV"
./bin/flintdb "SELECT * FROM temp/testcase.flintdb WHERE salary > 60000 INTO temp/output_high_salary.tsv.gz" -log -status

echo "SELECT INTO LiteDB (with ORDER BY)"
rm temp/output_sorted.flintdb* 2>/dev/null
./bin/flintdb "SELECT * FROM temp/testcase.flintdb ORDER BY salary DESC INTO temp/output_sorted.flintdb" -log -status
./bin/flintdb "SELECT * FROM temp/output_sorted.flintdb LIMIT 5" -pretty

echo "SELECT INTO with aggregation"
./bin/flintdb "SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM temp/testcase.flintdb INTO temp/output_summary.tsv" -log -status
./bin/flintdb "SELECT * FROM temp/output_summary.tsv" -pretty


echo ""
echo "========================================"
echo "Transaction Tests"
echo "========================================"
echo "DROP TABLE transactions"
./bin/flintdb "DROP TABLE temp/transactions.flintdb" -status

echo "CREATE TABLE transactions"
./bin/flintdb "CREATE TABLE temp/transactions.flintdb (id UINT NOT NULL, name STRING(100), amount DECIMAL(10,2), PRIMARY KEY (id)) WAL=TRUNCATE " -status

echo "BEGIN TRANSACTION and COMMIT (single process)"
./bin/flintdb "BEGIN TRANSACTION temp/transactions.flintdb; INSERT INTO temp/transactions.flintdb VALUES (1, 'Transaction1', 100.50); INSERT INTO temp/transactions.flintdb VALUES (2, 'Transaction2', 200.75); INSERT INTO temp/transactions.flintdb VALUES (3, 'Transaction3', 300.25); COMMIT" -status

echo "SELECT after COMMIT (should have 3 rows)"
./bin/flintdb "SELECT * FROM temp/transactions.flintdb" -pretty

# Note: ROLLBACK is not yet fully supported in the current WAL implementation
# The current WAL provides durability (crash recovery) but not full atomicity (rollback)
# echo "BEGIN TRANSACTION and ROLLBACK (single process)"
# ./bin/flintdb "BEGIN TRANSACTION temp/transactions.flintdb; INSERT INTO temp/transactions.flintdb VALUES (4, 'Transaction4', 400.00); INSERT INTO temp/transactions.flintdb VALUES (5, 'Transaction5', 500.00); ROLLBACK" -status

echo "Multiple transactions in sequence (single process)"
./bin/flintdb "BEGIN TRANSACTION temp/transactions.flintdb; INSERT INTO temp/transactions.flintdb VALUES (4, 'TX4', 400.00); COMMIT; BEGIN TRANSACTION temp/transactions.flintdb; INSERT INTO temp/transactions.flintdb VALUES (5, 'TX5', 500.00); COMMIT" -status

echo "SELECT after multiple commits (should have 5 rows)"
./bin/flintdb "SELECT * FROM temp/transactions.flintdb" -pretty


echo ""
echo "========================================"
echo "JDBC Integration Tests"
echo "========================================"

echo "Note: JDBC tests require jdbc.properties configuration"
echo "Create ~/.flintdb/jdbc.properties with your database connection"
echo ""
echo "Example jdbc.properties:"
echo "  sqlitetest = jdbc:sqlite:/absolute/path/to/test.db?table={table}"
echo "  mydb = jdbc:sqlite:/path/to/mydb.db?table={table}"
echo ""

# Check if SQLite driver is available
if [ -f lib/optional/sqlite-jdbc-*.jar ] || [ -f lib/sqlite-jdbc-*.jar ]; then
    echo "Testing with SQLite database..."
    
    # Create SQLite test database in current directory
    DB_FILE="temp/test_jdbc.sqlitedb"
    rm -f "$DB_FILE"
    sqlite3 "$DB_FILE" <<- EOF
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, salary REAL);
INSERT INTO users VALUES (100, 'JDBC_Alice', 30, 60000.00);
INSERT INTO users VALUES (101, 'JDBC_Bob', 25, 50000.00);
INSERT INTO users VALUES (102, 'JDBC_Charlie', 35, 70000.00);
EOF
    
    echo "SELECT FROM JDBC"
    ./bin/flintdb "SELECT * FROM jdbc:sqlite:${DB_FILE}?table=users" -pretty
    
    echo "SELECT FROM JDBC with WHERE"
    ./bin/flintdb "SELECT * FROM jdbc:sqlite:${DB_FILE}?table=users WHERE age > 28" -pretty
    
    echo "SELECT FROM JDBC INTO file"
    ./bin/flintdb "SELECT * FROM jdbc:sqlite:${DB_FILE}?table=users INTO temp/jdbc_output.csv" -log
    ./bin/flintdb "SELECT * FROM temp/jdbc_output.csv" -pretty
    
else
    echo "SQLite JDBC driver not found in lib/ directory"
    echo "To test JDBC functionality:"
    echo "  1. Download SQLite driver: https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/"
    echo "  2. Copy sqlite-jdbc-*.jar to lib/ directory"
    echo ""
fi

echo ""
echo "========================================"
echo "Cleanup"
echo "========================================"
rm temp/testcase_data*.csv 2>/dev/null
rm temp/output_*.tsv* 2>/dev/null
rm temp/output_*.csv 2>/dev/null
rm temp/output_sorted.flintdb* 2>/dev/null
rm temp/jdbc_output.csv 2>/dev/null
rm test_jdbc.db 2>/dev/null

echo "Test completed."
echo ""
#
# ls -l temp/testcase.flintdb*


