#!/bin/bash
# 

echo "========================================="
echo "Test 1: WAL=TRUNCATE (auto-truncate mode)"
echo "========================================="

SQL_CREATE_TABLE=$(cat <<- EOF
CREATE TABLE ../temp/testcase.flintdb (
    id UINT,
    name STRING(100),
    age UINT8,
    salary DECIMAL(10,2),

    PRIMARY KEY (id)
) WAL=TRUNCATE
EOF
)

echo "DROP TABLE"
./bin/flintdb "DROP TABLE ../temp/testcase.flintdb" -log
if [ -f ../temp/testcase.flintdb ]; then
    echo "Removing existing ../temp/testcase.flintdb"
    rm ../temp/testcase.flintdb ../temp/testcase.flintdb.*
fi


echo "CREATE TABLE"
./bin/flintdb "$SQL_CREATE_TABLE" -log -status


echo "INSERT INTO (with column specification)"
./bin/flintdb "INSERT INTO ../temp/testcase.flintdb (id, name, age, salary) VALUES (1, 'Alice', 30, 60000.00)" -log
./bin/flintdb "INSERT INTO ../temp/testcase.flintdb (id, name, age, salary) VALUES (2, 'Bob', 25, 50000.00)" -log

echo "INSERT INTO (without column specification - all columns in order)"
./bin/flintdb "INSERT INTO ../temp/testcase.flintdb VALUES (3, 'Charlie', 35, 70000.00)" -log


echo "UPDATE"
./bin/flintdb "UPDATE ../temp/testcase.flintdb SET salary = 65000.00 WHERE id = 2" -log

echo "DELETE"
./bin/flintdb "DELETE FROM ../temp/testcase.flintdb WHERE id = 2" -log

echo ""
echo "WAL file size after operations:"
if [ -f ../temp/testcase.flintdb.wal ]; then
    ls -lh ../temp/testcase.flintdb.wal
else
    echo "WAL file not found (may have been truncated)"
fi

echo ""
echo "========================================="
echo "Test 2: WAL=LOG (keep all logs mode)"
echo "========================================="

SQL_CREATE_TABLE_LOG=$(cat <<- EOF
CREATE TABLE ../temp/testcase_log.flintdb (
    id UINT,
    name STRING(100),
    age UINT8,
    salary DECIMAL(10,2),

    PRIMARY KEY (id)
) WAL=LOG
EOF
)

echo "DROP TABLE"
./bin/flintdb "DROP TABLE ../temp/testcase_log.flintdb" -log
if [ -f ../temp/testcase_log.flintdb ]; then
    rm ../temp/testcase_log.flintdb ../temp/testcase_log.flintdb.*
fi

echo "CREATE TABLE (LOG mode)"
./bin/flintdb "$SQL_CREATE_TABLE_LOG" -log

echo "INSERT operations"
./bin/flintdb "INSERT INTO ../temp/testcase_log.flintdb VALUES (1, 'Test1', 20, 40000.00)" -log
./bin/flintdb "INSERT INTO ../temp/testcase_log.flintdb VALUES (2, 'Test2', 30, 50000.00)" -log

echo ""
echo "WAL file size with LOG mode:"
if [ -f ../temp/testcase_log.flintdb.wal ]; then
    ls -lh ../temp/testcase_log.flintdb.wal
fi

echo ""
echo "========================================="
echo "Test 3: WAL=NONE (disabled mode)"
echo "========================================="



SQL_CREATE_TABLE_NONE=$(cat <<- EOF
CREATE TABLE ../temp/testcase_none.flintdb (
    id UINT,
    name STRING(100),

    PRIMARY KEY (id)
) WAL=NONE
EOF
)

echo "DROP TABLE"
./bin/flintdb "DROP TABLE ../temp/testcase_none.flintdb" -log
if [ -f ../temp/testcase_none.flintdb ]; then
    rm ../temp/testcase_none.flintdb ../temp/testcase_none.flintdb.*
fi

echo "CREATE TABLE (NONE mode)"
./bin/flintdb "$SQL_CREATE_TABLE_NONE" -log
./bin/flintdb "INSERT INTO ../temp/testcase_none.flintdb VALUES (1, 'NoWAL')" -log

echo ""
echo "WAL file should not exist:"
if [ -f ../temp/testcase_none.flintdb.wal ]; then
    echo "ERROR: WAL file exists when it shouldn't"
    ls -lh ../temp/testcase_none.flintdb.wal
else
    echo "OK: No WAL file (as expected)"
fi

