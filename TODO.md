1. row validate() => apply sql_exec.c, SQLExec.java
2. Improve FileSorter (swap offset only, no move data record)
3. WAL currently unstable
3. WAL dump -> Text
4. WAL memory storage for fastest bulk
5. Lazy Variant value type conversion for faster processing (struct decimal, date, time, ..., but indexed columns always converted)
6. 16K storage alignment