package flint.db;

public class TestcaseSQL {
	public static void main(String[] args) throws Exception {
		testcase0(304);
	}

	static String getMathFunction(String expr) {
		// Extract the mathematical function name from the expression
		String upper = expr.toUpperCase();
		if (upper.startsWith("MAX (")) return "MAX";
		if (upper.startsWith("MIN (")) return "MIN";
		if (upper.startsWith("AVG (")) return "AVG";
		if (upper.startsWith("SUM (")) return "SUM";
		if (upper.startsWith("COUNT (")) return "COUNT";
		// if (upper.startsWith("STDDEV (")) return "STDDEV";
		// if (upper.startsWith("VARIANCE (")) return "VARIANCE";
		// if (upper.startsWith("ABS (")) return "ABS";
		// if (upper.startsWith("ROUND (")) return "ROUND";
		// if (upper.startsWith("CEIL (")) return "CEIL";
		// if (upper.startsWith("FLOOR (")) return "FLOOR";
		// if (upper.startsWith("SQRT (")) return "SQRT";
		// if (upper.startsWith("POWER (")) return "POWER";
		// if (upper.startsWith("EXP (")) return "EXP";
		// if (upper.startsWith("LOG (")) return "LOG";
		// if (upper.startsWith("LOG10 (")) return "LOG10";
		// if (upper.startsWith("SIN (")) return "SIN";
		// if (upper.startsWith("COS (")) return "COS";
		// if (upper.startsWith("TAN (")) return "TAN";
		// if (upper.startsWith("ASIN (")) return "ASIN";
		// if (upper.startsWith("ACOS (")) return "ACOS";
		// if (upper.startsWith("ATAN (")) return "ATAN";
		// if (upper.startsWith("ATAN2 (")) return "ATAN2";
		// if (upper.startsWith("RADIANS (")) return "RADIANS";
		// if (upper.startsWith("DEGREES (")) return "DEGREES";
		// if (upper.startsWith("SIGN (")) return "SIGN";
		// if (upper.startsWith("LOG2 (")) return "LOG2";
		// if (upper.startsWith("LOG10 (")) return "LOG10";
		// if (upper.startsWith("TRUNCATE (")) return "TRUNCATE";
		// if (upper.startsWith("CONCAT (")) return "CONCAT";
		// if (upper.startsWith("LENGTH (")) return "LENGTH";
		// if (upper.startsWith("SUBSTRING (")) return "SUBSTRING";
		// if (upper.startsWith("LEFT (")) return "LEFT";
		// if (upper.startsWith("RIGHT (")) return "RIGHT";
		// if (upper.startsWith("POSITION (")) return "POSITION";
		// if (upper.startsWith("REPLACE (")) return "REPLACE";
		// if (upper.startsWith("TRIM (")) return "TRIM";
		// if (upper.startsWith("UPPER (")) return "UPPER";
		// if (upper.startsWith("LOWER (")) return "LOWER";
		// if (upper.startsWith("INITCAP (")) return "INITCAP";
		return null;
	}


	static void testcase2() throws Exception {
		String sql = "UPDATE temp/xxxdb SET B = 1, C = 1 , D = 'A001' , E = 'B''001' WHERE A = '000001c708854d37a9c49f0438594dae' ";
		int x = 0;
		if (x == 0) {
			SQL q = SQL.parse(sql);
			System.out.println("table : " + q.table());
			System.out.println("columns : " + java.util.Arrays.toString(q.columns()));
			System.out.println("values : " + java.util.Arrays.toString(q.values()));
			System.out.println("where : " + q.where());

			// CLI.main(new String[] { "-sql", sql });
		} else {
			System.out.println(sql = SQL.trim_mws(sql));
			String[] a = SQL.tokenize(sql);
			for (int i = 0; i < a.length; i++) {
				System.out.println(a[i]);
			}
		}
	}

	static void testcase3() throws Exception {
		SQL q = SQL.parse("DELETE FROM temp/testdb WHERE A = '00007a385f37436cac8b14776f54c65a'");
		System.out.println("table : " + q.table());
		System.out.println("where : " + q.where());

		CLI.main(new String[] { "-sql", q.origin() });
	}

	static void testcase4() throws Exception {
		SQL q = SQL.parse("INSERT INTO temp/xxxdb (A, B, C, D, E) VALUES ('00', 0, 0, '00', NULL)");
		System.out.println("table : " + q.table());

		CLI.main(new String[] { "-sql", q.origin() });
	}

	public static void testcase0(int testcase) throws Exception {
		switch (testcase) {
		case 1:
			System.out.println(SQL.trim_mws("SELECT \n `a 1`, b, c, ( d + 1 ) as d \n FROM table  USE INDEX(PRIMARY DESC) WHERE a = '1\\'a b   ' AND b = 2  AND c = '한 글' ORDER BY a DESC LIMIT 9, 10") + "<EOL>");
			break;

		case 2:
			String[] a = SQL.tokenize(SQL.trim_mws("SELECT a, b, c, ( d + 1 ) as d, MAX(c) as maxc, MIN(c) as minc FROM table USE INDEX (PRIMARY DESC) WHERE (a = '1\\'a b' AND b = 2 )  ORDER BY a DESC LIMIT 9, 10 "));
			for (String s : a)
				System.out.println(s + "<EOL>");
			break;

		case 101:
			System.out.println(SQL.parse("SELECT a, b, c, ( d + 1 ) as d FROM table USE INDEX (PRIMARY DESC)  WHERE (a = '1\\'a b' AND b = 2 )  ORDER BY a DESC, b ASC LIMIT 9, 10 INTO temp/output.tsv "));

			break;
		case 102:
			System.out.println(SQL.parse("DELETE FROM temp/testdb USE INDEX (PRIMARY DESC) WHERE A = '00007a385f37436cac8b14776f54c65a'"));
			break;
		case 103:
			System.out.println(SQL.parse("UPDATE temp/testdb USE INDEX (PRIMARY DESC) set  B = NULL, D = 'ABC' WHERE A = '00007a385f37436cac8b14776f54c65a'"));

			break;
		case 104:
			System.out.println(SQL.parse("INSERT IGNORE INTO temp/testdb (A, B, C, D, E) VALUES ('00', 0, 0, '00', NULL)"));

			break;
		case 105:
			System.out.println(SQL.parse("INSERT IGNORE INTO temp/testdb FROM temp/testdb_old"));

			break;
		case 107:
			System.out.println(SQL.parse("INSERT  INTO temp/testdb (a, b, c, d) FROM temp/testdb_old"));

			break;
		case 108:
			System.out.println(SQL.parse("DROP TABLE temp/testdb"));
			break;
		case 201:
			System.out.println(SQL.parse("SELECT A, B, C FROM temp/testdb USE INDEX(PRIMARY DESC) LIMIT 10 INTO temp/output.tsv"));
			break;

		case 301:
			System.out.println(SQL.parse("SELECT * FROM mytable CONNECT(jdbc:mysql://localhost/test?user=user&password=password) USE INDEX(PRIMARY DESC) INTO temp/output.tsv.gz"));
			break;
		case 302:
			System.out.println(SQL.parse("META client CONNECT(jdbc:mysql://localhost/test?user=user&password=password#com.mysql.jdbc.Driver)"));
			break;
		case 303:
			System.out.println(SQL.parse("show tables WHERE temp"));
			break;
		case 304:
			final SQL q = SQL.parse( //
					"CREATE TEMPORARY TABLE TEST_TBL" //
							.concat("(").concat("\n") //
							.concat("AA STRING(128) NOT NULL COMMENT 'CMT A',").concat("\n") //
							.concat("BB STRING(64) NOT NULL DEFAULT '' COMMENT 'CMT B',").concat("\n") //
							.concat("  cc   DECIMAL(10,3) DEFAULT 0,").concat("\n") //
							.concat("  dd   int DEFAULT 123,").concat("\n") //
							.concat("  ee   Date,").concat("\n") //
							.concat("  ff\t\nTIME,").concat("\n") //
							.concat("primary key (AA, BB),").concat("\n") //
							.concat("key   IX_CC   ( cc , BB)").concat("\n") //
							.concat(")").concat("\n") //
							.concat("DICTIONARY=dictionary.dic,").concat("\n") //
							.concat("COMPRESSOR=gz,").concat("\n") //
							.concat("COMPACT=1K,").concat("\n") //
							.concat("MMAP=32K,").concat("\n") //
							.concat("CACHE=16K,").concat("\n") //
							.concat("DATE=2000-01-01").concat("\n") //
							.concat(";") //
			);
			System.out.println(Json.stringify(q.meta(), true));
			System.out.println(SQL.stringify(q.meta()));
			break;

		case 305:
			final SQL q1 = SQL.parse("CREATE TABLE * (key STRING (64), report_id LONG DEFAULT '0', native BYTE DEFAULT '0', short_message STRING (256), PRIMARY KEY (key))");
			System.out.println(Json.stringify(q1.meta(), true));
			System.out.println(SQL.stringify(q1.meta()));
			break;

		case 401:
			System.out.println(SQL.parse("SELECT DISTINCT C1, C2, C3 FROM TABLE1 WHERE C1 = 'A' "));
			break;
		case 402:
			System.out.println(SQL.parse("SELECT C1, C2, C3, COUNT(*) as cnt FROM TABLE1 WHERE C1 = 'A' GROUP BY C2, C3 HAVING cnt > 1 ORDER BY C1 DESC LIMIT 10"));
			break;
		case 403:
			System.out.println(SQL.parse("""
                    SELECT 
                    C1, -- Column 1
                    C2, -- Column 2
                    C3
                    /* ,
                    COUNT(*) as v1
                    SUM(*) as v2
                    */
                    FROM TABLE1
                    WHERE C1 = 'A'
                    GROUP BY C2, C3
                    HAVING v1 > 1
                    ORDER BY C1 DESC
                    LIMIT 10
                    """));
			break;
		}
	}


}
