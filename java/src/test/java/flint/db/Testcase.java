/**
 * 
 */
package flint.db;

import java.io.File;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * 
 */
public class Testcase {

	public static void main(String[] args) throws Exception {
		testcase1();
	}

	static void TRUE(boolean expr, String exc) {
		if (expr)
			return;
		throw new RuntimeException(exc);
	}

	public static void testcase1() throws Exception {
		final int MAX = 365 * 2; // 1, 365
		final Calendar c = Calendar.getInstance();
		c.set(2023, 0, 1);
		final SecureRandom random = new SecureRandom();

		final File file = new File("temp/sampledb");
		Table.drop(file);

		final Meta meta = new Meta(file.getName()) //
				.columns( //
						new Column[] { //
								new Column.Builder("DT", Column.TYPE_DATE).create(), //
								new Column.Builder("A", Column.TYPE_STRING).bytes(32).create(), //
								new Column.Builder("B", Column.TYPE_INT).create(), //
								new Column.Builder("C", Column.TYPE_INT64).create(), //
								new Column.Builder("D", Column.TYPE_STRING).bytes(32).create(), //
								new Column.Builder("E", Column.TYPE_DATE).create(), //
								new Column.Builder("F", Column.TYPE_TIME).create(), //
						}) //
				.indexes( //
						new Index[] { //
								new Table.PrimaryKey(new String[] { "DT" }) //
						}) //
		// .compact(64) //
		// .compressor("gzip") //
		// .storage(Storage.TYPE_MEMORY) //
		;

		try (final IO.Closer CLOSER = new IO.Closer()) {
			// System.out.println(file);
			final Table ft = CLOSER.register(Table.open(file, meta, new Logger() {
				@Override
				public void log(String fmt, Object... args) {
					System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
				}

				@Override
				public void error(String fmt, Object... args) {
					System.err.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
					System.exit(0);
				}
			}));

			for (int i = 0; i < MAX; i++) {
				final Row r = Row.create(ft.meta(), //
						new Object[] { //
								new SimpleDateFormat("yyyy-MM-dd").format(c.getTime()), //
								UUID.randomUUID().toString().replace("-", ""), // A
								i + 1, // B
								Math.abs(random.nextInt()), // C
								UUID.randomUUID().toString().replace("-", ""), // D
								new Date(), //
								new Date()  //
						});
				ft.apply(r);

				c.add(Calendar.DATE, 1);
			}

			// final long rows = ft.find("USE INDEX (PRIMARY ASC)", new Filter.Visitor<Long>() {
			// @Override
			// public void visit(final Long i) throws Exception {
			// }
			// });
			// System.out.println("SCAN : " + rows + "rows");

			// assert CLOSER.register(ft.find("USE INDEX(PRIMARY ASC) WHERE DT = '2023-03-26'")).next() != -1 : "ASC 2023-03-26";
			for (int i = 0; i < MAX; i++) { // 365
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2023-01-01"));
				calendar.set(Calendar.DAY_OF_YEAR, i + 1);

				String dt = new java.text.SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
				// System.out.println(dd);
				try (final Cursor<Long> cursor = ft.find("USE INDEX(PRIMARY ASC) WHERE DT = '" + dt + "'")) {
					long n = cursor.next();
					TRUE(n != -1L, "ASC " + dt + " (" + calendar.get(Calendar.DAY_OF_YEAR) + ")");
					Row r = ft.read(n);
					TRUE(dt.concat(" 00:00:00.0").equals(r.getString("DT")), "ASC DT : " + dt + ", (" + calendar.get(Calendar.DAY_OF_YEAR) + ")" + ", but " + r.getString("DT"));
				}

				try (final Cursor<Long> cursor = CLOSER.register(ft.find("USE INDEX(PRIMARY DESC) WHERE DT = '" + dt + "'"))) {
					long n = cursor.next();
					TRUE(n != -1L, "DESC " + dt);
					Row r = ft.read(n);
					TRUE(dt.concat(" 00:00:00.0").equals(r.getString("DT")), "DESC DT : " + dt + ", but " + r.getString("DT"));
				}

				try (final Cursor<Long> cursor = CLOSER.register(ft.find("USE INDEX(PRIMARY ASC) WHERE DT >= '" + dt + "'"))) {
					long n = cursor.next();
					TRUE(n != -1L, "ASC >= " + dt + " (" + calendar.get(Calendar.DAY_OF_YEAR) + ")");
					Row r = ft.read(n);
					TRUE(dt.concat(" 00:00:00.0").equals(r.getString("DT")), "ASC2 DT : " + dt + ", but " + r.getString("DT") + " (" + calendar.get(Calendar.DAY_OF_YEAR) + ")");
				}

				try (final Cursor<Long> cursor = CLOSER.register(ft.find("USE INDEX(PRIMARY DESC) WHERE DT <= '" + dt + "'"))) {
					long n = cursor.next();
					TRUE(n != -1L, "DESC " + dt);
					Row r = ft.read(n);
					TRUE(dt.concat(" 00:00:00.0").equals(r.getString("DT")), "DESC2 DT : " + dt + ", but " + r.getString("DT"));
				}
			}
			try (final Cursor<Long> cursor = ft.find("USE INDEX(PRIMARY ASC) WHERE DT = '2023-05-01'")) {
				for (long i; (i = cursor.next()) > -1;) {
					final Row r = ft.read(i);
					final long ok = ft.delete(r.id());
					System.out.println("DELETE : " + r.id() + " (" + (ok > 0 ? "OK" : "ERR") + ") => " + r.map(new LinkedHashMap<>()));
				}
			}

			try (final Cursor<Long> cursor = CLOSER.register(ft.find("USE INDEX (PRIMARY DESC) WHERE DT <= '2023-06-30' LIMIT 1, 2"))) {
				int max = 3;
				for (long i; (i = cursor.next()) > -1;) {
					final Row r = ft.read(i);
					System.out.println("FOUND DESC DATE : " + r.get("DT") + "=> " + r.getDate("DT") + " => " + i);
					if (max-- <= 0)
						break;
				}
			}
		}

		try (final IO.Closer CLOSER = new IO.Closer()) {
			// System.out.println(file);
			CLOSER.register(Table.open(file, meta, new Logger.NullLogger()));
		}
	}
}
