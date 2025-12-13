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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 */
public class TestcaseMemoryTable {
	static final File file = new File("temp/sampledb");
	static final File transfer = new File("temp/transfer.db");

	public static void main(String[] args) throws Exception {
		testcase_memory_transfer();
		testcase_from_transfer();
	}

	public static void testcase_memory_transfer() throws Exception {
		final int MAX = 365; // 1, 365
		final Calendar c = Calendar.getInstance();
		c.set(2023, 0, 1);
		final SecureRandom random = new SecureRandom();

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
				.compact(64) //
				// .compressor("gzip") //
				.storage(Storage.TYPE_MEMORY) //
		;

		try (final IO.Closer CLOSER = new IO.Closer()) {
			// System.out.println(file);
			final Table ft = Table.open(file, meta, new Logger() {
				@Override
				public void log(String fmt, Object... args) {
					System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
				}

				@Override
				public void error(String fmt, Object... args) {
					System.err.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
					System.exit(0);
				}
			});

			CLOSER.register(ft);

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

			final AtomicLong rowCount = new AtomicLong(0);
			try (final Cursor<Long> scanCursor = ft.find("USE INDEX (PRIMARY ASC)")) {
				while ((scanCursor.next()) != null) {
					rowCount.incrementAndGet();
				}
			}
			final long rows = rowCount.get();
			System.out.println("SCAN   : " + rows + "rows");

			Cursor<Long> cursor = CLOSER.register(ft.find("USE INDEX(PRIMARY DESC) WHERE DT = '2023-12-22'"));
			for (long i; (i = cursor.next()) > -1;) {
				final Row r = ft.read(i);
				final long ok = ft.delete(r.id());
				System.out.println("DELETE : " + r.id() + " (" + (ok > 0 ? "OK" : "ERR") + ") => " + r.map(new LinkedHashMap<>()));
			}

			try (final Cursor<Long> findCursor = ft.find("USE INDEX (PRIMARY DESC) WHERE DT >= '2023-06-30' LIMIT 1, 2")) {
				Long i;
				while ((i = findCursor.next()) != null) {
					final Row r = ft.read(i);
					System.out.println("FOUND  : " + r.id() + " => " + r.map(new LinkedHashMap<>()));
				}
			}

			cursor = CLOSER.register(ft.find("USE INDEX (PRIMARY ASC) WHERE DT >= '2023-06-30' LIMIT 1, 2"));
			int max = 3;
			for (long i; (i = cursor.next()) > -1;) {
				final Row r = ft.read(i);
				System.out.println("DATE  : " + r.get("DT") + "=> " + r.getDate("DT") + " => " + i);
				if (max-- <= 0)
					break;
			}

			//
			Table.transfer(ft, transfer);
		}
	}

	private static void testcase_from_transfer() throws Exception {
		try (final IO.Closer CLOSER = new IO.Closer()) {
			final Table ft = Table.open(transfer, Table.OPEN_READ, new Logger.NullLogger());
			final Cursor<Long> cursor = ft.find("USE INDEX (PRIMARY ASC) WHERE DT >= '2023-06-30' LIMIT 1, 2");
			int max = 3;
			for (long i; (i = cursor.next()) > -1;) {
				final Row r = ft.read(i);
				System.out.println("FROM FILE => DATE  : " + r.get("DT") + "=> " + r.getDate("DT") + " => " + i);
				if (max-- <= 0)
					break;
			}

			CLOSER.register(ft);
		}
	}
}
