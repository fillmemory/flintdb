/**
 * 
 */
package flint.db;

import java.io.File;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ./bin/tools.sh -sql "SELECT A, B, C FROM temp/testdb.flintdb USE INDEX(PRIMARY DESC) LIMIT 10" -pretty
 */
public class TestcaseLargeRows {

	public static void main(String[] args) throws Exception {
		System.setProperty("file.encoding", "UTF-8");

		System.out.println("--------------------------- v2 ---------------------------");
		System.out.println("java.version : " + System.getProperty("java.version"));
		for (int i = 0; i < 1; i++) {
			final int ROWS = 1 * 1024 * 1024;
			// final int ROWS = 10;
			// final int ROWS = 138;
			final int MMAP_SIZE = 32 * 1024 * 1024;
			final int CACHE_SIZE = 32 * 1024; // -1, 10 * 1024, 2 * 1024 * 1024
			case_random_uuid(ROWS, MMAP_SIZE, CACHE_SIZE, CACHE_SIZE, true, true, false, -1);
			// case_random_uuid(M, ROWS, MMAP_SIZE, CACHE_SIZE, CACHE_SIZE, false, false, true, Index.PRIMARY);
		}

	}

	@SuppressWarnings("unused")
	static void case_random_uuid(final int MAX, final int mmap, final int rCacheSize, final int iCacheSize, Boolean DROP, Boolean WRITE, Boolean SEARCH, int vindex) throws Exception {
		final File file = new File("temp/testdb.flintdb");
		if (DROP)
			Table.drop(file);

		//
		final int SPACING = 36;

		final Meta meta = new Meta(file.getName()) //
				// .cacheSize(rCacheSize) //
				.compact(-1)  // -1, 16, 64
				// .compact(512 - (1 + 1 + 2 + 4 + 8)) //
				.cacheSize(rCacheSize) //
				.increment(mmap) //
				.columns( //
						new Column[] { //
								new Column.Builder("A", Column.TYPE_STRING).bytes(40).create(), //
								new Column.Builder("B", Column.TYPE_INT).create(), //
								new Column.Builder("C", Column.TYPE_INT64).create(), //
								new Column.Builder("D", Column.TYPE_STRING).bytes(36).create(), //
								new Column.Builder("E", Column.TYPE_STRING).bytes(36).create(), //
						}) //
				.indexes( //
						new Index[] { //
								new Table.PrimaryKey(new String[] { "A" }) //
						}) //
		;
		// System.out.println(Meta.toString(type, meta));

		// System.out.println(Json.stringify(Meta.translate(meta, new String[] { "A", "B" }), true));

		if (!file.exists())
			file.getParentFile().mkdirs();
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

		// System.out.println("rowLength : " + ft.meta().rowLength());

		final IO.StopWatch watch = new IO.StopWatch();

		long position = -1;
		if (WRITE) {
			// final PrintStream log = new PrintStream(new FileOutputStream(new File(file.getParentFile(), file.getName() + ".replay")));
			// log.println("-- " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())));

			final SecureRandom random = new SecureRandom();
			final byte[] array = new byte[SPACING / 2];

			for (int i = 1; i <= MAX; i++) {
				final int v = i;
				random.nextBytes(array);

				// final String A = Integer.toHexString(v);
				final String A = UUID.randomUUID().toString().replace("-", "");
				final Row r = Row.create(ft.meta(), //
						new Object[] { //
								A, // A
								v, // B
								v, // C
								UUID.randomUUID().toString().replace("-", ""), // D
								IO.Hex.encode(array, 0, array.length) // E
						}
				// E
				);
				// log.println(r.toString());
				ft.apply(r);
			}

			System.out.println("rows : " + ft.rows() + ", elapsed : " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", ops : " + watch.ops(ft.rows()));
			// watch.elapsed(true);
			// update

			// log.println("-- Modification");
			// final ThreadPool tp = ThreadPool.newPresentThread();
			for (long i = 1; i <= MAX; i += 1) {
				final long v = i;
				random.nextBytes(array);

				final Row r = Row.create(ft.meta(),  //
						new Object[] { //
								Long.toHexString(v), // A
								v, // B
								v, // C
								UUID.randomUUID().toString().replace("-", ""), // D
								IO.Hex.encode(array, 0, array.length) // E
						});
				// log.println(r.toString());
				final long affected = ft.apply(r);
				boolean forceLog = false;
				if (affected < 0 || forceLog) {
					System.err.println("position : " + affected + ", r : " + r);
					System.exit(0);
				}
			}
			// tp.await();
			// tp.shutdown();

			System.out.println("rows : " + ft.rows() + ", elapsed : " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", ops : " + watch.ops(ft.rows()));
			// log.close();
		}

		ft.close();

		final Boolean FULL_SEARCH = vindex > -1;
		if (FULL_SEARCH) {
			long founds = 0;

			final Table ft1 = Table.open(file, meta, new Logger() {
				@Override
				public void log(String fmt, Object... args) {
					System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
				}

				@Override
				public void error(String fmt, Object... args) {
					System.err.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + " " + String.format(fmt, args));
				}
			});

			for (int i = 1; i <= 2; i++) {
				watch.reset();
				final AtomicLong foundCount = new AtomicLong(0);
				try (final Cursor<Long> cursor = ft1.find("")) {
					Long v;
					while ((v = cursor.next()) != null) {
						final Row r = ft1.read(v);
						assert r != null;
						// System.out.println(v + " => " + r.getString(2));
						// System.out.println(String.format("%-6s => %-6s\t%s", v, r.getString(2), r));
						foundCount.incrementAndGet();
					}
				}
				founds = foundCount.get();
				System.out.println("FULL SEARCH-" + i + " : " + founds + ", elapsed : " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", ops : " + watch.ops(founds));
			}

			ft1.close();
		}
	}

}
