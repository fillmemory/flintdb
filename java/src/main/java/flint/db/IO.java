/**
 * IO.java
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Utility class for file and stream operations, including path resolution,
 * byte array manipulation, and resource management.
 */
public final class IO {

	public static File path(final String path) {
		final String s = path //
				.replace("~", System.getProperty("user.home")) //
				.replace("${HOME}", System.getProperty("user.home")) //
		// .replace("${PWD}", System.getProperty("user.dir")) //
		;
		return new File(s);
	}

	public static void pipe(final InputStream istream, final OutputStream ostream) throws IOException {
		if (istream != null) {
			byte[] bb = new byte[2048];
			int n;
			while ((n = istream.read(bb)) > -1) {
				if (n > 0)
					ostream.write(bb, 0, n);
			}
		}
	}

    static File tempdir() {
        File d = new File("temp/" + ProcessHandle.current().pid());
        d.mkdirs();
        d.deleteOnExit();
        return d;
    }


    public static void close(java.io.Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

	/**
	 * A utility class for managing resources that need to be closed.
	 * It allows for automatic resource management, ensuring that resources are
	 * closed in reverse order of their registration.
	 * This is particularly useful for managing file streams, database connections,
	 * and other closeable resources.
	 */
	public static final class Closer implements AutoCloseable {
		final ArrayList<AutoCloseable> a = new ArrayList<>();

		/**
		 * Closer is used to manage resources that need to be closed.
		 */
		public Closer() {
		}

		/**
		 * Closer is used to manage resources that need to be closed.
		 * @param closeable the closeable resource
		 */
		public Closer(final AutoCloseable closeable) {
			register(closeable);
		}

		/**
		 * Closer is used to manage resources that need to be closed.
		 * @param lock the lock to manage
		 */
		public Closer(final Lock lock) {
			lock(lock);
		}

		/**
		 * Registers an object with a closeable resource.
		 * @param <T> the type of the object
		 * @param object the object to register
		 * @return the registered object
		 */
		public <T extends AutoCloseable> T register(final T object) {
			if (null != object)
				a.add(object);
			return object;
		}

		/**
		 * Registers a temporary file to be deleted when the Closer is closed.
		 * @param tempFile
		 * @return
		 */
		public File register(final File tempFile) {
			a.add((AutoCloseable) () -> {
                            tempFile.delete();
                        });
			return tempFile;
		}

		/**
		 * Registers an object with a closeable resource.
		 * This allows the object to be returned while ensuring that the closeable
		 * resource is closed when the Closer is closed.
		 * @param <T> the type of the object
		 * @param object the object to register
		 * @param closeable the closeable resource
		 * @return the registered object
		 */
		public <T> T register(final T object, final AutoCloseable closeable) {
			a.add(closeable);
			return object;
		}

		/**
		 * Registers an object with a closeable resource using a Consumer.
		 * This allows the object to be returned while ensuring that the closeable
		 * resource is closed when the Closer is closed.
		 * @param <T> the type of the object
		 * @param object the object to register
		 * @param closeable the closeable resource
		 * @return the registered object
		 */
		public <T> T register(final T object, final Consumer<T> closeable) {
			a.add(() -> {
				closeable.accept(object);
			});
			return object;
		}

		/**
		 * Registers a runnable with a closeable resource.
		 * This allows the runnable to be executed while ensuring that the closeable
		 * resource is closed when the Closer is closed.
		 * @param runnable the runnable to register
		 * @param closeable the closeable resource
		 */
		public void register(final Runnable runnable, final AutoCloseable closeable) {
			a.add(closeable);
		}

		public void lock(final Lock lock) {
			lock.lock();
			a.add(() -> {
				lock.unlock();
			});
		}

		@SuppressWarnings("unchecked")
		public <T extends AutoCloseable> List<T> list(final Class<T> clazz) {
			final List<T> matches = new ArrayList<>();
			for (AutoCloseable e : a) {
				if (clazz.isAssignableFrom(e.getClass())) {
					matches.add((T) e);
				}
			}
			return matches;
		}

		/**
		 * Close all registered resources in reverse order
		 */
		@Override
		public void close() { // throws IOException
			for (int i = a.size() - 1; i >= 0; i--) {
				final AutoCloseable o = a.get(i);
				try {
					o.close();
					// } catch (IOException ex) {
					// throw ex;
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		}

		public <T extends AutoCloseable> T close(final T object) throws Exception {
			final int i = a.indexOf(object);
			if (i > -1)
				a.remove(i);
			object.close();
			return object;
		}
	}

	public static String readableBytesSize(final long bytes) {
		long v = bytes;
		if (v <= 0)
			return "0";

		final String[] units = new String[] { "B", "K", "M", "G", "T" };

		int digitGroups = (int) (Math.log10(v) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(v / Math.pow(1024, digitGroups)) + "" + units[digitGroups];
	}

	static final class Hex {
		static byte[] decode(final String string) throws IOException {
			final char[] a = string.toCharArray();
			if ((a.length & 0x01) != 0)
				throw new IOException("Odd number of characters.");

			final int len = a.length;
			final int outLen = len >> 1;
			final byte[] out = new byte[outLen];
			// two characters form the hex value.
			for (int i = 0, j = 0; j < len; i++) {
				int f = Character.digit(a[j], 16) << 4;
				j++;
				f = f | Character.digit(a[j], 16);
				j++;
				out[i] = (byte) (f & 0xFF);
			}
			return out;
		}

		static String encode(final byte[] bytes, final int off, final int count) {
			final char[] HEX_ARRAY = "0123456789abcded".toCharArray();
			final char[] hexChars = new char[count * 2];
			final int l = Math.min(off + count, bytes.length);
			for (int j = off; j < l; j++) {
				final int v = bytes[j] & 0xFF;
				hexChars[j * 2] = HEX_ARRAY[v >>> 4];
				hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
			}
			return new String(hexChars);
		}

		static String encode(final byte[] bytes) {
			return encode(bytes, 0, bytes.length);
		}

        @SuppressWarnings("unused")
		static String encode(final ByteBuffer bb) {
			final char[] HEX_ARRAY = "0123456789abcded".toCharArray();
			final int l = bb.remaining();
			final char[] hexChars = new char[l * 2];
			for (int j = 0; j < l; j++) {
				final int v = bb.get() & 0xFF;
				hexChars[j * 2] = HEX_ARRAY[v >>> 4];
				hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
			}
			return new String(hexChars);
		}
	}
	
	/**
	 * <pre>
	 * 24Bits Date Storage class
	 * year  : 14bits 0000~9999
	 * month : 4bits 1~12
	 * day   : 5bits 1~31
	 * bits layout : year:14b|month:4b|day:5b
	 * </pre>
	 */
	static final class Date24Bits {
		static final int YEAR0 = 0000;

		static final java.util.regex.Pattern DATE_SPLIT = java.util.regex.Pattern.compile("[- ]");

		/**
		 * encode to 24Bits Date
		 * 
		 * @param datetime yyyy-MM-dd formatted String
		 * @return 24Bits Date
		 */
		public static byte[] encode(String datetime) {
			//if (datetime != null && datetime.length() >= 10 && datetime.indexOf('-') == 4 && datetime.indexOf('-', 5) == 7) {
			if (datetime != null && datetime.length() >= 10 && datetime.charAt(4) == '-' && datetime.charAt(7) == '-') {
				// final String[] a = datetime.substring(0, 10).split("[- ]");
				final String[] a = DATE_SPLIT.split(datetime, 3);
				if (a.length >= 3) {
					try {
						final int year = Integer.parseInt(a[0]);
						final int month = Integer.parseInt(a[1]);
						final int day = Integer.parseInt(a[2]);
						return encode(year, month, day);
					} catch (NumberFormatException ex) {
						throw new java.lang.RuntimeException("Could not parse date : " + datetime);
					}
				}
			}

			throw new java.lang.RuntimeException("Could not parse date : " + datetime);
		}
 
		public static byte[] encode(int y, int M, int d) {
			final int v = (int) (((y - YEAR0) << 9) + (M << 5) + d);
			final byte[] bytes = new byte[3];
			bytes[2] = (byte) (v & 0xff);
			bytes[1] = (byte) ((v >> 8) & 0xff);
			bytes[0] = (byte) ((v >> 16) & 0xff);
			return bytes;
		}

		public static byte[] encode(java.util.Date d) {
			if (d != null) {
				//java.util.Calendar cal = java.util.Calendar.getInstance();
				//cal.setTime(d);
				// return encode(cal.get(java.util.Calendar.YEAR), cal.get(java.util.Calendar.MONTH) + 1, cal.get(java.util.Calendar.DAY_OF_MONTH));

				var i = d.toInstant().atZone(Row.ZONE_ID);
				return encode(i.getYear(), i.getMonthValue(), i.getDayOfMonth());
			}
			throw new java.lang.NullPointerException("Date is null");
		}


		/**
		 * decode from 24Bits Date
		 * 
		 * @param d24 24Bits Date
		 * @return Date String (yyyy-MM-dd formatted)
		 */
		public static java.util.Date decode(final byte[] b3) { 
			final int d24 = ((b3[0] & 0xff) << 16) + ((b3[1] & 0xff) << 8) + (b3[2] & 0xff);
			final int year = (d24 >> 9) + YEAR0;
			final int month = (d24 >> 5) & 15;
			final int day = d24 & 31;
			// return String.format("%04d-%02d-%02d", year, month, day);
			return java.sql.Timestamp.from(java.time.LocalDate.of(year, month, day).atStartOfDay(Row.ZONE_ID).toInstant());
		}
	}


	/**
	 * A simple stopwatch utility for measuring elapsed time.
	 * It supports start, stop, pause, resume, and reset functionalities.
	 * It can also calculate operations per second based on elapsed time.
	 */
	public static final class StopWatch {
		private long start = -1L;
		private long stop = -1L;
		private long time = 0L;
		private long suspend = 0L;
		private long resume = 0L;
		private boolean running = false;

		public StopWatch() {
			start();
		}

		public long start() {
			reset();
			return start;
		}

		public void reset() {
			suspend = resume = start = now();
			time = 0;
			running = true;
		}

		public void pause() {
			if (running) {
				running = false;

				final long ts = now();
				time += (ts - resume);
				resume = suspend = ts;
			}
		}

		public void resume() {
			if (!running) {
				resume = now();
				running = true;
			}
		}

		public long stop() {
			pause();
			stop = suspend;
			return stop;
		}

		public long elapsed() {
			if (!running)
				return time;
			return time + (now() - resume);
		}

		public long elapsed(boolean reset) {
			final long v = elapsed();
			if (reset)
				reset();
			return v;
		}

		private long now() {
			return System.currentTimeMillis();
		}

		public static long ops(final int count, final long ms) {
			return ops((long) count, ms);
		}

		public static long ops(final long count, final long ms) {
			// return (ms < 1L) ? Math.round(count * (1000.0d / Math.max(1, ms))) : Math.round(count / (ms / 1000.0d));
			// System.out.println("count : " + count + ", ms : " + ms);
			return Math.round(count / (Math.max(1, ms) / 1000.0d));
		}

		public long ops(final int count) {
			return ops((long) count, elapsed());
		}

		public long ops(final long count) {
			return ops(count, elapsed());
		}

		public String humanReadableTime() {
			return humanReadableTime(elapsed());
		}

		public static String humanReadableTime(final long ms) {
			try {
				long s = (ms / 1000L);
				if (s > (365 * 24 * 3600)) {
					int days = (int) (s / (24 * 3600));
					int years = (days / 365);
					int dd = (days % 365);
					return dd > 0 ? years + "Y" + dd + "D" : years + "Y";
				} else if (s > (24 * 3600)) {
					int days = (int) (s / (24 * 3600));
					s = s % (24 * 3600);
					int hours = (int) (s / 3600);
					if (hours > 0)
						return String.format("%dD%dh", days, hours);
					return String.format("%dD", days);
				} else if (s > 3600) {
					int hours = (int) (s / 3600);
					int remainder = (int) s - hours * 3600;
					int mins = remainder / 60;
					if (mins > 0)
						return String.format("%dh%dm", hours, mins);
					return String.format("%dh", hours);
				} else if (s > 60) {
					int mins = (int) (s / 60);
					int remainder = (int) (s - mins * 60);
					int secs = remainder;
					if (secs > 0)
						return String.format("%dm%ds", mins, secs);
					return String.format("%dm", mins);
				} else {
					if (s > 1)
						return String.format("%ds", s);
				}
			} catch (java.lang.NumberFormatException ex) {
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return String.format("%dms", ms);
		}
	}
}
