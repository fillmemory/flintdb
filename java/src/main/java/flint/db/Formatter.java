/**
 * Formatter.java
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;


/**
 * Formatter interface for converting between Row objects and raw data formats.
 * Provides methods for formatting rows into a specific data format and parsing raw data back into Row objects.
 * 
 * @param <D> Decoder 
 * @param <E> Encoder
 */
public interface Formatter<D, E>  extends AutoCloseable {
	/**
	 * Formats a Row object into the specified data format.
	 * 
	 * <p>This method converts the Row into a raw data representation suitable for storage or transmission.</p>
	 * 
	 * @param row the Row object to format
	 * @return the formatted data as an instance of type E
	 * @throws IOException if formatting fails due to invalid data or other issues
	 */
	E format(final Row row) throws IOException;

	/**
	 * Parses raw data into a Row object.
	 * @param raw
	 * @return
	 * @throws IOException
	 */
	Row parse(final D raw) throws IOException;


	/**
	 * Release resources associated with the formatted data.
	 * @param raw
	 * @throws IOException
	 */
	void release(final E raw) throws IOException;


	static boolean storageFormat(final File file) throws IOException {
		if (file.exists()) {
			if (file.length() >= 4) {
				try (java.io.InputStream instream = new java.io.FileInputStream(file)) {
					final byte[] h = new byte[4];
					instream.read(h);
					if (h[0] == 'I' && h[1] == 'T' && h[2] == 'B' && h[3] == 'L')
						return true;
				}
			}
		}
		return false;
	}

	/**
	 * Binary row formatter for encoding/decoding Row objects.
	 */
	public static final class BINROWFORMATTER implements Formatter<IoBuffer, IoBuffer> {
		private final static boolean EXACT = "1".equals(System.getProperty("filedb.formatter.exact", "1"));
		private final int rowBytes;
		private final Column[] columns;
		private final Meta meta;
		private final DirectBufferPool pool;
		private static final BigInteger BIGINT_2_POW_64 = BigInteger.ONE.shiftLeft(64); // cached constant
		private final ThreadLocal<byte[]> tlScratch;
		
		private byte[] ensureScratch(final int sz) {
			byte[] b = tlScratch.get();
			if (b.length < sz) {
				int n = b.length;
				while (n < sz) n = Math.max(n << 1, 1024);
				b = new byte[n];
				tlScratch.set(b);
			}
			return b;
		}

		public BINROWFORMATTER(final int rowBytes, final Meta meta) {
			this.rowBytes = rowBytes;
			this.meta = meta;
			this.columns = meta.columns();
			this.pool = new DirectBufferPool(rowBytes, 16);
			// Initialize scratch buffer sized to the largest declared var-length column
			int init = 0;
			for (Column c : this.columns) {
				final short t = c.type();
				if (t == Column.TYPE_STRING || t == Column.TYPE_DECIMAL) {
					init = Math.max(init, c.bytes());
				}
			}
			final int initialSize = Math.max(init, 1024);
			this.tlScratch = ThreadLocal.withInitial(() -> new byte[initialSize]);
		}

		@Override
		public void release(final IoBuffer raw) throws IOException {
			pool.returnBuffer(raw);
		}

		@Override
		public void close() {
			pool.close();
		}

		@Override
		public IoBuffer format(final Row row) throws IOException {
			final IoBuffer bb = pool.borrowBuffer(rowBytes); // bb must be released after use
			// final IoBuffer bb = IoBuffer.allocate(rowBytes);
			final Object[] array = row.array();
			bb.putShort((short) array.length);
			for (int i = 0; i < array.length; i++) {
				put(bb, columns[i], i, row);
			}
			return (IoBuffer) bb.flip();
			// return raw;
		}

		// @ForceInline
		static void put(final IoBuffer bb, final Column column, final int i, final Row row) throws IOException {
			try {
				final Object[] array = row.array();
				final short type = column.type();
				final Object v = array[i];
				if (v == null) {
					bb.putShort((short) 0);
					return;
				}

				bb.putShort(type);

				switch (type) {
				case Column.TYPE_INT64:
					bb.putLong((Long) v);
					// bb.putLong((v instanceof Long) ? (Long) v : new BigDecimal(v.toString()).longValue());
					return;
				
				case Column.TYPE_INT:
					bb.putInt((Integer) v);
					// bb.putInt((v instanceof Integer) ? (Integer) v : new BigDecimal(v.toString()).intValue());
					return;
				case Column.TYPE_UINT:
					// Write as int, will be interpreted as unsigned when reading
					bb.putInt(((Long) v).intValue());
					return;
					
				case Column.TYPE_INT8:
					bb.put((Byte) v);
					return;
				case Column.TYPE_UINT8:
					bb.put(((Short) v).byteValue());
					return;

				case Column.TYPE_INT16:
					bb.putShort((Short) v);
					return;
				case Column.TYPE_UINT16:
					// Write as short, will be interpreted as unsigned when reading
					bb.putShort(((Integer) v).shortValue());
					return;

				case Column.TYPE_STRING:
					final byte[] string = v.toString().getBytes(StandardCharsets.UTF_8);
					if (EXACT && string.length > column.bytes())
						throw new IOException( //
								"BufferOverflow( STRING " //
										+ ", " + column.name() + " : " + v //
										+ ", " + string.length + " > " + column.bytes() //
										+ ", row : " + row //
						);
					bb.putShort((short) string.length);
					bb.put(string);
					return;
				case Column.TYPE_DATE:
					if (v instanceof java.util.Date)
						bb.put(IO.Date24Bits.encode((java.util.Date) v));	
					else
						bb.put(IO.Date24Bits.encode(v.toString()));
					return;
				case Column.TYPE_TIME:
					if (v instanceof java.util.Date)
						bb.putLong(((java.util.Date) v).getTime());
					else
						bb.putLong(Row.date(v.toString()).getTime());
						//bb.putLong(TL_TIME_FMT.get().parse(v.toString()).getTime());
					return;

				case Column.TYPE_DOUBLE:
					bb.putDouble((v instanceof Double) ? (Double) v : new BigDecimal(v.toString()).doubleValue());
					return;
				case Column.TYPE_FLOAT:
					bb.putFloat((v instanceof Float) ? (Float) v : new BigDecimal(v.toString()).floatValue());
					return;

				case Column.TYPE_UUID:
					final ULONGLONG uuid = (ULONGLONG) v;
					bb.putLong(uuid.high());
					bb.putLong(uuid.low());
					return;
				case Column.TYPE_IPV6:
					// byte[] inet = InetAddress.getByName(v.toString()).getAddress();
					// bb.put(inet);
					// if (inet.length == 4)
					// bb.put(new byte[16 - 4]);
					final ULONGLONG ipv6 = (ULONGLONG) v;
					bb.putLong(ipv6.high());
					bb.putLong(ipv6.low());
					return;

				case Column.TYPE_DECIMAL:
					// System.err.println(v + ", " + v.getClass());
					byte[] decimal = null;
					if (v instanceof BigDecimal) {
						final BigDecimal b = (BigDecimal) v;
						b.setScale(column.precision(), RoundingMode.FLOOR).unscaledValue().toByteArray();
						decimal = b.setScale(column.precision(), RoundingMode.FLOOR).unscaledValue().toByteArray();
					} else if (v instanceof BigInteger) {
						decimal = ((BigInteger) v).toByteArray();
					} else if (v instanceof byte[]) {
						decimal = (byte[]) v;
					} else if (v instanceof long[]) {
						decimal = new ULONGLONG((long[]) v).asBytes();
					} else if (v instanceof ULONGLONG) {
						decimal = ((ULONGLONG) v).asBigInteger().toByteArray();
					} else {
						final BigDecimal b = new BigDecimal(v.toString());
						b.setScale(column.precision(), RoundingMode.FLOOR).unscaledValue().toByteArray();
						decimal = b.setScale(column.precision(), RoundingMode.FLOOR).unscaledValue().toByteArray();
					}

				if (EXACT && decimal.length > column.bytes())
					throw new IOException( //
							"BufferOverflow( DECIMAL " //
									+ ", " + column.name() + " : " + v //
									+ ", " + decimal.length + " > " + column.bytes() //
									+ ", row : " + row //
					);
				bb.putShort((short) decimal.length);
				// Write decimal in Little Endian (LSB first)
				for (int j = decimal.length - 1; j >= 0; j--) {
					bb.put(decimal[j]);
				}
				return;				case Column.TYPE_BYTES:
					final byte[] bytes = (byte[]) v;
					if (bytes.length > column.bytes())
						throw new IOException( //
								"BufferOverflow( BYTES " //
										+ ", " + column.name() + " : " + v //
										+ ", " + bytes.length + " > " + column.bytes() //
										+ ", row : " + row //
						);
					bb.putShort((short) bytes.length);
					bb.put(bytes);
					return;

				case Column.TYPE_BLOB:
				case Column.TYPE_OBJECT:
					throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
				}

				throw new IOException("Unknown Type : " + type);
			} catch (ClassCastException ex) {
				// ex.printStackTrace();
				throw new IOException("Column : " + column.name() + ", [" + i + "]" + " " + ex.getMessage(), ex);
			}
		}

		@Override
		public Row parse(final IoBuffer raw) throws IOException { // optimized for MappedIoBuffer but not limited to it
			final short length = raw.getShort();
			final Object[] a = new Object[length];
			final Column[] cols = this.columns; // cached for fast access
			int sz;
			byte[] v;
			// System.out.println("length : " + length);
			try {
				for (int i = 0; i < length; i++) {
					final short type = raw.getShort();
					// System.out.println("type : " + ct);
					switch (type) {
					case Column.TYPE_NULL: // NULL (0)
						a[i] = null;
						break;
					case Column.TYPE_ZERO: // ZERO (1)
						a[i] = 0;
						break;

					case Column.TYPE_INT64:
						a[i] = raw.getLong();
						break;
					case Column.TYPE_INT:
						a[i] = raw.getInt();
						break;
					case Column.TYPE_UINT:
						// fast path: no temp byte[4], no extra decode
						a[i] = Integer.toUnsignedLong(raw.getInt());
						break;

					case Column.TYPE_STRING:
						sz = raw.getShort();
						v = ensureScratch(sz);
						raw.get(v, 0, sz);
						a[i] = new String(v, 0, sz, StandardCharsets.UTF_8);
						break;
					case Column.TYPE_DECIMAL:
						sz = raw.getShort();
						v = ensureScratch(sz);
						raw.get(v, 0, sz);
						// a[i] = new BigDecimal(new String(v));
						final short scale = cols[i].precision();
						if (sz <= 8) {
							// Fast path: decode up to 8-byte little-endian two's-complement into long, then scale
							long x = 0L;
							for (int j = sz - 1; j >= 0; j--) x = (x << 8) | (v[j] & 0xFFL);  // Little Endian: read backwards
							// Sign-extend if negative and shorter than 8 bytes (check MSB in last byte)
							if ((v[sz - 1] & 0x80) != 0 && sz < 8) {
								x |= (-1L) << (sz * 8);
							}
							a[i] = BigDecimal.valueOf(x, scale);
						} else if (sz <= 16) {
							// Extended fast path up to 16 bytes using two longs and limited BigInteger ops (Little Endian)
							int loLen = 8;
							int hiLen = sz - 8;
							long hi = 0L, lo = 0L;
							// Read lo (first 8 bytes) in little-endian
							for (int j = loLen - 1; j >= 0; j--) lo = (lo << 8) | (v[j] & 0xFFL);
							// Read hi (remaining bytes) in little-endian
							for (int j = sz - 1; j >= loLen; j--) hi = (hi << 8) | (v[j] & 0xFFL);
							// Sign-extend hi if negative and shorter than full 8 bytes (check MSB in last byte)
							if ((v[sz - 1] & 0x80) != 0 && hiLen < 8) {
								hi |= (-1L) << (hiLen * 8);
							}
							BigInteger big = BigInteger.valueOf(hi).shiftLeft(64);
							BigInteger loBI = BigInteger.valueOf(lo);
							if (lo < 0) loBI = loBI.add(BIGINT_2_POW_64);
							big = big.add(loBI);
							a[i] = new BigDecimal(big, scale);
						} else {
							// For >16 bytes, convert little-endian to big-endian for BigInteger
							byte[] bigEndianBytes = new byte[sz];
							for (int j = 0; j < sz; j++) bigEndianBytes[j] = v[sz - 1 - j];
							a[i] = new BigDecimal(new BigInteger(bigEndianBytes), scale);
						}
						break;

					case Column.TYPE_DATE:
						v = ensureScratch(3);
						raw.get(v, 0, 3);
						a[i] = IO.Date24Bits.decode(v);
						break;
					case Column.TYPE_TIME:
						a[i] = new java.sql.Timestamp(raw.getLong());
						break;

					case Column.TYPE_INT8:
						a[i] = (raw.get());
						break;
					case Column.TYPE_UINT8:
						// fast path: avoid per-row allocation and decode
						// returns Short to align with encoder usage (Short)
						a[i] = (short) (raw.get() & 0xFF);
						break;

					case Column.TYPE_INT16:
						a[i] = raw.getShort();
						break;
					case Column.TYPE_UINT16:
						// fast path: avoid per-row allocation and decode
						// returns Integer (0..65535) to align with encoder usage (Integer)
						a[i] = (raw.getShort() & 0xFFFF);
						break;

					case Column.TYPE_DOUBLE:
						a[i] = raw.getDouble();
						break;
					case Column.TYPE_FLOAT:
						a[i] = raw.getFloat();
						break;

					case Column.TYPE_UUID:
						a[i] = new ULONGLONG(raw.getLong(), raw.getLong());
						break;
					case Column.TYPE_IPV6:
						// v = new byte[16];
						// raw.get(v);
						// a[i] = InetAddress.getByAddress(v).toString();
						a[i] = new ULONGLONG(raw.getLong(), raw.getLong());
						break;

					case Column.TYPE_BYTES:
						sz = raw.getShort();
						v = new byte[sz];
						raw.get(v);
						a[i] = v;
						break;

					case Column.TYPE_BLOB:
					case Column.TYPE_OBJECT:
						throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
					}
				}
			} catch (java.nio.BufferUnderflowException | java.lang.IndexOutOfBoundsException ex) {
				throw new IOException(ex.getMessage(), ex);
			}

			if (a != null && a.length > 0)
				return new RowImpl(meta, a);
			return null;
		}

		// Bulk parse: parse up to maxRows from raw and feed each Row to the consumer. Returns rows parsed.
		public int parseBatch(final IoBuffer raw, final int maxRows, final java.util.function.Consumer<Row> consumer) throws IOException {
			if (maxRows <= 0) return 0;
			final Column[] cols = this.columns;
			int rows = 0;
			try {
				while (rows < maxRows && raw.remaining() > 0) {
					final short length = raw.getShort();
					final Object[] a = new Object[length];
					int sz; byte[] v;
					for (int i = 0; i < length; i++) {
						final short type = raw.getShort();
						switch (type) {
						case Column.TYPE_NULL: a[i] = null; break;
						case Column.TYPE_ZERO: a[i] = 0; break;
						case Column.TYPE_INT64: a[i] = raw.getLong(); break;
						case Column.TYPE_INT: a[i] = raw.getInt(); break;
						case Column.TYPE_UINT: a[i] = Integer.toUnsignedLong(raw.getInt()); break;
						case Column.TYPE_STRING:
							sz = raw.getShort();
							v = ensureScratch(sz);
							raw.get(v, 0, sz);
							a[i] = new String(v, 0, sz, StandardCharsets.UTF_8);
							break;
						case Column.TYPE_DECIMAL:
							sz = raw.getShort();
							v = ensureScratch(sz);
							raw.get(v, 0, sz);
							final short scale = cols[i].precision();
							if (sz <= 8) {
								long x = 0L;
								for (int j = 0; j < sz; j++) x = (x << 8) | (v[j] & 0xFFL);
								if ((v[0] & 0x80) != 0 && sz < 8) x |= (-1L) << (sz * 8);
								a[i] = BigDecimal.valueOf(x, scale);
							} else if (sz <= 16) {
								int hiLen = sz - 8;
								long hi = 0L, lo = 0L;
								for (int j = 0; j < hiLen; j++) hi = (hi << 8) | (v[j] & 0xFFL);
								for (int j = hiLen; j < sz; j++) lo = (lo << 8) | (v[j] & 0xFFL);
								if ((v[0] & 0x80) != 0 && hiLen < 8) hi |= (-1L) << (hiLen * 8);
								BigInteger big = BigInteger.valueOf(hi).shiftLeft(64);
								BigInteger loBI = BigInteger.valueOf(lo);
								if (lo < 0) loBI = loBI.add(BIGINT_2_POW_64);
								big = big.add(loBI);
								a[i] = new BigDecimal(big, scale);
							} else {
								a[i] = new BigDecimal(new BigInteger(v, 0, sz), scale);
							}
							break;
						case Column.TYPE_DATE:
							v = ensureScratch(3);
							raw.get(v, 0, 3);
							a[i] = IO.Date24Bits.decode(v);
							break;
						case Column.TYPE_TIME: a[i] = new java.sql.Timestamp(raw.getLong()); break;
						case Column.TYPE_INT8: a[i] = raw.get(); break;
						case Column.TYPE_UINT8: a[i] = (short) (raw.get() & 0xFF); break;
						case Column.TYPE_INT16: a[i] = raw.getShort(); break;
						case Column.TYPE_UINT16: a[i] = (raw.getShort() & 0xFFFF); break;
						case Column.TYPE_DOUBLE: a[i] = raw.getDouble(); break;
						case Column.TYPE_FLOAT: a[i] = raw.getFloat(); break;
						case Column.TYPE_UUID: a[i] = new ULONGLONG(raw.getLong(), raw.getLong()); break;
						case Column.TYPE_IPV6: a[i] = new ULONGLONG(raw.getLong(), raw.getLong()); break;
						case Column.TYPE_BYTES:
							sz = raw.getShort();
							byte[] bytes = new byte[sz];
							raw.get(bytes);
							a[i] = bytes;
							break;
						case Column.TYPE_BLOB:
						case Column.TYPE_OBJECT:
							throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
						}
					}
					consumer.accept(new RowImpl(meta, a));
					rows++;
				}
			} catch (java.nio.BufferUnderflowException | java.lang.IndexOutOfBoundsException ex) {
				throw new IOException(ex.getMessage(), ex);
			}
			return rows;
		}
	}

}
