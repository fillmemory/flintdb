/**
 * ULONGLONG.java
 */
package flint.db;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * 16byte (128Bit) unsigned long long.
 */
final class ULONGLONG implements Comparable<ULONGLONG> {
	static final int BYTES = Long.BYTES * 2;

	final long[] v = new long[2];

	ULONGLONG(final long high, final long low) {
		v[0] = high;
		v[1] = low;
	}

	ULONGLONG(final long[] a) {
		v[0] = a[0];
		v[1] = a[1];
	}

	ULONGLONG(final ByteBuffer bb) {
		v[0] = bb.getLong();
		v[1] = bb.getLong();
	}

	public long high() {
		return v[0];
	}

	public long low() {
		return v[1];
	}

	public byte[] asBytes() {
		final byte[] a = new byte[BYTES];
		a[0] = (byte) ((v[0] >> 56) & 0xFF);
		a[1] = (byte) ((v[0] >> 48) & 0xFF);
		a[2] = (byte) ((v[0] >> 40) & 0xFF);
		a[3] = (byte) ((v[0] >> 32) & 0xFF);
		a[4] = (byte) ((v[0] >> 24) & 0xFF);
		a[5] = (byte) ((v[0] >> 16) & 0xFF);
		a[6] = (byte) ((v[0] >> 8) & 0xFF);
		a[7] = (byte) (v[0] & 0xFF);
		//
		a[8] = (byte) ((v[1] >> 56) & 0xFF);
		a[9] = (byte) ((v[1] >> 48) & 0xFF);
		a[10] = (byte) ((v[1] >> 40) & 0xFF);
		a[11] = (byte) ((v[1] >> 32) & 0xFF);
		a[12] = (byte) ((v[1] >> 24) & 0xFF);
		a[13] = (byte) ((v[1] >> 16) & 0xFF);
		a[14] = (byte) ((v[1] >> 8) & 0xFF);
		a[15] = (byte) (v[1] & 0xFF);

		// for (int i = 0; i < v.length; i++) { // optimized
		// 	long value = v[i];
		// 	for (int j = 0; j < Long.BYTES; j++) {
		// 		a[i * Long.BYTES + j] = (byte) ((value >> (56 - j * 8)) & 0xFF);
		// 	}
		// }
		return a;
	}

	public BigDecimal asBigDecimal() {
		final byte[] a = asBytes();
		return new BigDecimal(new BigInteger(a));
	}

	public BigInteger asBigInteger() {
		final byte[] a = asBytes();
		// return new BigInteger(1, a);
		return new BigInteger(a);
	}

	public java.net.InetAddress asIPV6() throws java.net.UnknownHostException {
		final byte[] a = asBytes();
		return java.net.InetAddress.getByAddress(a);
	}

	public java.util.UUID asUUID() {
		return new java.util.UUID(high(), low());
	}

	static ULONGLONG decodeHexString(final String s) throws IOException {
		final byte[] a = new byte[16];
		final char[] ca = ((s.length() & 0x01) == 0) ? s.toCharArray() : "0".concat(s).toCharArray();
		decodeHex(ca, a, a.length - (ca.length / 2));
		return new ULONGLONG(ByteBuffer.wrap(a));
	}

	static int decodeHex(final char[] data, final byte[] out, final int outOffset) throws IOException {
		final char[] a = data;
		if ((data.length & 0x01) != 0)
			throw new IOException("Odd number of characters.");

		final int len = a.length;
		final int outLen = len >> 1;
		// two characters form the hex value.
		for (int i = outOffset, j = 0; j < len; i++) {
			int f = Character.digit(a[j], 16) << 4;
			j++;
			f = f | Character.digit(a[j], 16);
			j++;
			out[i] = (byte) (f & 0xFF);
		}
		return outLen;
	}

	private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public String toHexString() {
		final byte[] a = new byte[BYTES];
		a[0] = (byte) ((v[0] >> 56) & 0xFF);
		a[1] = (byte) ((v[0] >> 48) & 0xFF);
		a[2] = (byte) ((v[0] >> 40) & 0xFF);
		a[3] = (byte) ((v[0] >> 32) & 0xFF);
		a[4] = (byte) ((v[0] >> 24) & 0xFF);
		a[5] = (byte) ((v[0] >> 16) & 0xFF);
		a[6] = (byte) ((v[0] >> 8) & 0xFF);
		a[7] = (byte) (v[0] & 0xFF);
		//
		a[8] = (byte) ((v[1] >> 56) & 0xFF);
		a[9] = (byte) ((v[1] >> 48) & 0xFF);
		a[10] = (byte) ((v[1] >> 40) & 0xFF);
		a[11] = (byte) ((v[1] >> 32) & 0xFF);
		a[12] = (byte) ((v[1] >> 24) & 0xFF);
		a[13] = (byte) ((v[1] >> 16) & 0xFF);
		a[14] = (byte) ((v[1] >> 8) & 0xFF);
		a[15] = (byte) (v[1] & 0xFF);

		final int dataLength = a.length;
		final char[] out = new char[dataLength << 1];
		for (int i = 0, j = 0; i < 0 + dataLength; i++) {
			out[j++] = DIGITS_LOWER[(0xF0 & a[i]) >>> 4];
			out[j++] = DIGITS_LOWER[0x0F & a[i]];
		}
		return new String(out);
	}

	@Override
	public String toString() {
		// return asBigInteger().toString();
		return new BigInteger(1, asBytes()).toString();
	}

	@Override
	public boolean equals(final Object o) {
		final ULONGLONG ll = (ULONGLONG) o;
		return v[0] == ll.v[0] && v[1] == ll.v[1];
	}

	@Override
	public int compareTo(final ULONGLONG ll) {
		int d = 0;
		for (int i = 0; i < v.length; i++) {
			d = Long.compareUnsigned(v[i], ll.v[i]);
			if (d != 0)
				return d;
		}
		return d;
	}

	public static int compare(final ULONGLONG o1, final ULONGLONG o2) {
		return o1.compareTo(o2);
	}
}
