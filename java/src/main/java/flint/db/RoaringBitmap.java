/**
 * A compact integer set based on the Roaring Bitmap format.
 *
 * Overview
 * - 32-bit integers are split into a 16-bit key (high) and 16-bit value (low).
 * - Values sharing the same high key are stored in a per-key container.
 * - Two container types are used:
 *   - ArrayContainer: sorted short[] for sparse ranges (fast binary search, small memory).
 *   - BitmapContainer: fixed 65,536-bit bitmap for dense ranges (fast bit ops).
 * - Containers automatically switch representation based on a threshold for performance.
 *
 * Characteristics
 * - Typical operations (add/contains/remove) are O(1) to O(log n) per key.
 * - Set operations (or/and/andNot) merge containers by key and use double dispatch to
 *   apply the most efficient algorithm for the container pair.
 * - Memory usage scales with the number of populated keys and per-key density.
 */
package flint.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

/**
 * RoaringBitmap main class holding the map of high 16-bit keys to containers.
 * <pre>
 * byte[8201] :
 * key (int) = 4 bytes, 
 * type (byte) = 1 byte,
 * cardinality (int) = 4 bytes
 * words = 8192 bytes
 * => with key = 4 + 1 + 4 + 8192 = 8201 bytes
 * => payload only without key = 1 + 4 + 8192 = 8197 bytes
 * </pre>
 */
public final class RoaringBitmap {
	// Roaring splits 32-bit integers into a 16-bit high key and 16-bit low value within a container.
	private static final int KEY_BITS = 16;
	private static final int LOW_MASK = (1 << KEY_BITS) - 1;
	private static final int ARRAY_TO_BITMAP_THRESHOLD = 4096; // heuristic used by standard roaring

	private final TreeMap<Integer, Container> containers = new TreeMap<>();
	private int cardinality; // total number of set integers

	// ----- Constructors -----
	/** Creates an empty RoaringBitmap. */
	public RoaringBitmap() { }

	// ----- Basic ops -----
	/** Returns true if this set has no elements. */
	public boolean isEmpty() { return cardinality == 0; }

	/** Returns the number of integers in this set. */
	public int cardinality() { return cardinality; }

	/** Removes all elements. */
	public void clear() {
		containers.clear();
		cardinality = 0;
	}

	/** Returns true if the given 32-bit integer is present. */
	public boolean contains(int x) {
		if (x < 0) return false;
		int key = high(x);
		short low = low(x);
		Container c = containers.get(key);
		return c != null && c.contains(low);
	}

	/** Adds a 32-bit integer to the set. Negative values are not supported. */
	public void add(int x) {
		if (x < 0) throw new IllegalArgumentException("negative values not supported");
		int key = high(x);
		short low = low(x);
		Container c = containers.get(key);
		if (c == null) {
			c = new ArrayContainer();
			containers.put(key, c);
		}
		if (c.add(low)) {
			cardinality++;
			Container nc = c.optimizeAfterMutation();
			if (nc != c) containers.put(key, nc);
		}
	}

	/**
	 * Adds all integers in [startInclusive, endExclusive) to the set.
	 * Ignores empty or invalid ranges. Negative values are not supported.
	 */
	public void addRange(int startInclusive, int endExclusive) {
		if (startInclusive < 0 || endExclusive < 0) throw new IllegalArgumentException("negative range not supported");
		if (endExclusive <= startInclusive) return;
		int sKey = high(startInclusive);
		int eKey = high(endExclusive - 1);
		for (int key = sKey; key <= eKey; key++) {
			int lowStart = (key == sKey) ? (startInclusive & LOW_MASK) : 0;
			int lowEndExclusive = (key == eKey) ? ((endExclusive - 1) & LOW_MASK) + 1 : (1 << KEY_BITS);
			Container c = containers.get(key);
			if (c == null) {
				// If the range fills an entire container, create a bitmap directly
				if (lowStart == 0 && lowEndExclusive == (1 << KEY_BITS)) {
					BitmapContainer bc = new BitmapContainer();
					bc.fillAll();
					containers.put(key, bc);
					cardinality += bc.cardinality;
					continue;
				}
				c = new ArrayContainer();
				containers.put(key, c);
			}
			int added = c.addRange((short) lowStart, (short) lowEndExclusive);
			cardinality += added;
			Container nc = c.optimizeAfterMutation();
			if (nc != c) containers.put(key, nc);
		}
	}

	/** Removes a 32-bit integer from the set if present. */
	public void remove(int x) {
		if (x < 0) return;
		int key = high(x);
		short low = low(x);
		Container c = containers.get(key);
		if (c == null) return;
		if (c.remove(low)) {
			cardinality--;
			if (c.cardinality() == 0) {
				containers.remove(key);
			} else {
				Container nc = c.optimizeAfterMutation();
				if (nc != c) containers.put(key, nc);
			}
		}
	}

	// ----- Rank/Select -----
	/**
	 * Rank query: returns the number of elements less than or equal to x.
	 * If x is negative, returns 0. If x is greater than all elements, returns cardinality.
	 */
	public int rank(int x) {
		if (x < 0 || isEmpty()) return 0;
		int key = high(x);
		short low = low(x);
		int sum = 0;
		// sum all containers with key strictly less than current key
		for (Map.Entry<Integer, Container> e : containers.headMap(key).entrySet()) {
			sum += e.getValue().cardinality();
		}
		Container c = containers.get(key);
		if (c != null) sum += c.rank(low);
		return sum;
	}

	/**
	 * Select query: returns the k-th smallest element (0-based).
	 * @throws IndexOutOfBoundsException if k is out of range [0, cardinality).
	 */
	public int select(int k) {
		if (k < 0 || k >= cardinality) throw new IndexOutOfBoundsException("k=" + k);
		int remaining = k;
		for (Map.Entry<Integer, Container> e : containers.entrySet()) {
			Container c = e.getValue();
			int sz = c.cardinality();
			if (remaining < sz) {
				int low = c.select(remaining) & 0xFFFF;
				return toInt(e.getKey(), low);
			}
			remaining -= sz;
		}
		throw new IndexOutOfBoundsException("k=" + k);
	}

	// ----- Serialization -----
	/**
	 * Writes this bitmap to a DataOutput in a simple custom format.
	 * Format: [magic 'RBM1'][int containers][for each: int key, byte type, payload]
	 *  - type 'A': [int size][size x short values]
	 *  - type 'B': [int cardinality][1024 x long words]
	 * 
	 * NOTE: RoaringBitmap uses DataOutputStream which writes in BIG ENDIAN format.
	 * This is kept as-is for standard RoaringBitmap format compatibility.
	 */
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(0x52424D31); // 'RBM1'
		out.writeInt(containers.size());
		for (Map.Entry<Integer, Container> e : containers.entrySet()) {
			out.writeInt(e.getKey());
			e.getValue().write(out);
		}
	}

	/** Reads a bitmap from the given DataInput written by writeTo. */
	public static RoaringBitmap readFrom(DataInput in) throws IOException {
		int magic = in.readInt();
		if (magic != 0x52424D31) throw new IOException("Invalid magic");
		int n = in.readInt();
		RoaringBitmap rb = new RoaringBitmap();
		for (int i = 0; i < n; i++) {
			int key = in.readInt();
			Container c = readContainer(in);
			rb.containers.put(key, c);
			rb.cardinality += c.cardinality();
		}
		return rb;
	}

	/** Returns a compact byte[] by delegating to writeTo. */
	public byte[] toByteArray() {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			writeTo(dos);
			dos.flush();
			return baos.toByteArray();
		} catch (IOException e) {
			// ByteArrayOutputStream shouldn't throw, wrap defensively
			throw new RuntimeException(e);
		}
	}

	/** Constructs a bitmap from a byte[] created by toByteArray. */
	public static RoaringBitmap fromByteArray(byte[] data) {
		try {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
			return readFrom(dis);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// ----- Set algebra -----
	/** Returns the union of this bitmap and another (this ∪ other). */
	public RoaringBitmap or(RoaringBitmap other) {
		RoaringBitmap out = new RoaringBitmap();
		java.util.Iterator<Map.Entry<Integer, Container>> it1 = this.containers.entrySet().iterator();
		java.util.Iterator<Map.Entry<Integer, Container>> it2 = other.containers.entrySet().iterator();
		while (it1.hasNext() && it2.hasNext()) {
			Map.Entry<Integer, Container> a = it1.next();
			Map.Entry<Integer, Container> b = it2.next();
			// advance the smaller one accordingly; we can’t advance both blindly.
			if (a.getKey() < b.getKey()) {
				out.containers.put(a.getKey(), a.getValue().cloneContainer());
				out.cardinality += a.getValue().cardinality();
				// put b back by simulating a single-step lookahead using a cursor pattern
				// Since we cannot push back, we’ll iterate with manual merge logic below.
				// Rewind to a single-pass merge using lookahead pointers.
				// Break to a more explicit merge implementation.
				//
				// Fallback to explicit pointers
				Map.Entry<Integer, Container> pa = a;
				Map.Entry<Integer, Container> pb = b;
				while (true) {
					while (pa != null && pb != null && pa.getKey() < pb.getKey()) {
						out.containers.put(pa.getKey(), pa.getValue().cloneContainer());
						out.cardinality += pa.getValue().cardinality();
						pa = it1.hasNext() ? it1.next() : null;
					}
					if (pa == null || pb == null) { a = pa; b = pb; break; }
					if (pb.getKey() < pa.getKey()) {
						out.containers.put(pb.getKey(), pb.getValue().cloneContainer());
						out.cardinality += pb.getValue().cardinality();
						pb = it2.hasNext() ? it2.next() : null;
						continue;
					}
					// equal keys
					Container merged = pa.getValue().or(pb.getValue());
					out.containers.put(pa.getKey(), merged);
					out.cardinality += merged.cardinality();
					pa = it1.hasNext() ? it1.next() : null;
					pb = it2.hasNext() ? it2.next() : null;
				}
				// Drain remainders; use outer drains below
				if (a != null) {
					out.containers.put(a.getKey(), a.getValue().cloneContainer());
					out.cardinality += a.getValue().cardinality();
				}
				while (it1.hasNext()) {
					Map.Entry<Integer, Container> r = it1.next();
					out.containers.put(r.getKey(), r.getValue().cloneContainer());
					out.cardinality += r.getValue().cardinality();
				}
				if (b != null) {
					out.containers.put(b.getKey(), b.getValue().cloneContainer());
					out.cardinality += b.getValue().cardinality();
				}
				while (it2.hasNext()) {
					Map.Entry<Integer, Container> r = it2.next();
					out.containers.put(r.getKey(), r.getValue().cloneContainer());
					out.cardinality += r.getValue().cardinality();
				}
				return out;
			} else if (b.getKey() < a.getKey()) {
				out.containers.put(b.getKey(), b.getValue().cloneContainer());
				out.cardinality += b.getValue().cardinality();
				// Use same fallback path by swapping roles
				Map.Entry<Integer, Container> pa = b;
				Map.Entry<Integer, Container> pb = a;
				while (true) {
					while (pa != null && pb != null && pa.getKey() < pb.getKey()) {
						out.containers.put(pa.getKey(), pa.getValue().cloneContainer());
						out.cardinality += pa.getValue().cardinality();
						pa = it2.hasNext() ? it2.next() : null;
					}
					if (pa == null || pb == null) { a = pb; b = pa; break; }
					if (pb.getKey() < pa.getKey()) {
						out.containers.put(pb.getKey(), pb.getValue().cloneContainer());
						out.cardinality += pb.getValue().cardinality();
						pb = it1.hasNext() ? it1.next() : null;
						continue;
					}
					Container merged = pb.getValue().or(pa.getValue());
					out.containers.put(pb.getKey(), merged);
					out.cardinality += merged.cardinality();
					pb = it1.hasNext() ? it1.next() : null;
					pa = it2.hasNext() ? it2.next() : null;
				}
				if (a != null) {
					out.containers.put(a.getKey(), a.getValue().cloneContainer());
					out.cardinality += a.getValue().cardinality();
				}
				while (it1.hasNext()) {
					Map.Entry<Integer, Container> r = it1.next();
					out.containers.put(r.getKey(), r.getValue().cloneContainer());
					out.cardinality += r.getValue().cardinality();
				}
				if (b != null) {
					out.containers.put(b.getKey(), b.getValue().cloneContainer());
					out.cardinality += b.getValue().cardinality();
				}
				while (it2.hasNext()) {
					Map.Entry<Integer, Container> r = it2.next();
					out.containers.put(r.getKey(), r.getValue().cloneContainer());
					out.cardinality += r.getValue().cardinality();
				}
				return out;
			} else {
				Container merged = a.getValue().or(b.getValue());
				out.containers.put(a.getKey(), merged);
				out.cardinality += merged.cardinality();
			}
		}
		// drain remainders
		while (it1.hasNext()) {
			Map.Entry<Integer, Container> r = it1.next();
			out.containers.put(r.getKey(), r.getValue().cloneContainer());
			out.cardinality += r.getValue().cardinality();
		}
		while (it2.hasNext()) {
			Map.Entry<Integer, Container> r = it2.next();
			out.containers.put(r.getKey(), r.getValue().cloneContainer());
			out.cardinality += r.getValue().cardinality();
		}
		return out;
	}

	/** Returns the intersection of this bitmap and another (this ∩ other). */
	public RoaringBitmap and(RoaringBitmap other) {
		RoaringBitmap out = new RoaringBitmap();
		java.util.Iterator<Map.Entry<Integer, Container>> it1 = this.containers.entrySet().iterator();
		java.util.Iterator<Map.Entry<Integer, Container>> it2 = other.containers.entrySet().iterator();
		Map.Entry<Integer, Container> e1 = it1.hasNext() ? it1.next() : null;
		Map.Entry<Integer, Container> e2 = it2.hasNext() ? it2.next() : null;
		while (e1 != null && e2 != null) {
			int k1 = e1.getKey();
			int k2 = e2.getKey();
			if (k1 < k2) {
				e1 = it1.hasNext() ? it1.next() : null;
			} else if (k2 < k1) {
				e2 = it2.hasNext() ? it2.next() : null;
			} else {
				Container merged = e1.getValue().and(e2.getValue());
				if (merged.cardinality() > 0) {
					out.containers.put(k1, merged);
					out.cardinality += merged.cardinality();
				}
				e1 = it1.hasNext() ? it1.next() : null;
				e2 = it2.hasNext() ? it2.next() : null;
			}
		}
		return out;
	}

	/** Returns the difference of this bitmap and another (this − other). */
	public RoaringBitmap andNot(RoaringBitmap other) {
		RoaringBitmap out = new RoaringBitmap();
		for (var e : this.containers.entrySet()) {
			Container left = e.getValue();
			Container right = other.containers.get(e.getKey());
			Container res = (right == null) ? left.cloneContainer() : left.andNot(right);
			if (res.cardinality() > 0) {
				out.containers.put(e.getKey(), res);
				out.cardinality += res.cardinality();
			}
		}
		return out;
	}

	// ----- Iteration -----
	/** Returns a primitive int iterator over the set in ascending order. */
	public PrimitiveIterator.OfInt iterator() {
		return new PrimitiveIterator.OfInt() {
			private final java.util.Iterator<Map.Entry<Integer, Container>> outerIt = containers.entrySet().iterator();
			private Container currentContainer = null;
			private int currentKey = 0;
			private PrimitiveIterator.OfInt currentIt = null;

			private void ensureNextContainer() {
				while ((currentIt == null || !currentIt.hasNext()) && outerIt.hasNext()) {
					Map.Entry<Integer, Container> e = outerIt.next();
					currentKey = e.getKey();
					currentContainer = e.getValue();
					currentIt = currentContainer.intIterator(currentKey);
				}
			}

			@Override public boolean hasNext() {
				ensureNextContainer();
				return currentIt != null && currentIt.hasNext();
			}

			@Override public int nextInt() {
				if (!hasNext()) throw new NoSuchElementException();
				return currentIt.nextInt();
			}
		};
	}

	// ----- Equality / hash -----
	@Override public boolean equals(Object o) {
		if (this == o) return true;
	if (!(o instanceof RoaringBitmap)) return false;
	RoaringBitmap other = (RoaringBitmap) o;
	if (this.cardinality != other.cardinality) return false;
	return this.containers.equals(other.containers);
	}

	@Override public int hashCode() { return containers.hashCode() * 31 + cardinality; }

	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("RoaringBitmap(cardinality=").append(cardinality).append("){");
		boolean first = true;
		var it = iterator();
		int printed = 0;
		while (it.hasNext() && printed < 64) { // cap to avoid huge strings
			if (!first) sb.append(',');
			first = false;
			sb.append(it.nextInt());
			printed++;
		}
		if (it.hasNext()) sb.append(",...");
		sb.append('}');
		return sb.toString();
	}

	// ----- Internals -----
	/** Upper 16 bits (key) of a 32-bit integer. */
	private static int high(int x) { return (x >>> KEY_BITS) & LOW_MASK; }
	/** Lower 16 bits (value) of a 32-bit integer. */
	private static short low(int x) { return (short) (x & LOW_MASK); }

	private static int toInt(int key, int low) { return (key << KEY_BITS) | (low & LOW_MASK); }

	// Container abstraction
	/**
	 * Container represents all 16-bit values for a specific high key.
	 *
	 * Notes
	 * - Double-dispatch API lets each pair of container types choose an optimal
	 *   implementation for set operations.
	 * - optimizeAfterMutation() lets a container switch representation when it
	 *   becomes too dense/sparse.
	 */
	private interface Container extends Cloneable {
		int cardinality();
		boolean contains(short low);
		boolean add(short low); // returns true if newly added
		boolean remove(short low); // returns true if removed
		int addRange(short startInclusive, short endExclusive); // returns count newly added
		/** number of elements in this container less than or equal to low */
		int rank(short low);
		/** returns the value (low 16 bits) at given 0-based index inside this container */
		short select(int idx);
		// Set operations (double-dispatch entry points)
		Container or(Container other);
		Container and(Container other);
		Container andNot(Container other);
		// Double-dispatch specializations (other is the left operand, this is the right operand)
		Container orWithArray(ArrayContainer other);
		Container orWithBitmap(BitmapContainer other);
		Container andWithArray(ArrayContainer other);
		Container andWithBitmap(BitmapContainer other);
		Container andNotWithArray(ArrayContainer other);   // other - this (left minus right)
		Container andNotWithBitmap(BitmapContainer other); // other - this
		Container cloneContainer();
		Container optimizeAfterMutation();
		PrimitiveIterator.OfInt intIterator(int key);
		/** write type marker and payload */
		void write(DataOutput out) throws IOException;
	}

	// Array-backed container for small cardinalities
	/** Sorted short[] container used when density is low (sparse). */
	private static final class ArrayContainer implements Container {
		private short[] values = new short[4];
		private int size = 0;

		@Override public int cardinality() { return size; }

		@Override public boolean contains(short low) {
			int idx = Arrays.binarySearch(values, 0, size, low);
			return idx >= 0;
		}

		@Override public boolean add(short low) {
			int idx = Arrays.binarySearch(values, 0, size, low);
			if (idx >= 0) return false;
			idx = -idx - 1;
			ensureCapacity(size + 1);
			// shift
			if (idx < size) System.arraycopy(values, idx, values, idx + 1, size - idx);
			values[idx] = low;
			size++;
			return true;
		}

		@Override public int addRange(short startInclusive, short endExclusive) {
			if ((startInclusive & 0xFFFF) >= (endExclusive & 0xFFFF)) return 0;
			int added = 0;
			for (int v = startInclusive & 0xFFFF; v < (endExclusive & 0xFFFF); v++) {
				if (add((short) v)) added++;
			}
			return added;
		}

		@Override public boolean remove(short low) {
			int idx = Arrays.binarySearch(values, 0, size, low);
			if (idx < 0) return false;
			if (idx < size - 1) System.arraycopy(values, idx + 1, values, idx, size - idx - 1);
			size--;
			return true;
		}

		@Override public int rank(short low) {
			int idx = Arrays.binarySearch(values, 0, size, low);
			if (idx >= 0) return idx + 1; // count <= low
			int ins = -idx - 1; // first position greater than low
			return ins;
		}

		@Override public short select(int idx) {
			if (idx < 0 || idx >= size) throw new IndexOutOfBoundsException();
			return values[idx];
		}

		// Double dispatch entry points
		@Override public Container or(Container other) { return other.orWithArray(this); }
		@Override public Container and(Container other) { return other.andWithArray(this); }
		@Override public Container andNot(Container other) { return other.andNotWithArray(this); }

		// Specializations where 'other' is the left operand
		@Override public Container orWithArray(ArrayContainer other) {
			ArrayContainer out = new ArrayContainer();
			out.values = new short[Math.max(this.size + other.size, 4)];
			int i = 0, j = 0, k = 0;
			while (i < this.size && j < other.size) {
				short a = this.values[i];
				short b = other.values[j];
				if (a == b) { out.values[k++] = a; i++; j++; }
				else if ((a & 0xFFFF) < (b & 0xFFFF)) { out.values[k++] = a; i++; }
				else { out.values[k++] = b; j++; }
			}
			while (i < this.size) out.values[k++] = this.values[i++];
			while (j < other.size) out.values[k++] = other.values[j++];
			out.size = k;
			return (out.size >= ARRAY_TO_BITMAP_THRESHOLD) ? out.toBitmap() : out;
		}

		@Override public Container orWithBitmap(BitmapContainer other) {
			BitmapContainer out = other.cloneContainer();
			for (int i = 0; i < size; i++) out.add(values[i]);
			return out;
		}

		@Override public Container andWithArray(ArrayContainer other) {
			ArrayContainer out = new ArrayContainer();
			out.values = new short[Math.min(this.size, other.size)];
			int i = 0, j = 0, k = 0;
			while (i < this.size && j < other.size) {
				short a = this.values[i];
				short b = other.values[j];
				if (a == b) { out.values[k++] = a; i++; j++; }
				else if ((a & 0xFFFF) < (b & 0xFFFF)) i++; else j++;
			}
			out.size = k;
			return out;
		}

		@Override public Container andWithBitmap(BitmapContainer other) {
			ArrayContainer out = new ArrayContainer();
			for (int i = 0; i < size; i++) if (other.contains(values[i])) out.add(values[i]);
			return out;
		}

		@Override public Container andNotWithArray(ArrayContainer other) {
			// other - this
			ArrayContainer out = new ArrayContainer();
			out.values = new short[other.size];
			int i = 0, j = 0, k = 0; // i over other, j over this
			while (i < other.size && j < this.size) {
				short a = other.values[i];
				short b = this.values[j];
				if (a == b) { i++; j++; }
				else if ((a & 0xFFFF) < (b & 0xFFFF)) { out.values[k++] = a; i++; }
				else { j++; }
			}
			while (i < other.size) out.values[k++] = other.values[i++];
			out.size = k;
			return out;
		}

		@Override public Container andNotWithBitmap(BitmapContainer other) {
			// other - this
			ArrayContainer out = new ArrayContainer();
			for (int i = 0; i < BitmapContainer.WORDS; i++) {
				long w = other.words[i];
				if (w == 0) continue;
				long maskThis = 0L; // build mask of bits present in this array for this word
				int base = i << 6;
				// collect this.values that fall into this word
				// linear scan is fine given ARRAY_TO_BITMAP_THRESHOLD
				for (int p = 0; p < this.size; p++) {
					int v = this.values[p] & 0xFFFF;
					int wi = v >>> 6;
					if (wi == i) maskThis |= 1L << (v & 63);
					else if (wi > i) break;
				}
				long r = w & ~maskThis;
				while (r != 0) {
					long t = r & -r;
					int bit = Long.numberOfTrailingZeros(t);
					out.add((short) (base + bit));
					r ^= t;
				}
			}
			return out.optimizeAfterMutation();
		}

		private void ensureCapacity(int cap) {
			if (cap <= values.length) return;
			int n = Math.max(cap, values.length * 2);
			values = Arrays.copyOf(values, n);
		}

		private BitmapContainer toBitmap() {
			BitmapContainer bc = new BitmapContainer();
			for (int i = 0; i < size; i++) bc.add(values[i]);
			return bc;
		}

		@Override public ArrayContainer cloneContainer() {
			ArrayContainer c = new ArrayContainer();
			c.values = Arrays.copyOf(this.values, this.size);
			c.size = this.size;
			return c;
		}

		@Override public Container optimizeAfterMutation() {
			return (this.size >= ARRAY_TO_BITMAP_THRESHOLD) ? this.toBitmap() : this;
		}

		@Override public PrimitiveIterator.OfInt intIterator(final int key) {
			return new PrimitiveIterator.OfInt() {
				int idx = 0;
				@Override public boolean hasNext() { return idx < size; }
				@Override public int nextInt() {
					if (!hasNext()) throw new NoSuchElementException();
					return toInt(key, values[idx++] & 0xFFFF);
				}
			};
		}

		@Override public void write(DataOutput out) throws IOException {
			out.writeByte('A');
			out.writeInt(size);
			for (int i = 0; i < size; i++) out.writeShort(values[i] & 0xFFFF);
		}
	}

	// Bitmap-backed container for larger cardinalities
	/** 65,536-bit container used when density is high (dense). */
	private static final class BitmapContainer implements Container {
		// 2^16 bits -> 1024 longs
		private static final int WORDS = 1 << (KEY_BITS - 6); // 65536 / 64
		private long[] words = new long[WORDS];
		private int cardinality = 0;

		@Override public int cardinality() { return cardinality; }

		@Override public boolean contains(short low) {
			int v = low & 0xFFFF;
			int w = v >>> 6;
			int b = v & 63;
			return (words[w] & (1L << b)) != 0;
		}

		@Override public boolean add(short low) {
			int v = low & 0xFFFF;
			int w = v >>> 6;
			long mask = 1L << (v & 63);
			long before = words[w];
			if ((before & mask) != 0) return false;
			words[w] = before | mask;
			cardinality++;
			return true;
		}

		@Override public int addRange(short startInclusive, short endExclusive) {
			int s = startInclusive & 0xFFFF;
			int e = endExclusive & 0xFFFF;
			if (e <= s) return 0;
			int added = 0;
			for (int v = s; v < e; v++) if (add((short) v)) added++;
			return added;
		}

		@Override public boolean remove(short low) {
			int v = low & 0xFFFF;
			int w = v >>> 6;
			long mask = 1L << (v & 63);
			long before = words[w];
			if ((before & mask) == 0) return false;
			words[w] = before & ~mask;
			cardinality--;
			return true;
		}

		@Override public int rank(short low) {
			int v = low & 0xFFFF;
			int wi = v >>> 6;
			int sum = 0;
			for (int i = 0; i < wi; i++) sum += Long.bitCount(words[i]);
			long w = words[wi];
			int bits = v & 63;
			long mask = (bits == 63) ? -1L : ((1L << (bits + 1)) - 1); // include bit position
			sum += Long.bitCount(w & mask);
			return sum;
		}

		@Override public short select(int idx) {
			if (idx < 0 || idx >= cardinality) throw new IndexOutOfBoundsException();
			int acc = 0;
			for (int word = 0; word < WORDS; word++) {
				int pc = Long.bitCount(words[word]);
				if (idx < acc + pc) {
					int within = idx - acc;
					long w = words[word];
					// find within-th set bit
					int count = 0;
					while (true) {
						long t = w & -w;
						int bit = Long.numberOfTrailingZeros(t);
						if (count == within) return (short) ((word << 6) + bit);
						w ^= t;
						count++;
					}
				}
				acc += pc;
			}
			throw new IndexOutOfBoundsException();
		}

		// Double dispatch entry points
		@Override public Container or(Container other) { return other.orWithBitmap(this); }
		@Override public Container and(Container other) { return other.andWithBitmap(this); }
		@Override public Container andNot(Container other) { return other.andNotWithBitmap(this); }

		// Specializations where 'other' is the left operand
		@Override public Container orWithArray(ArrayContainer other) {
			BitmapContainer out = this.cloneContainer();
			for (int i = 0; i < other.size; i++) out.add(other.values[i]);
			return (out.cardinality < ARRAY_TO_BITMAP_THRESHOLD) ? out.toArray() : out;
		}

		@Override public Container orWithBitmap(BitmapContainer other) {
			BitmapContainer out = new BitmapContainer();
			for (int i = 0; i < WORDS; i++) {
				long w = this.words[i] | other.words[i];
				out.words[i] = w;
				out.cardinality += Long.bitCount(w);
			}
			return (out.cardinality < ARRAY_TO_BITMAP_THRESHOLD) ? out.toArray() : out;
		}

		@Override public Container andWithArray(ArrayContainer other) {
			ArrayContainer out = new ArrayContainer();
			for (int i = 0; i < other.size; i++) if (this.contains(other.values[i])) out.add(other.values[i]);
			return out;
		}

		@Override public Container andWithBitmap(BitmapContainer other) {
			BitmapContainer out = new BitmapContainer();
			for (int i = 0; i < WORDS; i++) {
				long w = this.words[i] & other.words[i];
				out.words[i] = w;
				out.cardinality += Long.bitCount(w);
			}
			return (out.cardinality < ARRAY_TO_BITMAP_THRESHOLD) ? out.toArray() : out;
		}

		@Override public Container andNotWithArray(ArrayContainer other) {
			// other - this
			ArrayContainer out = new ArrayContainer();
			for (int i = 0; i < other.size; i++) if (!this.contains(other.values[i])) out.add(other.values[i]);
			return out;
		}

		@Override public Container andNotWithBitmap(BitmapContainer other) {
			// other - this
			BitmapContainer out = new BitmapContainer();
			for (int i = 0; i < WORDS; i++) {
				long w = other.words[i] & ~this.words[i];
				out.words[i] = w;
				out.cardinality += Long.bitCount(w);
			}
			return (out.cardinality < ARRAY_TO_BITMAP_THRESHOLD) ? out.toArray() : out;
		}

		private ArrayContainer toArray() {
			ArrayContainer ac = new ArrayContainer();
			ac.values = new short[this.cardinality == 0 ? 4 : this.cardinality];
			int k = 0;
			for (int word = 0; word < WORDS; word++) {
				long w = words[word];
				while (w != 0) {
					long t = w & -w; // lowest set bit
					int bit = Long.numberOfTrailingZeros(t);
					ac.values[k++] = (short) ((word << 6) + bit);
					w ^= t;
				}
			}
			ac.size = k;
			return ac;
		}

		@Override public BitmapContainer cloneContainer() {
			BitmapContainer c = new BitmapContainer();
			c.words = Arrays.copyOf(this.words, this.words.length);
			c.cardinality = this.cardinality;
			return c;
		}

		void fillAll() {
			Arrays.fill(words, ~0L);
			// Adjust last word: we have exactly 65536 bits, 1024 words, so it's aligned; no trim required.
			cardinality = 1 << KEY_BITS;
		}

		@Override public PrimitiveIterator.OfInt intIterator(final int key) {
			return new PrimitiveIterator.OfInt() {
				int word = 0;
				long w = words[0];
				int base = 0;
				@Override public boolean hasNext() {
					while (word < WORDS && w == 0L) { word++; if (word < WORDS) { w = words[word]; base = word << 6; } }
					return word < WORDS && w != 0L;
				}
				@Override public int nextInt() {
					if (!hasNext()) throw new NoSuchElementException();
					long t = w & -w;
					int bit = Long.numberOfTrailingZeros(t);
					w ^= t;
					return toInt(key, base + bit);
				}
			};
		}

		@Override public Container optimizeAfterMutation() {
			return (this.cardinality < ARRAY_TO_BITMAP_THRESHOLD) ? this.toArray() : this;
		}

		@Override public void write(DataOutput out) throws IOException {
			out.writeByte('B');
			out.writeInt(cardinality);
			for (int i = 0; i < WORDS; i++) out.writeLong(words[i]);
		}
	}

	// ----- Serialization helpers -----
	private static Container readContainer(DataInput in) throws IOException {
		int type = in.readByte() & 0xFF;
		if (type == 'A') {
			int size = in.readInt();
			ArrayContainer ac = new ArrayContainer();
			ac.values = new short[Math.max(4, size)];
			ac.size = 0;
			for (int i = 0; i < size; i++) {
				ac.values[i] = (short) (in.readUnsignedShort() & 0xFFFF);
				ac.size++;
			}
			return ac.optimizeAfterMutation();
		} else if (type == 'B') {
			int card = in.readInt();
			BitmapContainer bc = new BitmapContainer();
			for (int i = 0; i < BitmapContainer.WORDS; i++) bc.words[i] = in.readLong();
			bc.cardinality = card; // trust stored cardinality
			return bc.optimizeAfterMutation();
		} else {
			throw new IOException("Unknown container type: " + type);
		}
	}
}
