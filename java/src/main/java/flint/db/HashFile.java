/**
 * HashFile.java
 */
package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Hash Index Key File
 * 
 * <pre>
 * * Search
 * Big O          : Average,Worst = O(1) + O(log( n / HASH_SIZE ))
 * Hash Function  : (VALUE & 0x7FFFFFFF) % (HASH_SIZE)
 * Leaf Algorithm : Binary Search
 * 
 * * Node Layout : Fixed Depth 3
 * +--------------------------------------------------------------------------------------------------------------------------+
 * | IHEAD | ...                                                                                                              |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * | INTERNAL | INTERNAL | ...                                                                                                |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * | LEAF | LEAF | ...                                                                                                        |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * 
 * 
 * * ROOT : 8MB (HASH SIZE : 1024*1024)
 * +--------------------------------------------------------------------------------------------------------------------------+
 * |IHEAD : 8B | IHEAD : 8B |                                                                                   ... 1024*1024 |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * 
 * * IHEAD :  16B
 * +-------------------------------------------------------------------+
 * | FRONT INTERNAL OFFSET : 8B | TAIL INTERNAL OFFSET : 8B            |
 * +-------------------------------------------------------------------+
 * 
 * * INTERNAL : 112B (MAX : 12), Linked List
 * +--------------------------------------------------------------------------------------------------------------------------+
 * |[LEFT : 8B | RIGHT : 8B] | [LEAF OFFSET : 8B] | [LEAF OFFSET : 8B] |                                         ... MAX : 12 | 
 * +--------------------------------------------------------------------------------------------------------------------------+
 * 
 * * LEAF :  112B (MAX : 16), keep sorted and search w/ bin
 * +--------------------------------------------------------------------------------------------------------------------------+
 * |KEY : 8B | KEY : 8B |                                                    			                         ... MAX : 14 |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * 
 * </pre>
 */
final class HashFile implements Closeable {
	private final Storage storage;
	private final IoBuffer header;

	static final short IHEAD_BYTES = Long.BYTES * 2;
	static final short INTERNAL_ELEMEMTS_MAX = 12;
	static final short LEAF_KEYS_MAX = INTERNAL_ELEMEMTS_MAX + 2;
	static final short LEAF_BYTES = Long.BYTES * LEAF_KEYS_MAX;
	static final short INTERNAL_BYTES = LEAF_BYTES;
	private final int HASH_SIZE;

	private final ROOT root;
	private long count = 0L;

	static final long OFFSET_NULL = 0L;
	static final long KEY_NULL = -1L;

	/**
	 * 
	 * @param file      file
	 * @param m         HASH SIZE
	 * @param mutable   writing + reading / reading
	 * @param increment increment size
	 * @param hf        data sorter
	 * @throws IOException
	 */
	public HashFile(final File file, final int m, //
			final boolean mutable, final int increment,//
			final HashFunction hf //
	) throws IOException {
		if (m < 16)
			throw new RuntimeException("m >= 16");

		this.HASH_SIZE = m;

		this.storage = Storage.create(new Storage.Options() //
				.file(file) //
				.mutable(mutable) //
				// .headerBytes(HEAD_SZ) //
				.blockBytes((short) (LEAF_BYTES)) //
				.extraHeaderBytes((HASH_SIZE * IHEAD_BYTES)) //
				.increment(Math.max(increment, (LEAF_BYTES + 16) * 1024)) // disable : -1, 1024 * 1024
		);
		this.header = storage.head(12); // signature : 4 + count : 8
		this.header.rewind();

		final byte x = this.header.get();
		if (x > 0) { // check file is empty
			this.header.position(4); // signature
			count = this.header.getLong(); // count

			root = new ROOT(storage, HASH_SIZE, hf);
		} else {
			header.rewind();
			header.put(new byte[] { 'H', 'A', 'S', 'H' });
			header.putLong(0L);

			root = new ROOT(storage, HASH_SIZE, hf);

			storage.write(IoBuffer.wrap(memset(new byte[LEAF_BYTES], (byte) '#'))); // reserved;
		}
	}

	@Override
	public void close() throws IOException {
		// System.out.println("storage : " + storage.bytes());
		storage.close();
	}

	public long bytes() throws IOException {
		return storage != null ? storage.bytes() : 0;
	}

	Storage storage() {
		return this.storage;
	}

	private void flush() {
		final IoBuffer mbb = header.slice(4, Long.BYTES);
		mbb.putLong(count); //
	}

	public long count() throws IOException {
		return count;
	}

	public void put(final long key) throws IOException {
		final Leaf ok = root.put(key);
		if (ok == null) {
			// System.err.println("put error : " + key);
			return;
		}
		count++;
		flush();
	}

	public Long get(final long key) throws IOException {
		return root.find(key) == null ? null : key;
	}

	public Leaf find(final long key) throws IOException {
		return root.find(key);
	}

	public Long find(final Comparable<Long> key) throws IOException {
		return root.find(key);
	}

	public long traverse(final Consumer<Long> visitor) throws Exception {
		return root.traverse(visitor);
	}

	void trace(final Object x) throws Exception {
		root.trace(x);
	}

	public static interface HashFunction extends Comparator<Long> {
		int hash(final long key);
	}

	static interface Node {
	}

	static long[] join(final int capacity, final long[] source, final long[] target, final int offset, final int d, final long key) {
		if (d == 0)
			return null;
		// System.out.println("key : " + key + ", offset : " + offset + ", d : " + d + ", s : " + Arrays.toString(source));
		int i = 0, j = 0;
		for (; i < (offset + (d < 0 ? 0 : 1)); i++, j++)
			target[i] = source[j];

		if (i >= capacity)
			return new long[] { key };

		target[i++] = key;

		for (; i < target.length; i++, j++)
			target[i] = source[j];

		if (j < source.length)
			return new long[] { source[source.length - 1] };
		return null;
	}

	static Leaf[] join(final int capacity, final Leaf[] source, final Leaf[] target, final int offset, final int d, final Leaf key) {
		if (d == 0)
			return null;
		// System.out.println("key : " + key + ", offset : " + offset + ", d : " + d + ", s : " + Arrays.toString(source));
		if (d < 0 && offset > 0) {
			int i = 0, j = 0;
			for (; i < offset; i++, j++)
				target[i] = source[j];

			if (i >= capacity)
				return new Leaf[] { key };

			target[i++] = key;

			for (; i < target.length; i++, j++)
				target[i] = source[j];

			if (j < source.length)
				return new Leaf[] { source[source.length - 1] };
		} else {
			int i = 0, j = 0;
			for (; i <= offset; i++, j++)
				target[i] = source[j];

			if ((offset + 1) == target.length)
				return new Leaf[] { key };

			target[i++] = key;
			for (; i < target.length; i++, j++)
				target[i] = source[j];

			if (j < source.length)
				return new Leaf[] { source[source.length - 1] };
		}
		return null;
	}

	static long[] concat(final long[] a, final long[] b) {
		final long[] v = new long[a.length + b.length];
		int j = 0;
		for (int i = 0; i < a.length; i++, j++)
			v[j] = a[i];
		for (int i = 0; i < b.length; i++, j++)
			v[j] = b[i];
		return v;
	}

	static int range(final int max, final int v) {
		if (v > max)
			return max;
		if (v < 0)
			return 0;
		return v;
	}

	static byte[] memset(final byte[] bytes, final byte v) {
		for (int i = 0; i < bytes.length; i++)
			bytes[i] = v;
		return bytes;
	}

	// static void DEBUG(final boolean on, final String s) {
	// 	if (!on)
	// 		return;
	// 	System.out.println("DEBUG " + s);
	// }

	static final class ROOT implements Node {
		final IoBuffer root;
		final Storage storage;
		final int HASH_SIZE;
		final int ROOT_NODE_BYTES;
		final HashFunction hf;

		public ROOT(final Storage storage, final int HASH_MAX, final HashFunction hf) throws IOException {
			this.HASH_SIZE = HASH_MAX;
			this.ROOT_NODE_BYTES = (HASH_MAX * IHEAD_BYTES);
			this.storage = storage;
			this.root = storage.head(Storage.HEADER_BYTES, ROOT_NODE_BYTES);
			this.hf = hf;
			// System.err.println("root : " + root.isReadOnly() + ", " + root.remaining());
		}

		private Leaf put(final long key) throws IOException {
			final int hash = (hf.hash(key) & 0x7FFFFFFF) % HASH_SIZE;
			final IHEAD head = new IHEAD(storage, root.slice(hash * (IHEAD_BYTES), IHEAD_BYTES));
			long next = head.front();
			// System.err.println("put head hash : " + hash + " , front : " + next + " , key : " + key);
			if (OFFSET_NULL == next) {
				final Internal internal = Internal.create(storage);
				final Leaf leaf = Leaf.create(storage);
				leaf.update(new long[] { key });
				leaf.flush();
				internal.leaves(new Leaf[] { leaf });
				internal.flush();
				head.front(internal.offset());
				head.tail(internal.offset());
				head.flush();
				return leaf;
			} else {
				final List<Internal> internals = head.internals();
				final Position p1 = indexOf(internals, key);
				final Internal internal = internals.get(range(internals.size(), p1.offset + (p1.d < 0 ? p1.d : 0))); // + (p1.d < 0 ? -1 : 0)

				// final boolean DEBUG = (key == 0); // (key == 3);// (key == 89) || (key == 95);
				// DEBUG(DEBUG, "put key : " + key + ", " + Arrays.toString(internals.toArray()));

				final Leaf[] leaves = internal.leaves();
				final Position p2 = indexOf(leaves, key);
				// DEBUG(DEBUG, "put key : " + key + ", L1 : " + p1 + ", " + internal);

				final int li = range(leaves.length - 1, p2.offset + (p2.d < 0 ? p2.d : 0));
				final Leaf leaf = leaves[li]; // + (p2.offset > 0 && p2.d < 0 ? p2.d : 0)
				final long[] keys = leaf.keys();
				final Position p3 = indexOf(keys, key);
				// DEBUG(DEBUG, "put key : " + key + ", L2 : " + p2 + ", LF : " + leaf + " FROM " + Arrays.toString(leaves));
				// DEBUG(DEBUG, "put key : " + key + ", L3 : " + p3);

				if (p3.d != 0) {
					final long[] tmp = new long[keys.length + ((keys.length < LEAF_KEYS_MAX) ? 1 : 0)];
					final long[] pop = join(LEAF_KEYS_MAX, keys, tmp, p3.offset, p3.d, key);
					// DEBUG(DEBUG, "put key : " + key + ", LF : " + leaf + " => " + Arrays.toString(tmp) + ", pop => " + (pop == null ? null : Arrays.toString(pop)));

					if (pop != null) {
						if ((li < (leaves.length - 2)) && (leaves[li + 1].count() < LEAF_KEYS_MAX)) {
							// merge to right sibling
							final Leaf right = leaves[li + 1];
							right.update(concat(pop, right.keys()));
							right.flush();
							leaf.update(tmp);
							leaf.flush();
							return leaf;
						}

						final Leaf right = Leaf.create(storage);
						right.update(pop);
						right.flush();

						final Leaf[] a = new Leaf[leaves.length + ((leaves.length < INTERNAL_ELEMEMTS_MAX) ? 1 : 0)];
						final Leaf[] o = join(INTERNAL_ELEMEMTS_MAX, leaves, a, p2.offset, p2.d, right);
						if (o != null) {
							final Internal r = Internal.create(storage);
							r.leaves(o);

							r.left(internal.offset());
							r.right(internal.right());

							if (OFFSET_NULL != internal.right()) {
								final Internal rr = Internal.read(internal.right(), storage);
								rr.left(r.offset());
								rr.flush();
							}

							r.flush();
							internal.right(r.offset());
							internal.flush();

							if (OFFSET_NULL == r.right()) {
								head.tail(r.offset());
								head.flush();
							}
						}
						internal.leaves(a);
						internal.flush();
					}
					leaf.update(tmp);
					leaf.flush();

					return leaf;
				}
			}
			return null;
		}

		public Leaf find(final long key) throws IOException {
			final int hash = (hf.hash(key) & 0x7FFFFFFF) % HASH_SIZE;
			final IHEAD head = new IHEAD(storage, root.slice(hash * (IHEAD_BYTES), IHEAD_BYTES));
			final long next = head.front();

			if (OFFSET_NULL == next)
				return null;

			final List<Internal> internals = head.internals();
			final Position p1 = indexOf(internals, key);
			final Internal internal = internals.get(range(internals.size(), p1.offset + (p1.d < 0 ? p1.d : 0))); // + (p1.d < 0 ? -1 : 0)
			final Leaf[] leaves = internal.leaves();
			final Position p2 = indexOf(leaves, key);
			final int li = range(leaves.length - 1, p2.offset + (p2.d < 0 ? p2.d : 0));
			final Leaf leaf = leaves[li];
			final long[] keys = leaf.keys();
			final Position p3 = indexOf(keys, key);
			final Leaf found = p3.d == 0 ? leaf : null;
			// if (found == null) {
			// System.err.println("find key : " + key + ", L1 : " + p1 + ", " + Arrays.toString(internals.toArray()));
			// System.err.println("find key : " + key + ", L2 : " + p2 + ", " + internal);
			// System.err.println("find key : " + key + ", L3 : " + p3 + ", " + Arrays.toString(leaves));
			// System.err.println("find key : " + key + ", LF : " + leaf);
			// }
			return found;
		}

		private Long find(final Comparable<Long> key) throws IOException {
			final int hash = (key.hashCode() & 0x7FFFFFFF) % HASH_SIZE;
			// final int hash = (hf.hash(key) & 0x7FFFFFFF) % HASH_SIZE;
			final IHEAD head = new IHEAD(storage, root.slice(hash * (IHEAD_BYTES), IHEAD_BYTES));
			final long next = head.front();

			if (OFFSET_NULL == next)
				return null;

			final List<Internal> internals = head.internals();
			final Position p1 = indexOf(internals, key);
			final Internal internal = internals.get(range(internals.size(), p1.offset + (p1.d < 0 ? p1.d : 0))); // + (p1.d < 0 ? -1 : 0)
			final Leaf[] leaves = internal.leaves();
			final Position p2 = indexOf(leaves, key);
			final int li = range(leaves.length - 1, p2.offset + (p2.d < 0 ? p2.d : 0));
			final Leaf leaf = leaves[li];
			final long[] keys = leaf.keys();
			final Position p3 = indexOf(keys, key);
			return p3.d == 0 ? keys[p3.offset] : null;
		}

		private Position indexOf(final List<Internal> a, final long key) throws IOException {
			int low = 0;
			int high = a.size() - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				Internal midVal = a.get(mid);
				Leaf leaf = Leaf.read(midVal.leaf(0), storage);
				cmp = -hf.compare(key, leaf.front());
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		private Position indexOf(final List<Internal> a, final Comparable<Long> key) throws IOException {
			int low = 0;
			int high = a.size() - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				Internal midVal = a.get(mid);
				Leaf leaf = Leaf.read(midVal.leaf(0), storage);
				// cmp = -hf.compare(key, leaf.front());
				cmp = -key.compareTo(leaf.front());
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		private Position indexOf(final Leaf[] a, final long key) throws IOException {
			int low = 0;
			int high = a.length - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				Leaf midVal = a[mid];
				cmp = -hf.compare(key, midVal.front());
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		private Position indexOf(final Leaf[] a, final Comparable<Long> key) throws IOException {
			int low = 0;
			int high = a.length - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				Leaf midVal = a[mid];
				// cmp = -hf.compare(key, midVal.front());
				cmp = -key.compareTo(midVal.front());
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		private Position indexOf(final long[] a, final long key) {
			int low = 0;
			int high = a.length - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				long midVal = a[mid];
				cmp = -hf.compare(key, midVal);
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		private Position indexOf(final long[] a, final Comparable<Long> key) {
			int low = 0;
			int high = a.length - 1;
			int cmp = 0;
			while (low <= high) {
				int mid = (low + high) >>> 1; // (low + high) / 2
				long midVal = a[mid];
				// cmp = -hf.compare(key, midVal);
				cmp = -key.compareTo(midVal);
				if (cmp < 0)
					low = mid + 1;
				else if (cmp > 0)
					high = mid - 1;
				else
					return new Position(mid, 0);
			}
			return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
		}

		public long traverse(final Consumer<Long> visitor) throws Exception {
			long rows = 0L;
			for (int h = 0; h < this.HASH_SIZE; h++) {
				final IHEAD head = new IHEAD(storage, root.slice(h * IHEAD_BYTES, IHEAD_BYTES));
				final long front = head.front();
				if (OFFSET_NULL == front)
					continue;

				for (Internal n = Internal.read(front, storage); n != null; n = ((n.right() == OFFSET_NULL) ? null : Internal.read(n.right(), storage))) {
					final Leaf[] leaves = n.leaves();
					for (final Leaf l : leaves) {
						final long[] keys = l.keys();
						for (final long key : keys) {
							visitor.accept(key);
							rows++;
						}
					}
				}
			}
			return rows;
		}

		public void trace(final Object x) throws IOException {
			final PrintStream out = System.out;

			out.print(x + " => ");
			for (int hash = 0; hash < HASH_SIZE; hash++) {
				final IHEAD head = new IHEAD(storage, root.slice(hash * (IHEAD_BYTES), IHEAD_BYTES));
				final long next = head.front();
				if (OFFSET_NULL == next)
					continue;

				out.print("HASH : " + hash + " => ");
				final List<Internal> internals = head.internals();
				for (final Internal internal : internals) {
					out.print(" I:" + internal.offset + " <");
					final Leaf[] leaves = internal.leaves();
					for (final Leaf leaf : leaves) {
						out.print(" L:" + leaf.offset() + "[ ");
						final long[] keys = leaf.keys();
						for (final long key : keys) {
							out.print(key + ", ");
						}
						out.print("]");
					}
					out.print("> ");
				}

				out.println();
			}
		}
	}

	static final class IHEAD implements Node {
		final Storage storage;
		final IoBuffer p;
		long front = OFFSET_NULL;
		long tail = OFFSET_NULL;

		public IHEAD(final Storage storage, final IoBuffer p) {
			this.storage = storage;
			this.p = p;
			final IoBuffer mbb = p.slice();
			this.front = mbb.getLong();
			this.tail = mbb.getLong();
		}

		public void flush() {
			final IoBuffer mbb = p.slice();
			mbb.putLong(front);
			mbb.putLong(tail);
		}

		public void front(final long offset) {
			front = offset;
		}

		public long front() {
			return front;
		}

		public void tail(final long offset) {
			tail = offset;
		}

		public long tail() {
			return tail;
		}

		public List<Internal> internals() throws IOException {
			final List<Internal> a = new ArrayList<>();
			for (long next = front(); next != OFFSET_NULL;) {
				final Internal n = Internal.read(next, storage);
				next = n.right();
				a.add(n);
			}
			return a;
		}
	}

	static final class Internal implements Node {
		final long offset;
		final IoBuffer p;
		long left = OFFSET_NULL;
		long right = OFFSET_NULL;
		Leaf[] leaves;

		static Internal create(final Storage storage) throws IOException {
			final long offset = storage.write(IoBuffer.wrap(new byte[INTERNAL_BYTES]));
			return new Internal(offset, storage.read(offset), storage);
		}

		static Internal read(final long offset, final Storage storage) throws IOException {
			final IoBuffer p = storage.read(offset);
			if (null == p)
				return null;
			return new Internal(offset, p, storage);
		}

		private Internal(final long offset, final IoBuffer p, final Storage storage) throws IOException {
			this.offset = offset;
			this.p = p;

			final IoBuffer mbb = p.slice();
			this.left = mbb.getLong();
			this.right = mbb.getLong();
			final List<Leaf> a = new ArrayList<>();
			for (; mbb.remaining() > 0;) {
				final long o = mbb.getLong();
				if (OFFSET_NULL != o) {
					final Leaf l = Leaf.read(o, storage);
					a.add(l);
				}
			}
			leaves = a.toArray(Leaf[]::new);
		}

		@Override
		public String toString() {
			final long[] children = new long[leaves.length];
			for (int i = 0; i < children.length; i++)
				children[i] = leaves[i].offset();
			return "Internal offset : " + offset + ", children : " + Arrays.toString(children);
		}

		public void flush() {
			final IoBuffer mbb = p.slice();

			mbb.putLong(left);
			mbb.putLong(right);

			for (int i = 0; i < leaves.length; i++) {
				final Leaf l = leaves[i];
				mbb.putLong(l.offset());
			}
		}

		public int count() {
			return leaves.length;
		}

		public long offset() {
			return offset;
		}

		public long left() {
			return left;
		}

		public void left(final long left) {
			this.left = left;
		}

		public long right() {
			return right;
		}

		public void right(final long right) {
			this.right = right;
		}

		public long leaf(final int i) {
			return leaves[i].offset();
		}

		public void leaves(final Leaf[] a) {
			this.leaves = a;
		}

		public Leaf[] leaves() throws IOException {
			return leaves;
		}
	}

	static final class Leaf implements Node {
		final long offset;
		final IoBuffer p;
		long[] keys;

		static Leaf create(final Storage storage) throws IOException {
			final long offset = storage.write(IoBuffer.wrap(memset(new byte[LEAF_BYTES], (byte) 0xFF)));
			return new Leaf(offset, storage.read(offset));
		}

		static Leaf read(final long offset, final Storage storage) throws IOException {
			final IoBuffer p = storage.read(offset);
			if (null == p)
				return null;
			return new Leaf(offset, p);
		}

		private Leaf(final long offset, final IoBuffer p) {
			this.offset = offset;
			this.p = p;
		}

		@Override
		public String toString() {
			return "Leaf offset : " + offset + ", keys : " + Arrays.toString(keys());
		}

		public void flush() {
			if (keys == null)
				return;

			final IoBuffer mbb = p.slice();
			for (int i = 0; i < keys.length; i++) {
				mbb.putLong(keys[i]);
			}
		}

		public long[] keys() {
			final long[] a = new long[LEAF_KEYS_MAX];
			final IoBuffer mbb = p.slice();
			int i = 0;
			for (; mbb.remaining() > 0;) {
				final long v = mbb.getLong();
				if (KEY_NULL == v)
					break;
				a[i++] = v;
			}
			if (i < LEAF_KEYS_MAX) {
				final long[] v = new long[i];
				System.arraycopy(a, 0, v, 0, i);
				return v;
			}
			return a;
		}

		public long front() {
			final IoBuffer mbb = p.slice();
			return mbb.getLong();
		}

		public void update(final long[] keys) {
			this.keys = keys;
		}

		public int count() {
			if (keys != null)
				return (byte) keys.length;

			final IoBuffer mbb = p.slice();
			byte i = 0;
			for (; mbb.remaining() > 0; i++) {
				final long v = mbb.getLong();
				if (KEY_NULL == v)
					break;
			}
			return i;
		}

		public long offset() {
			return offset;
		}
	}

	static final class Position {
		final int offset;
		final int d;

		Position(final int offset, final int d) {
			this.offset = offset;
			this.d = d;
		}

		@Override
		public String toString() {
			return String.format("Position(%d, %d)", offset, d);
		}

		@Override
		public boolean equals(final Object o) {
			final Position p = (Position) o;
			if (offset == p.offset && d == p.d)
				return true;
			if ((offset + 1) == p.offset && d == 1 && p.d == -1)
				return true;
			return false;
		}
	}
}
