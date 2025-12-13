/**
 * BPlusTree.java
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * B+TreeFile
 * 
 * <pre>
 * * ROOT POINTER OFFSET : 0
 * 
 * * INTERNAL : 240B (MARK + LINK x15 + KEY POINTER x14)
 * +--------------------------------------------------------------------------------------------------------------------------+
 * | MARK : 8B (ALWAYS : -2L)  | LINK : 8B | KEY PTR : 8B | LINK | KEY PTR | LINK |                                      .... | 
 * +--------------------------------------------------------------------------------------------------------------------------+
 * - Duplication keys not allowed in All Internals
 * 
 * * LEAF :  240B (MAX : 28)
 * +--------------------------------------------------------------------------------------------------------------------------+
 * |LEFT LEAF : 8B | RIGHT LEAF : 8B | KEY : 8B | KEY |                                                          ... MAX : 28 |
 * +--------------------------------------------------------------------------------------------------------------------------+
 * 
 * 
 * * How to Delete a Key
 * 상위 Internal의 맨 오른쪽 Leaf에서만 삭제가 일어 나도록 할것
 * 상위 Internal의 keys가 한개인 경우, Internal 노드를 삭제하고 왼쪽 Internal측으로 우선으로 병합할것
 * 병합 실패시에는 양옆에 Internal중의 가용한 하나의 키를 빌려 올것
 * Internal 정렬과 함께 부모키 업데이트 할것
 * </pre>
 *
 */
final class BPlusTree implements Tree {
	static final long OFFSET_NULL = -1L;  // for node offset
	static final long KEY_NULL = -1L;     // for record offset
	static final long INTERNAL_MARK = -2L;
	static final long ROOT_SEEK_OFFSET = 0L;

	private final Storage storage;
	private final Cache<Long, Node> cache;
	private final IoBuffer header;
	private final Comparator<Long> sorter;
	private final Reader reader;
	private long count = 0;

	static final int NODE_BYTE_ALIGN = Integer.parseInt(System.getProperty("BPTREE.NODE_BYTE_ALIGN", String.format("%d", (1024)))); // (128 * 2); // Performance 256 ≥ 512 > 128
	static final int STORAGE_HEAD_BYTES = 16; // Storage.java block header ==> TODO : V2 changed to 11 bytes
	static final int HEAD_BYTES = (4 + Long.BYTES);
	static final int NODE_BYTES = (NODE_BYTE_ALIGN - STORAGE_HEAD_BYTES);
	static final int KEY_BYTES = Long.BYTES;
	static final int LINK_BYTES = Long.BYTES;
	static final int LEAF_KEYS_MAX = ((NODE_BYTES - (LINK_BYTES + LINK_BYTES)) / KEY_BYTES);
	static final int INTERNAL_KEYS_MAX = (LEAF_KEYS_MAX / 2);

    static final int DEFAULT_INCREMENT_BYTES = (1024 * 1024 * 16);

	static final byte[] EMPTY_BYTES = new byte[0];

	/**
	 * 
	 * @param file      B+Tree index file
	 * @param mutable	turn on/off mutable storage
	 * @param storage   Storage type (default, v2, memory)    
	 * @param increment Storage increment size
	 * @param cacheSize Node cacheSize
	 * @param sorter    Key sorter
	 * @throws IOException
	 */
	public BPlusTree(final File file, //
			final boolean mutable, //
			final String storage, //
			final int increment, //
			final int cacheSize, //
			final Comparator<Long> sorter, //
            final WAL wal //
	) throws IOException {
		if (null != storage && (!Storage.supported(storage)) || Storage.compressed(storage))
			throw new IllegalArgumentException("Unsupported storage type: " + storage);
		
		this.cache = Cache.create(cacheSize);
		this.sorter = sorter;
		this.storage = WAL.wrap(wal, //
                new Storage.Options() //
				.file(file) //
				.mutable(mutable) //
				.blockBytes((short) (NODE_BYTES)) //
				// .increment(Math.max(increment, NODE_BYTES * 1024)) // disable : -1, 1024 * 1024
				.increment(DEFAULT_INCREMENT_BYTES) // c default 16MB
				.storage(storage), // Storage.TYPE_DEFAULT, Storage.TYPE_V2, Storage.TYPE_MEMORY
                offset -> cache.remove(offset)
		);

		this.header = this.storage.head(HEAD_BYTES);
		this.reader = (final long offset) -> read(offset);

		// PRINT_IF(-1, -1, "INTERNAL_KEYS_MAX : " + INTERNAL_KEYS_MAX + ", LEAF_KEYS_MAX : " + LEAF_KEYS_MAX);

		final IoBuffer hbb = this.header.slice();
		final byte x = hbb.get();
		hbb.rewind();

		final byte[] MAGIC_NUMBER = new byte[] { 'B', '+', 'T', '1' };
		if (x > 0) { // check file is empty
			final byte[] signature = new byte[4];
			hbb.get(signature); // signature
			if (Arrays.compare(signature, MAGIC_NUMBER) != 0)
				throw new IOException("Bad Signature - " + (new String(signature, java.nio.charset.StandardCharsets.UTF_8)) + " " + file);

			count = hbb.getLong(); // count

			root();
		} else {
			hbb.put(MAGIC_NUMBER);
			hbb.putLong(0L);

			root(null);
		}

		// System.out.println("V1 INTERNAL_KEYS_MAX : " + INTERNAL_KEYS_MAX + ", LEAF_KEYS_MAX : " + LEAF_KEYS_MAX);
	}

	@Override
	public void close() throws IOException {
		storage.close();
		cache.close();
	}

	public long bytes() throws IOException {
		return storage != null ? storage.bytes() : 0;
	}

	public long count() throws IOException {
		return count;
	}

	// @ForceInline
	private void count(final long v) throws IOException {
		final IoBuffer mbb = header.slice(4, Long.BYTES);
		mbb.putLong(v);
	}

	public Storage storage() {
		return this.storage;
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
			return String.format("(%d, %d)", offset, d);
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

		@Override
		public int hashCode() {
			return java.util.Objects.hash(offset, d);
		}

		public static int absolute(final Position i, final int length) {
			if (i.offset > 0)
				return i.offset;
			if (i.offset + 1 == length && i.d >= 0)
				return i.offset;
			return Math.max(0, i.offset + (i.d < 0 ? 0 : 1));
		}
	}

	static interface Node {

		long offset();

		long key() throws IOException;
	}

	interface Reader {
		Node get(final long offset) throws IOException;
	}

	static final class KeyRef {
		transient final Reader reader;
		/**
		 * Leaf offset for key
		 */
		final long offset;
		/**
		 * Left node offset
		 */
		long left = OFFSET_NULL;
		/**
		 * Right node offset
		 */
		long right = OFFSET_NULL;

		/**
		 * 
		 * @param offset Leaf offset for key
		 * @param left   Left node offset
		 * @param right  Right node offset
		 * @param reader  Storage reader
		 */
		public KeyRef(final long offset, final long left, final long right, final Reader reader) {
			this.reader = reader;
			this.offset = offset;
			this.left = left;
			this.right = right;

			assert OFFSET_NULL != offset;
			assert left != right : String.format("KeyRef(%d, %d, %d)", offset, left, right);
		}

		/**
		 * Leaf Key
		 * 
		 * @return
		 * @throws IOException
		 */
		long min() throws IOException {
			assert OFFSET_NULL != offset : "OFFSET_NULL";
			final Leaf n = (Leaf) reader.get(offset);
			assert n != null : String.format("Leaf key offset : %s => NULL", offset);
			return n.keys[0];
		}

		/**
		 * Leaf Max Key
		 * 
		 * @return
		 * @throws IOException
		 */
		long max() throws IOException {
			assert OFFSET_NULL != offset : "OFFSET_NULL";
			final Leaf n = (Leaf) reader.get(offset);
			assert n != null : String.format("Leaf key offset : %s => NULL", offset);
			return n.keys[n.keys.length - 1];
		}

		/**
		 * Internal or Leaf Node
		 * 
		 * @return
		 * @throws IOException
		 */
		Node left() throws IOException {
			return OFFSET_NULL == left ? null : reader.get(left);
		}

		/**
		 * Internal or Leaf Node
		 * 
		 * @return
		 * @throws IOException
		 */
		Node right() throws IOException {
			return OFFSET_NULL == right ? null : reader.get(right);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o)
				return true;
			if (o == null || o.getClass() != KeyRef.class)
				return false;
			final KeyRef keyRef = (KeyRef) o;
			return offset == keyRef.offset && left == keyRef.left && right == keyRef.right && reader.equals(keyRef.reader);
		}

		@Override
		public int hashCode() {
			return (int) offset;
		}

		@Override
		public String toString() {
			try {
				final StringBuilder s = new StringBuilder();
				s.append("K:").append(offset).append("[").append(min()).append("]");
				s.append(" (");
				s.append(left).append(", ").append(right);
				s.append(")");
				return s.toString();
			} catch (Exception ex) {
				// ex.printStackTrace();
				return super.toString();
			}
		}
	}

	static final class Internal implements Node {
		// final byte type = 'I';
		final long offset;
		KeyRef[] keys = null;

		private Internal(final long offset) {
			this.offset = offset;
		}

		@Override
		public long offset() {
			return offset;
		}

		@Override
		public long key() throws IOException {
			return keys[0].min();
		}

		public void update(final KeyRef[] keys) throws IOException {
			assert keys != null && keys.length > 0 : "empty - " + this;
			assert keys.length <= INTERNAL_KEYS_MAX : String.format("keys.length:%s > INTERNAL_KEYS_MAX:%s", keys.length, INTERNAL_KEYS_MAX);
			this.keys = dup(keys);
		}

		// prevent reference copy
		static KeyRef[] dup(final KeyRef[] keys) { 
			final long[] a = new long[keys.length * 2 + 1];
			int j = 0;
			a[j++] = keys[0].left;
			for (int i = 0; i < keys.length; i++) {
				// assert keys[i] != null : String.format("keys[%d] : %s, this : %s", i, null, this);
				// assert keys[i].left != keys[i].right : String.format("keys[%d] : %s, this : %s", i, keys[i].left, this);
				a[j++] = keys[i].offset;
				a[j++] = keys[i].right;
			}

			final KeyRef[] nkeys = new KeyRef[keys.length];
			j = 0;
			long left = a[j++];
			for (int i = 0; i < keys.length; i++) {
				final long l = a[j++];
				final long right = a[j++];
				nkeys[i] = new KeyRef(l, left, right, keys[0].reader);
				left = right;
			}
			return nkeys;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o)
				return true;
			if (o == null || o.getClass() != Internal.class)
				return false;
			final Internal internal = (Internal) o;
			return offset == internal.offset && Arrays.equals(keys, internal.keys);
		}

		@Override
		public int hashCode() {
			return (int) offset;
		}

		@Override
		public String toString() {
			final StringBuilder s = new StringBuilder();
			s.append("I:").append(offset);
			s.append(" (");
			s.append(Arrays.toString(keys));
			s.append(")");
			return s.toString();
		}
	}

	static final class Leaf implements Node {
		// final byte type = 'L';
		final long offset;
		long left = OFFSET_NULL;
		long right = OFFSET_NULL;
		long[] keys = null;

		private Leaf(final long offset) {
			this.offset = offset;
		}

		@Override
		public long offset() {
			return offset;
		}

		@Override
		public long key() {
			return keys[0];
		}

		public long max() {
			return keys[keys.length - 1];
		}

		public void update(final long[] keys) {
			this.keys = keys;
		}

		public long left() {
			return left;
		}

		public void left(final long offset) {
			this.left = offset;
		}

		public long right() {
			return right;
		}

		public void right(final long offset) {
			this.right = offset;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o)
				return true;
			if (o == null || o.getClass() != Leaf.class)
				return false;
			final Leaf leaf = (Leaf) o;
			return offset == leaf.offset && Arrays.equals(keys, leaf.keys);
		}

		@Override
		public int hashCode() {
			return (int) offset;
		}

		@Override
		public String toString() {
			final StringBuilder s = new StringBuilder();
			s.append("L:").append(offset);
			s.append(" (");
			s.append(Arrays.toString(keys));
			s.append(")");
			return s.toString();
		}
	}

	static final class Context {
		final Context p;
		final Internal n;
		final Position i;
		Internal[] s;

		Context(final Context p, final Internal n, final Position i) {
			this.p = p;
			this.n = n;
			this.i = i;
		}

		@Override
		public String toString() {
			return String.format("%s", n.offset());
		}
	}

	private void flush(final Internal n) throws IOException {
		final IoBuffer bb = IoBuffer.allocate(NODE_BYTES);
		final KeyRef[] keys = n.keys;
		bb.putLong(INTERNAL_MARK); // Mark as Internal
		bb.putLong(keys[0].left);
		for (int i = 0; i < keys.length; i++) {
			assert keys[i] != null : String.format("keys[%d] : %s, this : %s", i, null, n);
			assert keys[i].left != keys[i].right : String.format("keys[%d] : %s, this : %s", i, keys[i].left, n);
			bb.putLong(keys[i].offset);
			bb.putLong(keys[i].right);
		}

		bb.flip();
		storage.write(n.offset, bb);

		// cache.remove(n.offset);
		cache.put(n.offset, n);
	}

	private void flush(final Leaf n) throws IOException {
		final long[] keys = n.keys;
		assert OFFSET_NULL != n.offset;
		assert keys != null && keys.length > 0;

		final IoBuffer bb = IoBuffer.allocate(NODE_BYTES);
		bb.putLong(n.left);
		bb.putLong(n.right);
		for (int i = 0; i < keys.length; i++)
			bb.putLong(keys[i]);

		bb.flip();
		storage.write(n.offset, bb);

		// cache.remove(n.offset);
		cache.put(n.offset, n);
	}

	private long offset() throws IOException {
		return storage.write(IoBuffer.wrap(EMPTY_BYTES));
	}

	Node read(final long offset) throws IOException {
		if (offset == OFFSET_NULL)
			return null;

		Node node = cache.get(offset);
		if (node == null) {
			node = decode(offset);
			// if (node instanceof Internal)
			cache.put(offset, node);
		}
		return node;
	}

	private Node decode(final long offset) throws IOException {
		if (OFFSET_NULL == offset)
			return null;

		final IoBuffer mbb = storage.read(offset);
		if (mbb == null)
			return null;

		// PRINT_IF(offset, -1, "offset : %s, remaining : %s", offset, mbb.remaining());

		final long mark = mbb.getLong();
		if (INTERNAL_MARK == mark) { // INTERNAL
			final KeyRef[] temp = new KeyRef[INTERNAL_KEYS_MAX];
			int sz = 0;

			long left = mbb.getLong();
			for (; mbb.remaining() >= KEY_BYTES;) {
				final long l = mbb.getLong();
				final long right = mbb.getLong();
				temp[sz++] = new KeyRef(l, left, right, reader);
				left = right;
			}

			final KeyRef[] keys = new KeyRef[sz];
			for (int i = 0; i < keys.length; i++)
				keys[i] = temp[i];

			assert keys.length > 0;
			final Internal n = new Internal(offset);
			n.keys = keys; // n.update(keys);
			return n;
		} else { // LEAF
			final long left = mark;
			final long right = mbb.getLong();

			final long[] temp = new long[LEAF_KEYS_MAX];
			int sz = 0;
			for (; mbb.remaining() >= KEY_BYTES;) {
				final long v = mbb.getLong();
				if (KEY_NULL == v)
					break;
				temp[sz++] = v;
			}

			final long[] keys = new long[sz];
			for (int i = 0; i < keys.length; i++)
				keys[i] = temp[i];

			assert keys.length > 0;
			final Leaf n = new Leaf(offset);
			n.left(left);
			n.right(right);
			n.update(keys);
			return n;
		}
	}

	//

	private Position position(final KeyRef[] a, final long key) throws IOException {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			KeyRef midVal = a[mid];
			cmp = -sorter.compare(key, midVal.min());
			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return new Position(mid, 0);
		}
		return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
	}

	private Position position(final KeyRef[] a, final Comparable<Long> key) throws IOException {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			KeyRef midVal = a[mid];
			cmp = -key.compareTo(midVal.min());
			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return new Position(mid, 0);
		}
		return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
	}

	private Position position(final long[] a, final long key) {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			long midVal = a[mid];
			cmp = -sorter.compare(key, midVal);
			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return new Position(mid, 0);
		}
		return (cmp < 0) ? new Position(high, 1) : new Position(low, -1);
	}

	private static Position position(final long[] a, final Comparable<Long> key) {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			long midVal = a[mid];
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

	private static long[] join(final int capacity, final long[] source, final long[] target, final int offset, final int d, final long key) {
		if (d == 0)
			return null;

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

	private static void join(final KeyRef[] a, final int offset, final int d, final KeyRef key, final KeyRef[] out) {
		int i = 0, j = 0;
		for (; i < (offset + (d < 0 ? 0 : 1)); i++, j++)
			out[i] = a[j];
		out[i++] = key;
		for (; i < out.length; i++, j++)
			out[i] = a[j];
	}

	private static long[] concat(final long[] a, final long[] b) {
		final long[] v = new long[a.length + b.length];
		int j = 0;
		for (int i = 0; i < a.length; i++, j++)
			v[j] = a[i];
		for (int i = 0; i < b.length; i++, j++)
			v[j] = b[i];
		return v;
	}

	private static KeyRef[] concat(final KeyRef[]... a) {
		int length = 0;
		for (final KeyRef[] e : a)
			length += e.length;

		final KeyRef[] v = new KeyRef[length];
		int j = 0;
		for (final KeyRef[] e : a) {
			for (int i = 0; i < e.length; i++, j++)
				v[j] = e[i];
		}
		return v;
	}

	private static long[] slice(final long[] a, final int offset, final int length) {
		final int sz = length - offset;
		final long[] v = new long[sz];
		for (int i = 0, j = offset; i < sz; i++, j++)
			v[i] = a[j];
		return v;
	}

	private static KeyRef[] slice(final KeyRef[] a, final int offset, final int length) {
		final int sz = length - offset;
		final KeyRef[] v = new KeyRef[sz];
		for (int i = 0, j = offset; i < sz; i++, j++)
			v[i] = a[j];
		return v;
	}

	///
	Node root = null;

	Node root() throws IOException {
		if (root != null)
			return root;

		final IoBuffer bb = storage.read(ROOT_SEEK_OFFSET);
		bb.getInt();
		final long offset = bb.getLong();
		return OFFSET_NULL == offset ? null : (root = read(offset));
	}

	private void root(final Node n) throws IOException {
		final IoBuffer bb = IoBuffer.allocate(NODE_BYTES);
		bb.put(new byte[] { 'R', 'O', 'O', 'T' });
		bb.putLong((n == null) ? OFFSET_NULL : n.offset());
		bb.flip();
		storage.write(ROOT_SEEK_OFFSET, bb);

		root = n;
	}

	public int height() throws IOException {
		return height(root());
	}

	/**
	 * Debug function
	 */
	int height(final Node p) throws IOException {
		if (null == p)
			return 0;

		int height = 1;
		for (Node n = p;; height++) {
			if (n.getClass() == Leaf.class)
				return height;
			final Internal node = (Internal) n;
			final KeyRef[] keys = node.keys;
			final KeyRef k = keys[0];
			n = k.left();
		}
	}

	private Node free(final Node n) throws IOException {
		assert n.offset() > 0;
		final boolean ok = storage.delete(n.offset());
		assert ok : String.format("free - %s", n.offset());
		cache.remove(n.offset());
		// STACKTRACE_IF(n.offset(), 58, "free");
		return n;
	}

	private Leaf containable(final Leaf n) throws IOException {
		final long r = n.right();
		final long l = n.left();
		if (OFFSET_NULL == r) {
			if (OFFSET_NULL != l) {
				final Leaf left = (Leaf) read(l);
				if (left.keys.length < LEAF_KEYS_MAX)
					return left;
			}
		} else {
			final Leaf right = (Leaf) read(r);
			if (right.keys.length < LEAF_KEYS_MAX) {
				return right;
			} else { // when right is full
				if (OFFSET_NULL != l) {
					final Leaf left = (Leaf) read(l);
					if (left.keys.length < LEAF_KEYS_MAX)
						return left;
				}
			}
		}
		return null;
	}

	private static Internal containable(final Node l, final Node r) {
		if (null == r) {
			if ((null != l) && (l.getClass() == Internal.class) && (((Internal) l).keys.length < INTERNAL_KEYS_MAX))
				return (Internal) l;
		} else {
			if ((r.getClass() == Internal.class) && (((Internal) r).keys.length < INTERNAL_KEYS_MAX)) {
				return (Internal) r;
			} else { // when right is full
				if ((null != l) && (l.getClass() == Internal.class) && (((Internal) l).keys.length < INTERNAL_KEYS_MAX))
					return (Internal) l;
			}
		}
		return null;
	}

	//
	static void PRINT_IF(final long key, final long debug, final String fmt, final Object... argv) {
		// DO NOT USE THIS METHOD IN PRODUCTION CODE
		// PRINT_IF0(key, debug, fmt, argv);
	}

	@Deprecated
	static void PRINT_IF0(final long key, final long debug, final String fmt, final Object... argv) {
		if (key != debug && debug != -1L)
			return;
		final String s = String.format(fmt, argv);
		if (key < 0)
			System.out.println("LOG : " + s);
		else
			System.out.println("LOG [" + key + "] : " + s);
	}

	static void STACKTRACE_IF(final long key, final long debug, final String fmt, final Object... argv) {
		if (key != debug && debug != -1L)
			return;
		final String s = String.format(fmt, argv) + " TRACE : " + new Exception().getStackTrace()[2];
		if (key < 0)
			System.out.println("LOG : " + s);
		else
			System.out.println("LOG [" + key + "] : " + s);
	}

	static void BUG_IF(final long key, final long debug, final String fmt, final Object... argv) {
		if (key != debug && debug != -1L)
			return;
		final String s = String.format(fmt, argv);
		if (key < 0)
			throw new RuntimeException("BUG : " + s);
		else
			throw new RuntimeException("BUG [" + key + "] : " + s);
	}

	///

	private Leaf putLF(final Leaf n, final long key) throws IOException {
		final Leaf leaf = (Leaf) n;
		final long[] keys = leaf.keys;
		final Position pos = position(keys, key);
		if (0 == pos.d) // existing key
			return null;

		Leaf popped = null;
		final long[] temp = new long[keys.length + ((keys.length < LEAF_KEYS_MAX) ? 1 : 0)];
		final long[] split = join(LEAF_KEYS_MAX, keys, temp, pos.offset, pos.d, key);

		// DEBUG("key : %s, leaf : %s, pos : %s", key, n, pos);

		if (null != split) {
			Leaf sibling = containable(leaf);
			if (sibling != null) {
				if (sibling.offset() == leaf.right()) {
					// 기존 구현: overflow된 split 키를 오른쪽 형제의 맨 앞에 붙여 최소값을 감소시킴
					// 문제: 부모 Internal 노드가 leaf 최소값을 정렬 기준으로 사용하는 경우 기존 최소값 변경으로 경계 불일치/중복 offset 유발 가능
					// 수정: split 키는 오버플로 집합에서 가장 큰 키이므로 형제 뒤쪽에 append 하여 최소값 보존
					final long[] a = concat(sibling.keys, split); // append instead of prepend
					sibling.update(a);
					flush(sibling);
				} else {
					// DEBUG(key, "putL => concat left L:" + sibling.offset());
					final long first = temp[0];
					for (int j = 1; j < temp.length; j++)
						temp[j - 1] = temp[j];
					temp[temp.length - 1] = split[0];

					final long[] a = concat(sibling.keys, new long[] { first });
					sibling.update(a);
					flush(sibling);
				}
			} else {
				// DEBUG(key, "putL => split");
				sibling = new Leaf(offset());
				sibling.update(split);
				sibling.left(leaf.offset());
				sibling.right(leaf.right());
				if (OFFSET_NULL != leaf.right()) {
					final Leaf rr = (Leaf) read(leaf.right());
					rr.left(sibling.offset());
					flush(rr);
				}
				leaf.right(sibling.offset());
				flush(sibling);

				popped = sibling;
			}
		}

		leaf.update(temp);

		// ordering & monotonic sibling min preservation assertions (debug-only semantics)
		// assert leaf.keys.length == leaf.keys.length; // placeholder no-op to keep assert block style consistent
		// for (int i = 1; i < leaf.keys.length; i++) {
		// 	assert sorter.compare(leaf.keys[i - 1], leaf.keys[i]) <= 0 : "Leaf order violated after split cooperation";
		// }
        
		flush(leaf);
		count(++count);
		return popped;
	}

	private KeyRef put(final Context context, final Node n, final long key) throws IOException {
		if (n.getClass() == Leaf.class) {
			final Leaf popped = putLF((Leaf) n, key);
			// DEBUG(key, "putL => popped : " + popped);
			if (popped != null) {
				final KeyRef k = new KeyRef(popped.offset(), popped.left(), popped.offset(), reader);
				if (context == null) {
					// DEBUG("NEW ROOT case 1");
					final Internal root = new Internal(offset());
					root.update(new KeyRef[] { k });
					flush(root);
					root(root);
					return null;
				}
				return k;
			}
			return null;
		}

		final Internal node = (Internal) n;
		final KeyRef[] keys = node.keys;
		final Position pos = position(keys, key);
		if (0 == pos.d) // existing key
			return null;

		// DEBUG(key, "n : %s, pos : %s", n, pos);

		final KeyRef k = keys[pos.offset];
		final KeyRef nk = put(new Context(context, node, pos), (pos.d < 0) ? k.left() : k.right(), key);
		// DEBUG(key, "nk : " + nk + ", n : " + n);
		if (null == nk)
			return null;

		final KeyRef[] nkeys = new KeyRef[keys.length + 1];
		join(keys, pos.offset, pos.d, nk, nkeys);
		// DEBUG("temp.nkeys : %s", Arrays.toString(keys(nkeys)));

		KeyRef[] temp = nkeys.length <= INTERNAL_KEYS_MAX ? nkeys : slice(nkeys, 0, INTERNAL_KEYS_MAX);
		KeyRef[] split = nkeys.length <= INTERNAL_KEYS_MAX ? null : new KeyRef[] { nkeys[nkeys.length - 1] };
		// if (split != null)
		// DEBUG("temp : %s, split : %s", Arrays.toString(keys(temp)), (split == null ? -1 : split[0].key()));

		if (null == split) {
			// DEBUG(key, "nk : " + nk + ", node : " + n);
			node.update(temp);
			flush(node);
			return null;
		}

		final Internal p = context == null ? null : context.n;
		if (p == null) {
			// DEBUG("NEW ROOT case 2");
			final int MAX = temp.length + 1;
			final int HALF = (MAX / 2) + (MAX % 2 == 1 ? 1 : 0);

			final KeyRef[] a = concat(temp, split);
			final KeyRef[] lkeys = new KeyRef[HALF];
			final KeyRef[] rkeys = new KeyRef[(MAX - 1 - HALF)];

			int j = 0;
			for (int m = 0; m < lkeys.length; m++)
				lkeys[m] = a[j++];
			final KeyRef rootkey = a[j++];
			for (int m = 0; m < rkeys.length; m++)
				rkeys[m] = a[j++];

			final Internal left = node;
			final Internal right = new Internal(offset());

			// NOTE-1 모든 Internal의 키는 중복 허용하지 않음, B+Tree에서 가장 핵심 부분 1, 혼동되는 부분
			final long swap = rootkey.right;
			rootkey.left = left.offset();
			rootkey.right = right.offset();
			rkeys[0].left = swap;

			left.update(lkeys);
			flush(left);
			right.update(rkeys);
			flush(right);

			final Internal root = new Internal(offset());
			root.update(new KeyRef[] { rootkey });
			flush(root);
			root(root);

			return null;
		} else {
			@SuppressWarnings("null")
            final Position pi = context.i;
			final Node left = (pi.offset - 1 >= 0) ? read(p.keys[pi.offset - 1].offset) : null;
			final Node right = (pi.offset + 1 < p.keys.length) ? read(p.keys[pi.offset + 1].offset) : null;

			KeyRef r = null;
			Internal sibling = containable(left, right);
			if (null == sibling) {
				// NOTE-2 모든 Internal의 키는 중복 허용하지 않음, B+Tree에서 가장 핵심 부분 2, 가장 혼동되는 부분
				// PRINT_IF(key, 42, "split => temp : " + Arrays.toString(temp) + ", split : " + Arrays.toString(split));
				split[0].left = temp[temp.length - 1].right;
				temp = slice(temp, 0, temp.length - 1); // 원래의 노드는 축소

				sibling = new Internal(offset());
				sibling.update(split);

				r = new KeyRef(min(read(split[0].left)).offset(), node.offset(), sibling.offset(), reader); // 상위 노드에 추가할 키
				// PRINT_IF(key, 42, "split => r : " + r);
			} else if (right == sibling) {
				final KeyRef[] a = concat(split, sibling.keys);
				sibling.update(a);
			} else if (left == sibling) {
				final KeyRef f = temp[0];
				for (int j = 1; j < temp.length; j++)
					temp[j - 1] = temp[j];
				temp[temp.length - 1] = split[0];

				final KeyRef[] a = concat(sibling.keys, new KeyRef[] { f });
				sibling.update(a);
			}

			flush(sibling);
			node.update(temp);
			flush(node);
			return r;
		}
	}

	public void put(final long key) throws IOException {
		final Node root = root();
		if (null == root) {
			final Leaf leaf = new Leaf(offset());
			leaf.update(new long[] { key });
			flush(leaf);
			root(leaf);
			count(++count);
		} else {
			put(null, root, key);
		}
	}

	public boolean delete(final long key) throws IOException {
		final Node root = root();
		if (root == null)
			return false;

		final long[] out = new long[] { OFFSET_NULL };
		if (root.getClass() == Leaf.class) {
			final Leaf leaf = (Leaf) root;
			delete(null, leaf, key, out);
			return OFFSET_NULL != out[0];
		}

		delete(root, key, out);
		return OFFSET_NULL != out[0];
	}

	private void delete(final Node start, final long key, final long[] out) throws IOException {
		Context context = null;
		Internal p = null;
		Position pi = null;
		for (Node n = start; n != null;) {
			// PRINT_IF(key, -1, "DELETE n : %s", n);
			if (n.getClass() == Leaf.class) {
				delete(context, (Leaf) n, key, out);
				return;
			}

			final Internal node = (Internal) n;
			final KeyRef[] keys = node.keys;
			final Position i = position(keys, key);
			final KeyRef k = keys[i.offset];

			// PRINT_IF(key, -1, "DELETE n.offset : %s, i : %s, sk : %s", n.offset(), i, k);

			n = i.d >= 0 ? k.right() : k.left();
			context = new Context(context, node, i);
			if (p != null)
				context.s = new Internal[] { left(p, pi, n), right(p, pi, n) };
			p = node;
			pi = i;
		}
	}

	private void delete(final Context context, final Leaf n, final long key, final long[] out) throws IOException {
		final long[] keys = n.keys;
		final Position found = position(keys, key);
		PRINT_IF(key, -1, "LEAF : %s, found : %s", n, found);
		if (0 != found.d) // error
			return;

		out[0] = key; // => deletion ok
		count(--count);

		long[] temp = new long[keys.length - 1];
		for (int i = 0, j = 0; i < temp.length;) {
			final long v = keys[j++];
			if (v != key)
				temp[i++] = v;
		}

		if (context == null) { // ROOT
			PRINT_IF(key, -1, "ROOT[%s]", n.offset);
			if (temp.length > 0) {
				n.update(temp);
				flush(n);
			} else {
				free(n);
				root(null);
			}
			return;
		}

		if (temp.length > 0) {
			n.update(temp);
			flush(n);
			return;
		}

		final Internal p = context.n;
		final Position pi = context.i;
		final KeyRef[] pkeys = p.keys;
		final Leaf left = (Leaf) read(n.left());
		final Leaf right = (Leaf) read(n.right());

		if (null == context.p && pkeys.length == 1) {
			PRINT_IF(key, -1, "ROOT N : %s, L : %s, R : %s, P : %s", n, left, right, p.offset);
			free(n);
			free(p);

			if (null != right) {
				right.left(OFFSET_NULL);
				flush(right);
				root(right);
			}
			if (null != left) {
				left.right(OFFSET_NULL);
				flush(left);
				root(left);
			}

			return;
		}

		if (pkeys.length == 1 && pi.d >= 0) {
			// 왼쪽 한개만 남은 경우
			if (null != right) {
				right.left(n.left());
				flush(right);
			}
			if (null != left) {
				left.right(n.right());
				flush(left);
			}
			free(n);

			final int ppi = Position.absolute(context.p.i, context.p.n.keys.length);
			if (context.p != null && (ppi > 0 || (ppi == 0 && context.p.i.d >= 0))) { //
				@SuppressWarnings("null")
                final KeyRef k = new KeyRef(left.offset, //
						left.left, //
						left.offset, //
						reader //
				);
				PRINT_IF(key, -1, "LEAF[%s] single parent w/ left upkey : %s", n.offset, k);
				rebalance(context, -1, k, key);
				return;
			}

			@SuppressWarnings("null")
            final KeyRef k = new KeyRef(right.offset, //
					left.offset, //
					right.offset, //
					reader //
			);
			PRINT_IF(key, -1, "LEAF[%s] single parent w/ right upkey : %s", n.offset, k);
			rebalance(context, 1, k, key);
			return;
		}

		final int abs = Position.absolute(pi, pkeys.length);
		PRINT_IF(key, -1, "LEAF[%s] pi : %s, abs : %s, pkeys.length : %s, pkey : %s", n.offset, pi, abs, pkeys.length, pkeys[abs]);

		if (pkeys.length >= 2) { // && right != null
			if (right != null) {
				right.left(n.left());
				flush(right);
			}
			if (left != null) {
				// left.right(right.offset());
				left.right(n.right());
				flush(left);
			}

			free(n);

			final long[] a = new long[pkeys.length];
			int j = 0;
			for (int i = 0; i < pkeys.length; i++)
				if (pkeys[i].left != n.offset)
					a[j++] = pkeys[i].left;
			if (pkeys[pkeys.length - 1].right != n.offset)
				a[j++] = pkeys[pkeys.length - 1].right;
			PRINT_IF(key, -1, "LEAF[%s] pkeys a => %s", n.offset(), Arrays.toString(a));

			final KeyRef[] nkeys = new KeyRef[a.length - 1];
			final long l = a[0];
			for (int i = 1; i < a.length; i++) {
				nkeys[i - 1] = new KeyRef(a[i], l, a[i], reader);
			}

			PRINT_IF(key, -1, "LEAF[%s] pkeys update => %s", n.offset(), Arrays.toString(nkeys));
			p.update(nkeys);
			flush(p);

			if (abs == 0) {
				for (Context ctx = context.p; ctx != null; ctx = ctx.p) {
					Internal pp = ctx.n;
					if (null == pp)
						break;

					if (ctx.i.d >= 0) {
						final KeyRef ppk = pp.keys[ctx.i.offset];
						pp.keys[ctx.i.offset] = new KeyRef(nkeys[0].left().offset(), ppk.left, ppk.right, reader);
						PRINT_IF(key, -1, "LEAF[%s] ppkeys[%s] update => %s", n.offset(), pp.offset, Arrays.toString(pp.keys));
						pp.update(pp.keys);
						flush(pp);
						break;
					}
				}
			}

			return;
		}

		if (0 == abs && null != right && right.keys.length >= 2) {
			PRINT_IF(key, -1, "LEAF[%s] Borrow from right : %s", n.offset, right);
			n.update(new long[] { right.keys[0] });
			right.update(slice(right.keys, 1, right.keys.length));
			flush(right);
			flush(n);
			return;
		}

		if (0 == abs && null != left && left.keys.length >= 2) {
			PRINT_IF(key, -1, "LEAF[%s] Borrow from left :  %s", n.offset, left);

			n.update(new long[] { left.keys[left.keys.length - 1] });
			left.update(slice(left.keys, 0, left.keys.length - 1));
			flush(left);
			flush(n);
			return;
		}

		if (0 == abs && null != right) {
			PRINT_IF(key, -1, "LEAF[%s] deleted  pi : %s", n.offset, pi);
			right.left(n.left);
			flush(right);
			if (left != null) {
				left.right(right.offset);
				flush(left);
			}
			free(n);

			if (context.p != null && context.p.i.offset == 0 && context.p.i.d < 0) {
				final Leaf rr = min(context.p.n.keys[0].right());
				if (null != rr) {
					final KeyRef k = new KeyRef(rr.offset, //
							right.offset, //
							rr.offset, //
							reader //
					);
					PRINT_IF(key, -1, "LEAF[%s] deleted w/ rr upkey : %s", n.offset, k);
					rebalance(context, 1, k, key);
					return;
				}
			}

			final KeyRef k = new KeyRef(right.offset, //
					n.left, //
					right.offset, //
					reader //
			);
			PRINT_IF(key, -1, "LEAF[%s] deleted w/ upkey : %s", n.offset, k);
			rebalance(context, -1, k, key);
			return;
		}

		BUG_IF(key, -1, "LEAF[%s] p : %s", n.offset, p);
		// return null;
	}

	@SuppressWarnings("null")
    private void rebalance(final Context ctx, final int d, final KeyRef nk, final long key) throws IOException {
        final Internal p = ctx != null && ctx.p == null ? null : ctx.p.n;
		final Internal n = ctx.n;
		final Position found = ctx.i;
		// PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s", n.offset, d, (p == null ? null : p.offset));

		final KeyRef k = n.keys[found.offset];
		final Position pi = p == null ? null : ctx.p.i;
		final KeyRef[] pkeys = p == null ? null : p.keys;
		// final int abs = Position.absolute(pi, pkeys.length);

		PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s, pi : %s, nk : %s", n.offset, d, (p == null ? null : p.offset), pi, nk);

		if (-1 == d) {
			final Internal left = p == null ? (Internal) k.left() : ctx.s[0]; // left(p, pi, n);
			PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s, merge to left => %s, nk => %s ", n.offset, d, (p == null ? null : p.offset), left, nk);
			if (left.keys.length >= 2) {
				final KeyRef lkey = left.keys[left.keys.length - 1];
				left.update(slice(left.keys, 0, left.keys.length - 1));

				final KeyRef mid = new KeyRef(min(nk.right()).offset, nk.left, nk.right, reader);
				n.update(new KeyRef[] { mid });

				PRINT_IF(key, -1, "INTERNAL[%s] borrow from left[%s], L : %s, N : %s, LK : %s, NK : %s, MID : %s", n.offset, left.offset(), left, n, lkey, nk, mid);
				flush(left);
				flush(n);

                final int abs = pi.offset + ((pi.d < 0 && pi.offset > 0) ? -1 : 0);
                final KeyRef pkey = pkeys[abs];
				pkeys[abs] = new KeyRef(min(nk.right()).left, pkey.left, pkey.right, reader);
				PRINT_IF(key, -1, "INTERNAL[%s] borrow from left[%s], => abs : %s, pkeys : %s", n.offset, left.offset(), abs, Arrays.toString(pkeys));

				p.update(pkeys);
				flush(p);
			} else {
				final KeyRef mid = new KeyRef(min(nk.right()).offset, nk.left, nk.right, reader);
				left.update(concat(left.keys, new KeyRef[] { mid })); // new KeyRef[] { nk }
				flush(left);
				free(n);
				PRINT_IF(key, -1, "INTERNAL[%s] merge to left[%s] left.keys : %s", n.offset(), left.offset(), Arrays.toString(left.keys));

				if (pkeys.length >= 2) {
                    final int abs = pi.offset + ((pi.d < 0 && pi.offset > 0) ? -1 : 0);
					final KeyRef pkey = pkeys[abs];

					PRINT_IF(key, -1, "INTERNAL[%s] merge to left[%s] => pkeys before : %s", n.offset(), left.offset(), Arrays.toString(kOffsets(pkeys)));

					if (abs + 1 < pkeys.length)
						pkeys[abs + 1].left = left.offset;
					if (abs - 1 >= 0)
						pkeys[abs - 1].right = left.offset;

					final KeyRef[] a = new KeyRef[pkeys.length - 1];
					for (int i = 0, j = 0; i < pkeys.length; i++)
						if (pkey.offset != pkeys[i].offset)
							a[j++] = pkeys[i];

					PRINT_IF(key, -1, "INTERNAL[%s] merge to left[%s] => pkeys after : %s", n.offset(), left.offset(), Arrays.toString(kOffsets(a)));
					p.update(a);
					flush(p);
				} else {
					if (null != ctx.p.p) {
						final Internal pleft = (Internal) left(ctx.p.p.n, ctx.p.p.i, ctx.p.n);
						if (pleft != null) {
							final Internal ll = (Internal) pleft.keys[pleft.keys.length - 1].right();
							final KeyRef upkey = new KeyRef( //
									min(left.keys[left.keys.length - 1].right()).offset, //
									ll.offset, left.offset, //
									reader);

							PRINT_IF(key, -1, "INTERNAL[%s] single parent w/ ll upkey : %s", n.offset, upkey);
							rebalance(ctx.p, -1, upkey, key);
							updateKeys(ctx.p, upkey.offset, key);
						} else {
							final Internal pright = (Internal) right(ctx.p.p.n, ctx.p.p.i, ctx.p.n);
							final Internal rr = (Internal) pright.keys[0].left();
							final KeyRef upkey = new KeyRef( //
									min(rr.keys[0].left()).offset, //
									left.offset, rr.offset, //
									reader);
							PRINT_IF(key, -1, "INTERNAL[%s] merge to left[%s] => upkey : %s", n.offset(), left.offset(), upkey);
							rebalance(ctx.p, 1, upkey, key);
						}
					} else {
						PRINT_IF(key, -1, "INTERNAL[%s] ROOT nk : %s, left : %s", n.offset, nk, left);
						final Node root = root();
						free(root);
						root(left);
					}
				}
			}
		} else if (1 == d) {
			final Internal right = p == null ? (Internal) k.right() : ctx.s[1]; // right(p, pi, n);
			if (right.keys.length >= 2) {
				PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s, borrow from right => %s ", n.offset, d, (p == null ? null : p.offset), right);
				final KeyRef[] rkeys = new KeyRef[right.keys.length - 1];
				for (int i = 0; i < rkeys.length; i++)
					rkeys[i] = right.keys[i + 1];
				right.update(rkeys);

				n.update(new KeyRef[] { nk });
				PRINT_IF(key, -1, "INTERNAL[%s] borrow from right[%s] => keys : %s", n.offset, right.offset(), Arrays.toString(n.keys));
				flush(right);
				flush(n);

				updateKeys(ctx.p, right.keys[0].offset, key);
			} else {
				right.update(concat(new KeyRef[] { nk }, right.keys));
				PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s, merge to right => %s ", n.offset, d, (p == null ? null : p.offset), right);

				flush(right);
				free(n);

				if (pkeys.length == 1 && null != ctx.p.p) {
					final Internal pright = (Internal) right(ctx.p.p.n, ctx.p.p.i, ctx.p.n);
					if (pright != null) {
						final Internal rr = (Internal) pright.keys[0].left();

						final KeyRef upkey = new KeyRef( //
								min(rr.keys[0].left()).offset, //
								right.offset, rr.offset, //
								reader);
						PRINT_IF(key, -1, "INTERNAL[%s] single parent w/ rr upkey : %s", n.offset, upkey);
						rebalance(ctx.p, 1, upkey, key);
					} else {
						final Internal pleft = (Internal) left(ctx.p.p.n, ctx.p.p.i, ctx.p.n);
						final Internal ll = (Internal) pleft.keys[pleft.keys.length - 1].right();

						final KeyRef upkey = new KeyRef( //
								min(right.keys[0].right()).offset, //
								ll.offset, right.offset, //
								reader);

						PRINT_IF(key, -1, "INTERNAL[%s] single parent w/ ll upkey : %s", n.offset, upkey);
						rebalance(ctx.p, -1, upkey, key);
						updateKeys(ctx.p, upkey.offset, key);
					}
				} else if (pkeys.length >= 2) {
					PRINT_IF(key, -1, "INTERNAL[%s] d : %s, p.offset : %s, merge to right => %s ", n.offset, d, (p == null ? null : p.offset), right);
                    final int abs = pi.offset + ((pi.d < 0 && pi.offset > 0) ? -1 : 0);
					final KeyRef pkey = pkeys[abs];
					if (abs + 1 < pkeys.length)
						pkeys[abs + 1].left = right.offset;
					if (abs - 1 >= 0)
						pkeys[abs - 1].right = right.offset;

					final KeyRef[] a = new KeyRef[pkeys.length - 1];
					for (int i = 0, j = 0; i < pkeys.length; i++)
						if (pkey.offset != pkeys[i].offset)
							a[j++] = pkeys[i];

					PRINT_IF(key, -1, "INTERNAL[%s] merge to right[%s] => pkeys after : %s", n.offset(), right.offset(), Arrays.toString(kOffsets(a)));
					p.update(a);
					flush(p);

					updateKeys(ctx.p, p.keys[0].offset, key);
				} else if (pkeys.length == 1) {
					final Node root = root();
					free(root);
					PRINT_IF(key, -1, "INTERNAL[%s] ROOT : %s, nk : %s, root : %s", n.offset, n, nk, root.offset());
					// right.update(concat(new KeyRef[] {}, right.keys));

					root(right);
				}
			}
		}
	}

	private void updateKeys(final Context context, final long upkey, final long key) throws IOException {
		// long up = upkey;
		for (Context ctx = context; ctx != null; ctx = ctx.p) {
			final Internal n = ctx.n;
			if (null != read(n.offset)) { // check alive
				final Position found = ctx.i;
				final KeyRef[] keys = n.keys;
				final int abs = Math.min(keys.length - 1, found.offset + ((found.d < 0 && found.offset > 0) ? -1 : 0));
				final KeyRef sk = keys[abs];
				PRINT_IF(key, -1, "INTERNAL[%s] key update sk : %s", n.offset, sk.offset);
				Leaf front = min(sk.right());
				if (null == front)
					front = min(sk.left());

				if (front.offset != sk.offset) {
					keys[abs] = new KeyRef(front.offset, sk.left, sk.right, reader);
					// PRINT_IF(key, -1, "INTERNAL[%s] key update found : %s sk : %s => %s", n.offset, found, sk.offset, nk.offset);
					n.update(keys);
					flush(n);
				}
			}
		}
	}

	private long[] kOffsets(final KeyRef[] keys) {
		final long[] a = new long[keys.length];
		for (int i = 0; i < keys.length; i++) {
			a[i] = keys[i] == null ? OFFSET_NULL : keys[i].offset;
		}
		return a;
	}

	private Internal left(final Internal p, final Position pi, final Node n) throws IOException {
		if (pi.d >= 0)
			return (Internal) p.keys[pi.offset].left();
		if (pi.offset - 1 >= 0)
			return (Internal) p.keys[pi.offset - 1].left();
		return null;
	}

	private Internal right(final Internal p, final Position pi, final Node n1) throws IOException {
		if (pi.d < 0)
			return (Internal) p.keys[pi.offset].right();
		if (pi.offset + 1 < p.keys.length)
			return (Internal) p.keys[pi.offset + 1].right();
		return null;
	}

	Leaf min(final Node i) throws IOException {
		Leaf found = null;
		Node n = i;
		for (; n != null;) {
			if (n.getClass() == Leaf.class) {
				found = (Leaf) n;
				break;
			}

			final Internal node = (Internal) n;
			final KeyRef[] keys = node.keys;
			final KeyRef k = keys[0];
			n = k.left();
		}
		return found;
	}

	Leaf max(final Node i) throws IOException {
		Leaf found = null;
		Node n = i;
		for (; n != null;) {
			if (n.getClass() == Leaf.class) {
				found = (Leaf) n;
				break;
			}

			final Internal node = (Internal) n;
			final KeyRef[] keys = node.keys;
			final KeyRef k = keys[keys.length - 1];
			n = k.right();
		}
		return found;
	}

	/// key search functions

	private Long get(final long key, final Node n) throws IOException {
		if (n.getClass() == Leaf.class) {
			final Leaf leaf = (Leaf) n;
			final long[] keys = leaf.keys;
			final Position i = position(keys, key);
			if (0 == i.d) // existing key
				return key;
			return null;
		}

		final Internal node = (Internal) n;
		final KeyRef[] keys = node.keys;
		final Position i = position(keys, key);
		if (0 == i.d) // existing key
			return key;

		final KeyRef k = keys[i.offset];
		return get(key, (i.d < 0) ? k.left() : k.right());
	}

	private Long get(final Comparable<Long> key, final Node n) throws IOException {
		if (n.getClass() == Leaf.class) {
			final Leaf leaf = (Leaf) n;
			final long[] keys = leaf.keys;
			final Position i = position(keys, key);
			if (0 == i.d) // existing key
				return keys[i.offset];
			return null;
		}

		final Internal node = (Internal) n;
		final KeyRef[] keys = node.keys;
		final Position i = position(keys, key);
		if (0 == i.d) // existing key
			return keys[i.offset].min();

		final KeyRef k = keys[i.offset];
		return get(key, (i.d < 0) ? k.left() : k.right());
	}

	public Long get(final long key) throws IOException {
		final Node root = root();
		return root == null ? null : get(key, root);
	}

	public Long get(final Comparable<Long> key) throws IOException {
		final Node root = root();
		return root == null ? null : get(key, root);
	}

	private static final Cursor<Long> EOF = new Cursor<Long>() {
		@Override
		public void close() throws Exception {
		}

		@Override
		public Long next() {
			return -1L;
		}
	};

	static abstract class CursorImpl implements Cursor<Long> {
		final Leaf h;
		final int o;
		Leaf node;
		int offset;

		@Override
		public void close() throws Exception {
		}

		CursorImpl(final Leaf leaf, final int i) {
			this.h = this.node = leaf;
			this.o = this.offset = i;
		}

		boolean head() {
			return o == offset && h.equals(node);
		}
	}

	/**
	 * 
	 * @param sort       Filter.ASCENDING / Filter.DESCENDING
	 * @param comparable Comparable key to search for
     */
	public Cursor<Long> find(final int sort, final Comparable<Long> comparable) throws Exception {
		final Node root = root();
        if (root == null)
            return EOF;

		if (sort == Filter.ASCENDING) {
			// PRINT_IF(0, 0, "find asc : %s", comparable);
			final Leaf n = min(root, comparable);
			if (n == null)
				return EOF;

			// PRINT_IF(0, 0, "find node : %s", n);
			final int first = first(n.keys, comparable);
			// PRINT_IF(0, 0, "find asc : %s, start : %s", n, first);
			// assert first > -1;
			return new CursorImpl(n, first) {
				@Override
				public Long next() {
					try {
						if (node != null && offset >= node.keys.length) {
							offset = 0;
							node = (Leaf) read(node.right());
						}

						if (node == null)
							return -1L;

						final long v = node.keys[offset++];
						if (head())
							return v;

						if (0 != comparable.compareTo(v)) {
							node = null;
							return -1L;
						}
						return v;
					} catch (Exception ex) {
					}
					return -1L;
				}
			};
		} else if (sort == Filter.DESCENDING) {
			final Leaf n = max(root, comparable);
			if (n == null)
				return EOF;

			final int first = last(n.keys, comparable);
			// PRINT_IF(0, 0, "find desc : %s, start : %s", n, first);
			return new CursorImpl(n, first) {
				@Override
				public Long next() {
					try {
						if (node != null && offset < 0) {
							node = (Leaf) read(node.left());
							offset = node != null ? node.keys.length - 1 : -1;
						}

						if (node == null)
							return -1L;

						final long v = node.keys[offset--];
						if (head())
							return v;

						// System.err.println("v : " + v);
						if (0 != comparable.compareTo(v)) {
							node = null;
							return -1L;
						}
						return v;
					} catch (Exception ex) {
					}
					return -1L;
				}
			};
		}

		return EOF;
	}

	private static int first(final long[] a, final Comparable<Long> key) {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		int match = low;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			long midVal = a[mid];
			cmp = -key.compareTo(midVal);
			match = cmp == 0 ? mid : match;
			if (cmp < 0)
				low = mid + 1;
			else if (cmp >= 0)
				high = mid - 1;
		}
		return match;
	}

	private static int last(final long[] a, final Comparable<Long> key) {
		int low = 0;
		int high = a.length - 1;
		int cmp = 0;
		int match = high;
		while (low <= high) {
			int mid = (low + high) >>> 1; // (low + high) / 2
			long midVal = a[mid];
			cmp = -key.compareTo(midVal);
			match = cmp == 0 ? mid : match;
			if (cmp <= 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
		}
		return match;
	}

	// private Leaf min1(final Node start, final Comparable<Long> key) throws Exception {
	// for (Node n = start; n != null;) {
	// if (n instanceof Leaf) {
	// // PRINT_IF(0, 0, "leaf : %s => %s", key, n);
	// return (Leaf) n;
	// }
	//
	// final Internal node = (Internal) n;
	// final KeyRef[] a = node.keys;
	//
	// Node found = null;
	// for (int i = 0; i < a.length; i++) {
	// final KeyRef k = a[i];
	// final int d = key.compareTo(max(k.left()).max());
	// if (d <= 0) {
	// n = found = k.left();
	// PRINT_IF(0, 0, "min : %s => i : %s, d : %s => %s", key, i, d, n);
	// break;
	// }
	// }
	// if (found == null) {
	// final KeyRef k = a[a.length - 1];
	// n = k.right();
	// }
	// }
	// return null;
	// }

	private Leaf min(final Node start, final Comparable<Long> key) throws Exception {
		for (Node n = start; n != null;) {
			if (n.getClass() == Leaf.class) {
				// PRINT_IF(0, 0, "leaf : %s => %s", key, n);
				return (Leaf) n;
			}

			final Internal node = (Internal) n;
			final KeyRef[] a = node.keys;

			int low = 0;
			int high = a.length - 1;
			int match = 0;
			int cmp = Integer.MAX_VALUE;
			while (low <= high) {
				final int mid = (low + high) >>> 1; // (low + high) / 2
				final KeyRef midVal = a[mid];
				// final int d = key.compareTo(midVal.min());
				final int d = key.compareTo(max(midVal.left()).max());
				// PRINT_IF(0, 0, "min : %s => mid : %s : %s, d : %s", n.key(), mid, midVal.min(), d);
				if (d <= 0)
					high = mid - 1;
				else
					low = mid + 1;

				match = mid;
				cmp = d;
			}

			final KeyRef k = a[match];
			final int d = cmp; // key.compareTo(max(k.left()).max());
			n = (d <= 0) ? k.left() : k.right();
			// PRINT_IF(0, 0, //
			// "match : %s / %s, d : %s => %s~%s, key : %s, n.offset : %s, expr : %s", //
			// match, a.length, d, k.min(), k.max(), k, n.offset(), //
			// key //
			// );
		}
		return null;
	}

	@SuppressWarnings("null")
    private Leaf max(final Node start, final Comparable<Long> key) throws Exception {
		for (Node n = start; n != null;) {
			if (n.getClass() == Leaf.class)
				return (Leaf) n;

			final Internal node = (Internal) n;
			final KeyRef[] a = node.keys;

			int low = 0;
			int high = a.length - 1;
			KeyRef match = null;
			int cmp = 0;
			while (low <= high) {
				final int mid = (low + high) >>> 1; // (low + high) / 2
				final KeyRef midVal = match = a[mid];
				final int d = cmp = -key.compareTo(midVal.min());
				// PRINT_IF(0, 0, "min : %s => mid : %s : %s, d : %s", n.key(), mid, midVal.min(), d);
				if (d <= 0)
					low = mid + 1;
				else
					high = mid - 1;
			}

			final KeyRef k = match;
			final int d = -cmp; // = key.compareTo(k.min());
			n = d < 0 ? k.left() : k.right();
			// PRINT_IF(0, 0, "min a : %s, match : %s, d : %s => %s", a.length, match, d, k);
		}
		return null;
	}
}
