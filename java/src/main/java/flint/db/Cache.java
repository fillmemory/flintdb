/**
 * Cache.java
 */
package flint.db;

/**
 * A simple cache interface for storing key-value pairs.
 * It provides methods to put, get, remove, and clear entries.
 * The cache can be implemented with a fixed size, and it will evict the oldest entry
 * when the size limit is reached.
 */
interface Cache<K, V> extends java.io.Closeable {
	V put(final K k, final V v);

	V get(final K k);

	V remove(final K k);

	void clear();

	static <K, V> Cache<K, V> create(final int size) { // , final Comparator<K> comparator
		return size > 1 ? new Default<>(size) : new Null<>();
	}

	static final class Null<K, V> implements Cache<K, V> {
		@Override
		public V put(final K k, final V v) {
			return null;
		}

		@Override
		public V get(final K k) {
			return null;
		}

		@Override
		public V remove(final K k) {
			return null;
		}

		@Override
		public void clear() {
		}

		@Override
		public void close() {
		}
	}

	static final class Default<K, V>  implements Cache<K, V> { 
        private final java.util.Map<K, V> map;

		public Default(final int capacity) {
            // java.util.Collections.synchronizedMap
            this.map = (new java.util.LinkedHashMap<>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(final java.util.Map.Entry<K, V> eldest) {
                    return this.size() > capacity;
                }
            });
		}

		@Override
		public void close() {
			this.clear();
		}

        @Override
		public V put(final K k, final V v) {
			return map.put(k, v);
		}

		@Override
		public V get(final K k) {
			return map.get(k);
		}

		@Override
		public V remove(final K k) {
			return map.remove(k);
		}

		@Override
		public void clear() {
            map.clear();
		}
	}
}
