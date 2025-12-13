/**
 * Sortable.java
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * An interface designed to facilitate sorting operations for arrays or lists, leveraging file or memory storage.
 * 
 * <pre>
 * References:
 * https://www.geeksforgeeks.org/heap-sort/
 * https://www.geeksforgeeks.org/dual-pivot-quicksort/
 * </pre>
 */
public interface Sortable<T> {

	T get(long i) throws Exception;

	void put(long i, T v) throws Exception;

	long count() throws Exception;

	public static <T> void sort(final Sortable<T> array, final Comparator<T> comparator) throws Exception {
		quick(array, comparator);
	}

	public static <T> void quick(final Sortable<T> array, final Comparator<T> comparator) throws Exception {
		final long n = array.count();
		if (n <= 1) return;
		DualPivotQuickSort.sort(array, 0, n - 1, comparator);
	}

	static final class DualPivotQuickSort {

		static <T> void sort(final Sortable<T> array, final long low, final long high, final Comparator<T> comparator) throws Exception {
			if (low < high) {
				// Small partitions benefit a lot from insertion sort
				if (high - low + 1 <= 32) {
					insertionSort(array, low, high, comparator);
					return;
				}
				// piv[] stores left pivot and right pivot.
				// piv[0] means left pivot and
				// piv[1] means right pivot
				final long[] piv = partition(array, low, high, comparator);

				sort(array, low, piv[0] - 1, comparator); 
				sort(array, piv[0] + 1, piv[1] - 1, comparator);
				sort(array, piv[1] + 1, high, comparator);
			}
		}

		private static <T> void swap(final Sortable<T> array, final long i, final long j) throws Exception {
			final T temp = array.get(i);
			array.put(i, array.get(j));
			array.put(j, temp);
		}

		private static <T> long[] partition(final Sortable<T> array, final long low, final long high, final Comparator<T> comparator) throws Exception {
			// Read once and compare to reduce get() calls
			T al = array.get(low);
			T ah = array.get(high);
			if (comparator.compare(al, ah) > 0) {
				swap(array, low, high);
				al = array.get(low);
				ah = array.get(high);
			}

			// p is the left pivot, and q
			// is the right pivot.
			long j = low + 1;
			long g = high - 1, k = low + 1;
			T p = al, q = ah;

			while (k <= g) {

				// Cache array.get(k) once per iteration
				T ak = array.get(k);

				// If elements are less than the left pivot
				if (comparator.compare(ak, p) < 0) {
					swap(array, k, j);
					j++;
				} else if (comparator.compare(ak, q) >= 0) {
					// If elements are greater than or equal to the right pivot
					while (k < g) {
						T ag = array.get(g);
						if (comparator.compare(ag, q) > 0) {
							g--;
							continue;
						}
						// swap k and g
						swap(array, k, g);
						g--;
						// element at position k has changed; refresh ak
						ak = array.get(k);
						break;
					}

					if (comparator.compare(ak, p) < 0) {
						swap(array, k, j);
						j++;
					}
				}
				k++;
			}
			j--;
			g++;

			// Bring pivots to their appropriate positions.
			swap(array, low, j);
			swap(array, high, g);

			// Returning the indices of the pivots
			// because we cannot return two elements
			// from a function, we do that using an array.
			return new long[] { j, g };
		}

		private static <T> void insertionSort(final Sortable<T> array, final long low, final long high, final Comparator<T> comparator) throws Exception {
			for (long i = low + 1; i <= high; i++) {
				final T key = array.get(i);
				long j = i - 1;
				while (j >= low && comparator.compare(array.get(j), key) > 0) {
					array.put(j + 1, array.get(j));
					j--;
				}
				array.put(j + 1, key);
			}
		}
	}


	static final class HeapSort {
		
		public static <T> void sort(final Sortable<T> array, final Comparator<T> comparator) throws Exception {
			long N = array.count();
			// Build heap (rearrange array)
			for (long i = N / 2 - 1; i >= 0; i--)
				heapify(array, N, i, comparator);

			// One by one extract an element from heap
			for (long i = N - 1; i > 0; i--) {
				// Move current root to end
				T temp = array.get(0);
				array.put(0, array.get(i));
				array.put(i, temp);

				// call max heapify on the reduced heap
				heapify(array, i, 0, comparator);
			}
		}

		private static <T> void heapify(final Sortable<T> array, final long N, final long i, final Comparator<T> comparator) throws Exception {
			long largest = i; // Initialize largest as root
			long l = 2 * i + 1; // left = 2*i + 1
			long r = 2 * i + 2; // right = 2*i + 2

			// If left child is larger than root
			if (l < N && comparator.compare(array.get(l), array.get(largest)) > 0)
				largest = l;

			// If right child is larger than largest so far
			if (r < N && comparator.compare(array.get(r), array.get(largest)) > 0)
				largest = r;

			// If largest is not root
			if (largest != i) {
				final T swap = array.get(i);
				array.put(i, array.get(largest));
				array.put(largest, swap);

				// Recursively heapify the affected sub-tree
				heapify(array, N, largest, comparator);
			}
		}
	}

	static final class MergeSort {
		public static <T> void sort(final Sortable<T> array, final Comparator<T> comparator) throws Exception {
			final long n = array.count();
			if (n <= 1) return;

			final Object[] aux = new Object[(int) n];

			// Bottom-up iterative merge sort for better stack usage and locality
			for (long width = 1; width < n; width <<= 1) {
				for (long left = 0; left < n - width; left += (width << 1)) {
					final long mid = left + width - 1;
					final long right = Math.min(left + (width << 1) - 1, n - 1);

					// Skip merge if already ordered
					final T midVal = array.get(mid);
					final T nextVal = array.get(mid + 1);
					if (comparator.compare(midVal, nextVal) <= 0) continue;

					merge(array, left, mid, right, aux, comparator);
				}
			}
		}


		private static <T> void merge(final Sortable<T> array,
									long left, long mid, long right,
									Object[] aux,
									Comparator<T> comparator) throws Exception {
			long i = left;
			long j = mid + 1;
			int k = (int) left;

			// Prime caches to avoid repeated get() calls
			T vi = i <= mid ? array.get(i) : null;
			T vj = j <= right ? array.get(j) : null;

			while (i <= mid && j <= right) {
				if (comparator.compare(vi, vj) <= 0) {
					aux[k++] = vi;
					i++;
					vi = i <= mid ? array.get(i) : null;
				} else {
					aux[k++] = vj;
					j++;
					vj = j <= right ? array.get(j) : null;
				}
			}

			while (i <= mid) {
				aux[k++] = vi;
				i++;
				vi = i <= mid ? array.get(i) : null;
			}

			while (j <= right) {
				aux[k++] = vj;
				j++;
				vj = j <= right ? array.get(j) : null;
			}

			for (long p = left; p <= right; p++) {
				@SuppressWarnings("unchecked")
				final T val = (T) aux[(int) p];
				array.put(p, val);
			}
		}
	}


	/**
	 * FileSorter is a utility class for sorting large datasets using file-based storage.
	 * It manages row data and index files, allowing efficient addition, retrieval,
	 * and sorting of rows without loading all data into memory. Designed for use with
	 * external sorting algorithms and supports heap sort via the Sortable interface.
	 * Implements AutoCloseable for safe resource management.
	 */
	public static final class FileSorter implements AutoCloseable {
		private final File[] files;
		private final List<Long> offsets = new java.util.ArrayList<>();
		private final Storage data;
		private final Formatter.BINROWFORMATTER formatter;
		private volatile long rows = 0;
		private final IO.Closer closer = new IO.Closer();
        private static final File TEMP_DIR = IO.tempdir();

		/**
		 * Constructs a FileSorter for the given file and table metadata.
		 * Initializes row formatter, index, and data file channels.
		 * @param file The base file for sorting and index storage.
		 * @param meta Table metadata describing row format.
		 * @throws IOException If file operations fail.
		 */
		public FileSorter(final File file, final Meta meta) throws IOException {
			// Calculate row bytes, use safe default for variable-length rows
			int rowBytes = 4080; // Default for variable-length rows with compact mode
			try {
				int calculated = meta.rowBytes();
				// Only use calculated value if it's valid and within short range
				if (calculated > 0 && calculated <= 4080) {
					rowBytes = calculated;
				}
			} catch (Exception e) {
				// Use default value if calculation fails
			}
			
			this.formatter = closer.register(new Formatter.BINROWFORMATTER(rowBytes, meta));
			this.files = new File[] { // index and data files, it will be deleted on close
                closer.register(file), 
                closer.register(new File(file.getAbsolutePath() + ".data")) 
            };
			this.data = closer.register(Storage.create(
				new Storage.Options()
				.blockBytes((short)rowBytes)
                .compact(compact((short)rowBytes))
				.file(files[0])
			));
		}

		public FileSorter(final Meta meta) throws IOException {
			this(tempfile(), meta);
		}

        static int compact(final short bytes) {
            if (bytes >= 4080) return 4080; // storage block header (16) + data (4080) = 4096
            return -1;
        }

		static File tempfile() {
			var file = new File(TEMP_DIR, "filesort-" + (System.nanoTime()) + ".sort");
			file.getParentFile().mkdirs();
			return file;
		}

		/**
		 * Closes all resources associated with this FileSorter, including files and channels.
		 */
		@Override
		public void close() {
			closer.close();
		}

		/**
		 * Returns the number of rows currently added to the sorter.
		 * @return Number of rows.
		 */
		public long rows() {
			return rows;
		}

		/**
		 * Adds a row to the sorter, writing its data and index to disk.
		 * @param row The row to add.
		 * @return Always returns 0 (reserved for future use).
		 * @throws IOException If writing fails.
		 */
		public long add(final Row row) throws IOException {
			IoBuffer buffer = null;
			try {
				buffer = formatter.format(row);
				long offset = data.write(buffer);

				offsets.add(offset);
				rows++;
				return 0;
			} finally {
				formatter.release(buffer);
			}
		}

		/**
		 * Retrieves a row by its index from disk storage.
		 * @param i The row index.
		 * @return The Row object at the specified index.
		 * @throws Exception If reading fails.
		 */
		public Row read(final long i) throws Exception {
			var offset = offsets.get((int) i);
			var buffer = data.read(offset);
			var r = formatter.parse(buffer);
			r.id(offset); // for swap
			return r;
		}

		/**
		 * Sorts all rows using the provided comparator and heap sort algorithm.
		 * @param comparator Comparator for row ordering.
		 * @return The number of rows sorted.
		 * @throws Exception If sorting fails.
		 */
		public long sort(final Comparator<Row> comparator) throws Exception {
			final long total = rows();
			var sort = new Sortable<Row>() {
                @Override
                public Row get(long i) throws Exception {
                    return read(i);
                }

                @Override
                public void put(long i, Row r) throws Exception {
					offsets.set((int) i, r.id());
                }

                @Override
                public long count() throws Exception {
                    return total;
                }
            };

			MergeSort.sort(sort, comparator);
			return total;
		}
	}
}
