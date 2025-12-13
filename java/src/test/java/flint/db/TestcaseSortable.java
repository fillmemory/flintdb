/**
 * 
 */
package flint.db;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class TestcaseSortable {

	public static void main(String args[]) throws Exception {
		// testcase_basic_sort();
		testcase_file_sort();
	}

	static <T> void shuffle(T[] array) {
		Random rnd = ThreadLocalRandom.current();
		for (int i = array.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			T a = array[index];
			array[index] = array[i];
			array[i] = a;
		}
	}

	static void testcase_basic_sort() throws Exception {
		final Integer[] a = new Integer[1 * 1024];
		for (int i = 0; i < a.length; i++) {
			a[i] = i + 1;
		}
		shuffle(a);
		final java.util.ArrayList<Integer> aList = new java.util.ArrayList<>();
		for (int i : a)
			aList.add(i);

		System.out.println("Unsorted:");
		System.out.println((aList));

		Sortable.sort(new Sortable<Integer>() {
			@Override
			public Integer get(long i) {
				return aList.get((int) i);
			}

			@Override
			public void put(long i, Integer v) {
				aList.set((int) i, v);
			}

			@Override
			public long count() {
				return aList.size();
			}
		}, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return Integer.compare(o1, o2);
			}
		});

		System.out.println("Sorted:");
		System.out.println(aList);
		System.out.println("count : " + aList.size());
	}


	static void testcase_file_sort() throws Exception {
		var meta = new Meta()
			.columns(
				new Column[] {
					new Column.Builder("A", Column.TYPE_INT).create(),
					new Column.Builder("B", Column.TYPE_INT).create(),
				}
			)
		;

		try(var sorter = new Sortable.FileSorter(meta)) {
			Random rnd = ThreadLocalRandom.current();
			java.util.Set<Integer> usedA = new java.util.HashSet<>();
			int count = 0;
			final int MAX = 30;
			while(count < MAX) {
				int a = rnd.nextInt(MAX) + 1;
				if (usedA.contains(a)) continue;
				usedA.add(a);
				int b = rnd.nextInt(MAX) + 1;
				sorter.add(Row.create(meta, new Object[] {a, b}));
				count++;
			}

			System.out.println("-------- original rows -------- ");
			for(int i=0; i<sorter.rows(); i++) {
				System.out.println(sorter.read(i));
			}

			final int DIR = 1;
			sorter.sort((Row r1, Row r2) -> {
				int d = r1.getInt("A").compareTo(r2.getInt("A"));
				if (d == 0)
					d = r1.getInt("B").compareTo(r2.getInt("B"));
				return d * DIR;
			});

			System.out.println("-------- sorted rows -------- ");
			for(int i=0; i<sorter.rows(); i++) {
				System.out.println(sorter.read(i));
			}
		}
	}
}
