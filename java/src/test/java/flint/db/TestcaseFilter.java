package flint.db;

public class TestcaseFilter {

	static void testcase1() {
		String s = "A = '00007a385f37436cac8b14776f54c65a' AND B = 1";
		Index index = new Table.PrimaryKey(new String[] { "A", "B", "C" });
		Meta meta = new Meta() //
				.columns(new Column[] { //
						new Column.Builder("A", Column.TYPE_STRING).bytes(40).create(), //
						new Column.Builder("B", Column.TYPE_INT).create(), //
						new Column.Builder("C", Column.TYPE_INT).create(), //
						new Column.Builder("Z", Column.TYPE_INT).create(), //
				}) //
				.indexes(new Index[] { index });

		System.out.println("trim_mws => " + SQL.trim_mws(s));
		final Comparable<Row> filter = Filter.compile(meta, index, s)[0];
		System.out.println(filter);

		System.out.println("d : " + filter.compareTo(Row.create(meta, new Object[] { "00007a385f37436cac8b14776f54c65a", -1, "9", "1" })));
		System.out.println("d : " + filter.compareTo(Row.create(meta, new Object[] { "00007a385f37436cac8b14776f54c65a", 1, "9", "1" })));
		System.out.println("d : " + filter.compareTo(Row.create(meta, new Object[] { "00007a385f37436cac8b14776f54c65a", 2, "9", "1" })));
	}

	public static void main(String[] args) throws Exception {
		testcase1();
	}
}
