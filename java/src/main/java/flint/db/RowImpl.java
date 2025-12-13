/**
 * 
 */
package flint.db;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 *
 */
final class RowImpl implements Row {
	final Meta meta;
	final Object[] array;
	// private final ByteBuffer raw;
	long id = -1L;

	RowImpl(final Meta meta, final Object[] array) {
		if (array == null || array.length == 0) // || array[0] == null
			throw new IllegalArgumentException();
		this.meta = meta;
		this.array = array;
	}

	@Override
	public Meta meta() {
		return meta;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(id);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public boolean equals(final Object o) {
		final Row r = (Row) o;
		if (id != -1 && id == r.id())
			return true;
		if (array.length != r.array().length)
			return false;

		for (int i = 0; i < array.length; i++) {
			final Object v1 = array[i];
			final Object v2 = r.get(i);

			if (v1 == v2)
				continue;
			if (v1 != null && v2 == null)
				return false;
			if (v1 == null && v2 != null)
				return false;

			int d = 0;
			try {
				if ((v1 instanceof Comparable && v2 instanceof Comparable)) {
					d = ((Comparable) v1).compareTo((Comparable) v2);
					// System.err.println("d : " + d + ", i : " + i + ", v1 : " + v1 + ", v2 : " + v2);
					if (d == 0)
						continue;
					return false;
				}

				if (v1 instanceof byte[] && v2 instanceof byte[]) {
					d = java.util.Arrays.compare((byte[]) v1, (byte[]) v2);
					// System.err.println("d : " + d + ", i : " + i + ", v1 : " + v1 + ", v2 : " + v2);
					if (d == 0)
						continue;
					return false;
				}
			} catch (Exception ex) {
				System.err.println("" + v1 + " <> " + v2 + " " + (v1 != null ? v1.getClass() : null) + ", " + (v2 != null ? v2.getClass() : null) + " " + ex.getMessage());
				// ex.printStackTrace();
				throw ex;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		// try {
		// final TSVFile.XROWFORMATTER formatter = new TSVFile.XROWFORMATTER("", meta.columns(), //
		// new TSVFile.Format() //
		// .setDelimiter('\t') //
		// .setNull("\\N") //
		// );
		// return new String(formatter.format(this));
		// } catch (Exception ex) {
		// ex.printStackTrace();
		// }
		return toString(",");
	}

	@Override
	public String toString(final String delimiter) {
		final StringBuilder s = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			if (i > 0)
				s.append(delimiter);
			s.append(array[i]);
		}
		return s.toString();
	}	

	@Override
	public Row copy() {
		final Object[] a = new Object[array.length];
		for (int i = 0; i < a.length; i++)
			a[i] = array[i];
		final RowImpl r = new RowImpl(meta, a);
		r.id = id;
		return r;
	}

	@Override
	public int size() {
		return array.length;
	}

	@Override
	public Object[] array() {
		return array;
	}

	@Override
	public long id() {
		return this.id;
	}

	@Override
	public void id(final long id) {
		this.id = id;
	}

	@Override
	public <T extends Map<String, Object>> T map(final T dest) {
		final Column[] columns = meta.columns();
		for (int i = 0; i < columns.length; i++) {
			final Column c = columns[i];
			final Object v = array[i];
			dest.put(c.name(), Row.cast(v, c.type(), c.precision()));
		}
		return dest;
	}

    @Override
    public Map<String, Object> map() {
        return map(new java.util.LinkedHashMap<>());
    }

	private int column(final String name) {
		return meta.column(name);
	}

	@Override
	public boolean contains(final String name) {
		return column(name) > -1;
	}

	@Override
	public Object get(final int i) {
		return array[i];
	}

	@Override
	public Object get(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1) {
			final StringBuilder s = new StringBuilder();
			final Column[] columns = meta.columns();
			for (int n = 0; n < columns.length; n++) {
				if (n > 0)
					s.append(", ");
				s.append(columns[n].name());
			}
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, s));
		}

		return array[i];
	}

	@Override
	public void set(final int i, final Object value) {
		array[i] = Row.cast(value, meta.columns()[i].type(), meta.columns()[i].precision());
	}

	@Override
	public void set(final String name, final Object value) {
		final int i = column(name);
		if (i == -1)
			throw new ArrayIndexOutOfBoundsException("IndexOutOfBoundsException => " + name);
		set(i, value);
	}

	@Override
	public String getString(final int i) {
		final Object v = array[i];
		return (v != null) ? v.toString() : null;
	}

	@Override
	public String getString(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getString(i);
	}

	@Override
	public Integer getInt(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Integer)
			return (Integer) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).intValue() : null;
	}

	@Override
	public Integer getInt(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getInt(i);
	}

	@Override
	public Long getLong(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Long)
			return (Long) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).longValue() : null;
	}

	@Override
	public Long getLong(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getLong(i);
	}

	@Override
	public Double getDouble(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Double)
			return (Double) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).doubleValue() : null;
	}

	@Override
	public Double getDouble(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getDouble(i);
	}

	@Override
	public Float getFloat(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Float)
			return (Float) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).floatValue() : null;
	}

	@Override
	public Float getFloat(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getFloat(i);
	}

	@Override
	public Short getShort(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Short)
			return (Short) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).shortValue() : null;
	}

	@Override
	public Short getShort(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getShort(i);
	}

	@Override
	public Byte getByte(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof Byte)
			return (Byte) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i).byteValue() : null;
	}

	@Override
	public Byte getByte(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getByte(i);
	}

	@Override
	public BigDecimal getBigDecimal(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof BigDecimal)
			return (BigDecimal) array[i];

		final String v = getString(i);
		return (v != null && !"".equals(v)) ? castAsNumber(v, i) : null;
	}

	@Override
	public BigDecimal getBigDecimal(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getBigDecimal(i);
	}

	private BigDecimal castAsNumber(final Object v, final int i) {
		try {
			return new BigDecimal(v.toString());
		} catch (NumberFormatException ex) {
			final Column c = meta.columns()[i];
			throw new NumberFormatException("column " + c.name() + " type " + Column.typename(c.type()) + ", but " + ("".equals(v) ? "''" : v));
		}
	}

	@Override
	public Date getDate(final int i) {
		final Object v = get(i);
		if (v != null) {
			if (v instanceof Date)
				return (Date) v;
			if (v instanceof String)
				return Row.date((String) v);
		}
		return null;
	}

	@Override
	public Date getDate(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getDate(i);
	}

	@Override
	public byte[] getBytes(final int i) {
		if (array[i] == null)
			return null;
		if (array[i] instanceof byte[])
			return (byte[]) array[i];
		return null;
	}

	@Override
	public byte[] getBytes(final String name) {
		final int i = column(name);
		if (i >= array.length)
			throw new ArrayIndexOutOfBoundsException(String.format(".get(%s) : %s >= %s", name, i, array.length));
		if (i == -1)
			throw new java.util.NoSuchElementException(String.format(".get(%s) : %s", name, i));
		return getBytes(i);
	}


    @Override
    public boolean validate() {
        final Column[] columns = meta.columns();
        for (int i = 0; i < columns.length; i++) {
            final Column c = columns[i];
            final Object v = array[i];
            if (v == null && c.notnull())
                return false;
        }
        return true;
    }
}
