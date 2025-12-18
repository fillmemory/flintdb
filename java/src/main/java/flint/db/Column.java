/**
 * 
 */
package flint.db;

/**
 * Column definition for table structure
 * 
 * Defines column properties including name, data type, size constraints,
 * precision, default values, and comments. Supports various data types
 * optimized for different use cases.
 */
public final class Column {
    // Null and Basic Integer Types
    /** Null value type */
    public static final short TYPE_NULL = 0;
    /** Zero value type */
    static final short TYPE_ZERO = 1; // reserved for future use
    /** 32-bit signed integer */
    public static final short TYPE_INT = 2;
    /** 32-bit unsigned integer */
    public static final short TYPE_UINT = 3;

    // 8-bit Integer Types
    /** 8-bit signed integer */
    public static final short TYPE_INT8 = 4;
    /** 8-bit unsigned integer */
    public static final short TYPE_UINT8 = 5;

    // 16-bit Integer Types
    /** 16-bit signed integer */
    public static final short TYPE_INT16 = 6;
    /** 16-bit unsigned integer */
    public static final short TYPE_UINT16 = 7;

    // 64-bit and Floating Point Types
    /** 64-bit signed integer */
    public static final short TYPE_INT64 = 8;
    /** 64-bit double precision floating point */
    public static final short TYPE_DOUBLE = 9;
    /** 32-bit single precision floating point */
    public static final short TYPE_FLOAT = 10;

    // String and Numeric Types
    /** Variable-length string with configurable byte limit */
    public static final short TYPE_STRING = 11;
    /** Fixed-point decimal number with precision control */
    public static final short TYPE_DECIMAL = 12;
    /** Binary data (reserved for future use) */
    public static final short TYPE_BYTES = 13; // reserved for future

    // Date and Time Types
    /** Date value (yyyy-MM-dd format) */
    public static final short TYPE_DATE = 14;
    /** Timestamp value (yyyy-MM-dd HH:mm:ss format) */
    public static final short TYPE_TIME = 15;

    // Special Types
    /** UUID (128-bit universally unique identifier) */
    public static final short TYPE_UUID = 16;
    /** IPv6 address (128-bit) */
    public static final short TYPE_IPV6 = 17;
    /** Binary large object (reserved for future use) */
    static final short TYPE_BLOB = 18; // reserved for future

    // public static final short TYPE_HYPERLOGLOG = 21; // reserved for future
    // public static final short TYPE_ROARINGBITMAP = 22; // reserved for future
    // public static final short TYPE_GEOPOINT = 30; // reserved for future

    /** Object type (reserved for future use) */
    static final short TYPE_OBJECT = 31; // reserved for future

    private final String name;
    private final short bytes;
    private final Short precision;
    private final short type;
    private final boolean notnull;
    private final Object value;
    private final String comment;

    /**
     * Constructor for Column definition
     * 
     * @param name      Column name (will be normalized to lowercase)
     * @param type      Data type constant
     * @param bytes     Maximum byte length for the column
     * @param precision Decimal precision (for decimal types)
     * @param notnull   Whether the column is NOT NULL
     * @param value     Default value
     * @param comment   Column description
     */
    public Column(final String name, final short type, final short bytes, final short precision, final boolean notnull,
            final Object value, final String comment) {
        this.name = normalize(name);
        this.bytes = bytes(type, bytes, precision);
        this.precision = precision > 0 ? precision : null;
        this.type = type;
        this.notnull = notnull;
        this.value = Row.cast(value, type, precision);
        this.comment = comment;
    }

    /**
     * Normalize column name to lowercase
     * 
     * @param name Original column name
     * @return Normalized lowercase name
     */
    public static String normalize(final String name) {
        return name.toLowerCase();
    }

    /**
     * Get column name
     * 
     * @return Column name (normalized to lowercase)
     */
    public String name() {
        return name;
    }

    /**
     * Get maximum byte length for this column
     * 
     * @return Byte length
     */
    public short bytes() {
        return bytes;
    }

    /**
     * Get decimal precision for this column
     * 
     * @return Precision value, 0 if not applicable
     */
    @SuppressWarnings("UnnecessaryUnboxing")
    public short precision() {
        return (precision != null && precision > 0) ? precision.shortValue() : 0;
    }

    /**
     * Get data type constant for this column
     * 
     * @return Type constant (e.g., TYPE_STRING, TYPE_INT)
     */
    public short type() {
        return type;
    }

    public boolean notnull() {
        return notnull;
    }

    /**
     * Get default value for this column
     * 
     * @return Default value (properly cast to column type)
     */
    public Object value() {
        return value;
    }

    /**
     * Get column comment/description
     * 
     * @return Column comment or null if none
     */
    public String comment() {
        return comment;
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * Builder pattern for creating Column instances
     * 
     * Provides a fluent interface for constructing columns with optional
     * parameters.
     */
    public static final class Builder {
        private final String name;
        private final short type;
        private short bytes = -1;
        private Short precision = -1;
        private boolean notnull = false;
        private Object value;
        private String comment;

        /**
         * Create column builder with name and type
         * 
         * @param name Column name
         * @param type Data type constant
         */
        public Builder(final String name, final short type) {
            this.name = name;
            this.type = type;
        }

        /**
         * Set byte length for the column
         * 
         * @param bytes Maximum byte length (0 to Short.MAX_VALUE)
         * @return This builder for chaining
         * @throws RuntimeException if bytes out of valid range
         */
        public Builder bytes(final int bytes) {
            if (bytes < 0 || bytes > Short.MAX_VALUE)
                throw new RuntimeException("Bytes " + bytes + " out of bounds for range 0 ~ " + Short.MAX_VALUE);
            this.bytes = (short) bytes;
            return this;
        }

        /**
         * Set byte length and precision for decimal columns
         * 
         * @param bytes     Maximum byte length (0 to Short.MAX_VALUE)
         * @param precision Decimal precision (0 to bytes)
         * @return This builder for chaining
         * @throws RuntimeException if parameters out of valid range
         */
        public Builder bytes(final int bytes, final int precision) {
            if (bytes < 0 || bytes > Short.MAX_VALUE)
                throw new RuntimeException("Bytes " + bytes + " out of bounds for range 0 ~ " + Short.MAX_VALUE);
            if (precision < 0 || precision > bytes)
                throw new RuntimeException("Precision " + bytes + " out of bounds for range 0 ~ " + bytes);
            this.bytes = (short) bytes;
            this.precision = (short) precision;
            return this;
        }

        /**
         * Set NOT NULL constraint for the column
         * 
         * @param notnull Whether the column is NOT NULL
         * @return This builder for chaining
         */
        public Builder notnull(final boolean notnull) {
            this.notnull = notnull;
            return this;
        }

        /**
         * Set default value for the column
         * 
         * @param value Default value (will be cast to appropriate type)
         * @return This builder for chaining
         */
        public Builder value(final Object value) {
            this.value = value;
            return this;
        }

        /**
         * Set comment/description for the column
         * 
         * @param comment Column description
         * @return This builder for chaining
         */
        public Builder comment(final String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * Create the Column instance with configured parameters
         * 
         * @return Configured Column instance
         */
        public Column create() {
            return new Column(name, type, bytes, precision, notnull, value, comment);
        }
    }

    /**
     * Get human-readable type name for a type constant
     * 
     * @param type Type constant
     * @return Type name string
     */
    public static String typename(int type) {
        switch (type) {
            case Column.TYPE_INT:
                return "TYPE_INT";
            case Column.TYPE_UINT:
                return "TYPE_UINT";

            case Column.TYPE_INT8:
                return "TYPE_INT8";
            case Column.TYPE_UINT8:
                return "TYPE_UINT8";

            case Column.TYPE_INT16:
                return "TYPE_INT16";
            case Column.TYPE_UINT16:
                return "TYPE_UINT16";

            case Column.TYPE_INT64:
                return "TYPE_INT64";
            case Column.TYPE_DOUBLE:
                return "TYPE_DOUBLE";
            case Column.TYPE_FLOAT:
                return "TYPE_FLOAT";

            case Column.TYPE_DATE:
                return "TYPE_DATE";
            case Column.TYPE_TIME:
                return "TYPE_TIME";
            case Column.TYPE_UUID:
                return "TYPE_UUID";
            case Column.TYPE_IPV6:
                return "TYPE_IPV6";

            case Column.TYPE_STRING:
                return "TYPE_STRING";

            case Column.TYPE_DECIMAL:
                return "TYPE_DECIMAL";
            case Column.TYPE_BYTES:
                return "TYPE_BYTES";

            case Column.TYPE_BLOB:
                return "TYPE_BLOB";
            case Column.TYPE_OBJECT:
                return "TYPE_OBJECT";
            default:
                return "unknown";
        }
    }

    /**
     * Parse type constant from type name string
     * 
     * @param type Type name (case insensitive)
     * @return Type constant
     * @throws RuntimeException if type name is unknown
     */
    public static short valueOf(final String type) {
        switch ("TYPE_".concat(type.toUpperCase())) {
            case "TYPE_INT":
                return TYPE_INT;
            case "TYPE_UINT":
                return TYPE_UINT;
            case "TYPE_INT8":
                return TYPE_INT8;
            case "TYPE_UINT8":
                return TYPE_UINT8;
            case "TYPE_INT16":
                return TYPE_INT16;
            case "TYPE_UINT16":
                return TYPE_UINT16;
            case "TYPE_INT64":
                return TYPE_INT64;
            case "TYPE_DOUBLE":
                return TYPE_DOUBLE;
            case "TYPE_FLOAT":
                return TYPE_FLOAT;
            case "TYPE_DATE":
                return TYPE_DATE;
            case "TYPE_TIME":
                return TYPE_TIME;
            case "TYPE_UUID":
                return TYPE_UUID;
            case "TYPE_IPV6":
                return TYPE_IPV6;
            case "TYPE_STRING":
                return TYPE_STRING;
            case "TYPE_DECIMAL":
                return TYPE_DECIMAL;
            case "TYPE_BYTES":
                return TYPE_BYTES;
            case "TYPE_BLOB":
                return TYPE_BLOB;
            case "TYPE_OBJECT":
                return TYPE_OBJECT;
        }
        // try {
        // return Short.parseShort(type);
        // } catch (Exception ex) {
        // }
        throw new RuntimeException("UNKNOWN TYPE : " + type);
    }

    /**
     * Calculate appropriate byte length for a column type
     * 
     * @param type  Column type constant
     * @param bytes Requested byte length (for variable-length types)
     * @return Actual byte length to use
     * @throws IllegalArgumentException if type/bytes combination is invalid
     */
    static short bytes(final short type, final short bytes) {
        switch (type) {
            case Column.TYPE_STRING:
                if (bytes <= 0)
                    throw new java.lang.IllegalArgumentException("TYPE_STRING");
                return (short) (bytes);
            case Column.TYPE_DATE:
                return 3;
            case Column.TYPE_TIME:
                return 8;

            case Column.TYPE_INT:
            case Column.TYPE_UINT:
                return Integer.BYTES;
            case Column.TYPE_INT8:
            case Column.TYPE_UINT8:
                return Byte.BYTES;

            case Column.TYPE_INT16:
            case Column.TYPE_UINT16:
                return Short.BYTES;

            case Column.TYPE_INT64:
                return Long.BYTES;

            case Column.TYPE_DOUBLE:
                return Double.BYTES;
            case Column.TYPE_FLOAT:
                return Float.BYTES;

            case Column.TYPE_UUID:
                return 16;
            case Column.TYPE_IPV6:
                return 16;
            case Column.TYPE_DECIMAL:
                if (bytes <= 0)
                    return 8 + 1;
                return (short) (bytes);

            case Column.TYPE_BYTES:
                return (short) (bytes);
            case Column.TYPE_BLOB:
            case Column.TYPE_OBJECT:
                throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
            default:
                throw new java.lang.IllegalArgumentException("Column.TYPE_UNKNOWN : " + type);
        }
    }

    /**
     * Compute effective bytes with precision awareness for DECIMAL when bytes is
     * not provided.
     * For other types, behavior is identical to bytes(type, bytes).
     */
    static short bytes(final short type, final short bytes, final short precision) {
        if (type == Column.TYPE_DECIMAL && bytes <= 0 && precision > 0) {
            // Estimate worst-case unscaled byte length from precision.
            // bits = ceil(precision * log2(10))
            // bytes = ceil(bits/8) + 1 (sign/two's-complement headroom)
            double bitsD = Math.ceil(precision * 3.3219280948873626d);
            int req = (int) Math.ceil(bitsD / 8.0d) + 1;
            if (req <= 0)
                req = 9; // fallback
            if (req > Short.MAX_VALUE)
                req = Short.MAX_VALUE;
            return (short) req;
        }
        return bytes(type, bytes);
    }
}
