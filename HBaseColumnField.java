am com.aeromexico.analytics.job.model;

/**
 * The Class HBaseColumnField is responsible for fields in HBase.
 */
public class HBaseColumnField extends BasicColumnField {

    /**
     * Serialized version UID of the class.
     */
    private static final long serialVersionUID = 1L;

    /** The column family. */
    private final String columnFamily;

    /**
     * Instantiates a new h base column field.
     *
     * @param fieldName the field name
     * @param columnFamily the column family
     * @param columnName the column name
     */
    public HBaseColumnField(final String fieldName, final String columnFamily, final String columnName) {

        super(fieldName, columnName);
        this.columnFamily = columnFamily;
    }

    /**
     * Instantiates a new h base column field.
     *
     * @param fieldName the field name
     * @param columnFamily the column family
     */
    public HBaseColumnField(final String fieldName, final String columnFamily) {

        this(fieldName, columnFamily, fieldName);
    }

    /**
     * Gets the column family.
     *
     * @return the column family
     */
    public String getColumnFamily() {

        return columnFamily;
    }

}
