package com.am.analytics.job.model;

/**
 * The Class BasicColumnField is responsible to implementation basic behavior
 * of @ColumnField
 */
public class BasicColumnField implements ColumnField {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The field name. */
    private final String fieldName;

    /** The column name. */
    private final String columnName;

    /**
     * Instantiates a new basic column field.
     *
     * @param fieldName the field name
     */
    public BasicColumnField(final String fieldName) {

        this(fieldName, fieldName);
    }

    /**
     * Instantiates a new basic column field.
     *
     * @param fieldName the field name
     * @param columnName the column name
     */
    public BasicColumnField(final String fieldName, final String columnName) {

        this.fieldName = fieldName;
        this.columnName = columnName;
    }

    /* (non-Javadoc)
     * @see com.am.analytics.job.model.ColumnField#getFieldName()
     */
    public String getFieldName() {

        return fieldName;
    }

    /* (non-Javadoc)
     * @see com.am.analytics.job.model.ColumnField#getColumnName()
     */
    public String getColumnName() {

        return columnName;
    }
}
