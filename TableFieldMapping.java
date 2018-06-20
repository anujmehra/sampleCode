package com.am.analytics.job.model;

import java.io.Serializable;
import java.util.Collection;

/**
 * The Class TableFieldMapping have mapping information of HBase table.
 *
 * @param <T> the generic type
 */
public class TableFieldMapping<T extends ColumnField> implements Serializable {

    /**
     * Serialized version UID of the class.
     */
    private static final long serialVersionUID = 1L;

    /** The table name. */
    private final String tableName;

    /** The key field. */
    private final String keyField;

    /** The column fields. */
    private final Collection<T> columnFields;

    /**
     * Instantiates a new table field mapping.
     *
     * @param tableName the table name
     * @param keyField the key field
     * @param columnFields the column fields
     */
    public TableFieldMapping(final String tableName, final String keyField, final Collection<T> columnFields) {

        this.tableName = tableName;
        this.keyField = keyField;
        this.columnFields = columnFields;
    }

    /**
     * Gets the table name.
     *
     * @return the table name
     */
    public String getTableName() {

        return tableName;
    }

    /**
     * Gets the key field.
     *
     * @return the key field
     */
    public String getKeyField() {

        return keyField;
    }

    /**
     * Gets the column fields.
     *
     * @return the column fields
     */
    public Collection<T> getColumnFields() {

        return columnFields;
    }
}
