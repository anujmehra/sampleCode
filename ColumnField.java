package com.am.analytics.job.model;

import java.io.Serializable;

/**
 * The Interface ColumnField.
 */
public interface ColumnField extends Serializable {

    /**
     * Gets the field name.
     *
     * @return the field name
     */
    public String getFieldName();

    /**
     * Gets the column name.
     *
     * @return the column name
     */
    public String getColumnName();
}
