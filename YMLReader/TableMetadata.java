package com.am.analytics.job.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Class TableMetadata havemetadata information of HBase tables.
 */
public class TableMetadata {

    /** The name. */
    @JsonProperty("name")
    private String name;

    /** The column prefix. */
    @JsonProperty("columnPrefix")
    private String columnPrefix;

    /** The columns. */
    @JsonProperty("column")
    private ColumnMetadata[] columns;

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {

        return name;
    }

    /**
     * Gets the column prefix.
     *
     * @return the column prefix
     */
    public String getColumnPrefix() {

        return columnPrefix;
    }

    /**
     * Gets the columns.
     *
     * @return the columns
     */
    public ColumnMetadata[] getColumns() {

        return columns;
    }

}
