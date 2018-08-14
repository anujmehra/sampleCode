package com.am.analytics.job.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Class DatabaseMetadata have metadata information related to database.
 */
public class DatabaseMetadata {

    /** The database name. */
    @JsonProperty("name")
    private String databaseName;

    /** The table metadata. */
    @JsonProperty("table")
    private TableMetadata tableMetadata;

    /**
     * Gets the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {

        return databaseName;
    }

    /**
     * Gets the table metadata.
     *
     * @return the table metadata
     */
    public TableMetadata getTableMetadata() {

        return tableMetadata;
    }

}
