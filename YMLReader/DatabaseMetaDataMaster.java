package com.am.analytics.job.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.am.analytics.job.model.DatabaseMetadata;

/**
 * The Class DatabaseMetaDataMaster.
 */
public class DatabaseMetaDataMaster {

	/** The database metadata. */
	@JsonProperty("database_metadata")
	private DatabaseMetadata databaseMetadata;
	
	/**
	 * Gets the database metadata.
	 *
	 * @return the database metadata
	 */
	public DatabaseMetadata getDatabaseMetadata() {
        return databaseMetadata;
    }

}
