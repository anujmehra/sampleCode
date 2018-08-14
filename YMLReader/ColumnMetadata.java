package com.am.analytics.job.model;


import com.am.analytics.common.core.utils.DataType;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Class ColumnMetadata have metadata of column.
 */
public class ColumnMetadata {

	/** The name. */
	@JsonProperty("name")
	private String name;

	/** The type. */
	@JsonProperty("type")
	private String type;

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public DataType getType() {
		return DataType.valueOf(type.toUpperCase());
	}
}
