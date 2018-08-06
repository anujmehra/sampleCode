package com.am.analytics.common.core.etl;

import com.am.analytics.common.core.exception.DataExtractionException;

/**
 * The Interface DataExtractor: Interface for extracting the data.
 *
 * @param <O> the generic type
 */
public interface DataExtractor<O> {

    /**
     * This method extracts the data corresponding to batchId.
     *
     * @param batchId the batch id
     * @return the o
     * @throws DataExtractionException the data extraction exception
     */
    public O extract(long batchId) throws DataExtractionException;
}
