package com.am.analytics.common.core.etl;

import com.am.analytics.common.core.exception.DataLoadingContinueException;
import com.am.analytics.common.core.exception.DataLoadingHaltException;

/**
 * The Interface LoaderStep: Interface for Loading the data in ETLWorkflow.
 *
 * @param <I> the generic type
 */
public interface LoaderStep<I> extends ETLStep<I, Void> {

    /**
     * This method process the input data corresponding to batchId.
     */
    @Override
    public Void process(I input, long batchId) throws DataLoadingHaltException, DataLoadingContinueException;

}
