package com.am.analytics.common.core.etl;

import com.am.analytics.common.core.exception.DataTransformationException;

/**
 * The Interface TransformationStep: Interface for transforming the data in ETLWorkflow.
 *
 * @param <I> the generic type
 * @param <O> the generic type
 */
public interface TransformationStep<I, O> extends ETLStep<I, O> {

    /**
     * This method process the input data corresponding to batchId and returns the output data.
     */
    @Override
    public O process(I input, long batchId) throws DataTransformationException;

}
