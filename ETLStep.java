package com.am.analytics.common.core.etl;

import com.am.analytics.common.core.exception.WorkflowContinueProcessException;
import com.am.analytics.common.core.exception.WorkflowHaltException;

/**
 * The Interface ETLStep: Interface for processing the ETL step.
 *
 * @param <I> the generic type
 * @param <O> the generic type
 */
public interface ETLStep<I, O> {

    /**
     * This method process the input data corresponding to batchId and return output data.
     *
     * @param input the input
     * @param batchId the batch id
     * @return the o
     * @throws WorkflowHaltException the workflow halt exception
     * @throws WorkflowContinueProcessException the workflow continue process exception
     */
    public O process(I input, long batchId) throws WorkflowHaltException, WorkflowContinueProcessException;
}
