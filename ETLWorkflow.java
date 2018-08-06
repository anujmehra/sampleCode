package com.am.analytics.common.core.etl;

import com.am.analytics.common.core.exception.ETLWorkflowExecutionException;

/**
 * The Interface ETLWorkflow: Interface for executing ETLWorkflow.
 */
public interface ETLWorkflow {

    /**
     * This method execute the workflow.
     *
     * @throws ETLWorkflowExecutionException the ETL workflow execution exception
     */
    public void execute() throws ETLWorkflowExecutionException;
}
