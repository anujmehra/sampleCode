package com.am.analytics.common.core.etl.impl;

import java.util.ArrayList;
import java.util.List;

import com.am.analytics.common.core.etl.DataExtractor;
import com.am.analytics.common.core.etl.ETLStep;
import com.am.analytics.common.core.etl.ETLWorkflow;
import com.am.analytics.common.core.etl.LoaderStep;
import com.am.analytics.common.core.etl.TransformationStep;
import com.am.analytics.common.core.exception.DataLoadingContinueException;
import com.am.analytics.common.core.exception.DataLoadingHaltException;
import com.am.analytics.common.core.exception.ETLWorkflowExecutionException;
import com.am.analytics.common.core.exception.ETLWorkflowExecutionException.ETLWorkflowExecutionExceptionBuilder;
import com.am.analytics.common.core.exception.WorkflowHaltException;
import com.am.analytics.common.core.utils.Logger;

/**
 * The Class BasicETLWorkflow: Implementation class for ETLWorkflow.
 */
public class BasicETLWorkflow implements ETLWorkflow {

    /** The input source. */
    private final DataExtractor<?> inputSource;

    /** The steps. */
    private final List<ETLStep<?, ?>> steps;

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(BasicETLWorkflow.class);

    /**
     * Instantiates a new basic etl workflow.
     *
     * @param inputSource the input source
     */
    private BasicETLWorkflow(final DataExtractor<?> inputSource) {

        this.inputSource = inputSource;
        this.steps = new ArrayList<>();
    }

    /** The batch id. */
    private long batchId;

    /* (non-Javadoc)
     * @see com.am.analytics.common.core.etl.ETLWorkflow#execute()
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public final void execute() throws ETLWorkflowExecutionException {

        Object output = null;
        if (null != inputSource) {
            try {
                output = inputSource.extract(batchId);
            } catch (WorkflowHaltException e) {
                throw ETLWorkflowExecutionExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
            }
        }

        for (final ETLStep etlStep : steps) {
            if (etlStep instanceof TransformationStep<?, ?>) {
                try {
                    final TransformationStep transforamtionStep = (TransformationStep<?, ?>) etlStep;
                    output = transforamtionStep.process(output, batchId);
                } catch (WorkflowHaltException e) {
                    throw ETLWorkflowExecutionExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
                }
            } else if (etlStep instanceof LoaderStep<?>) {
                try {
                    final LoaderStep loaderStep = (LoaderStep<?>) etlStep;
                    loaderStep.process(output, batchId);
                } catch (DataLoadingHaltException e) {
                    throw ETLWorkflowExecutionExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
                } catch (DataLoadingContinueException e) {
                    LOGGER.error(e, "Exception occurred while executing the workflow for batchID " + batchId);
                }
            }
        }

    }

    /**
     * The Class ETLWorkflowBuilder.
     */
    public static class ETLWorkflowBuilder {

        /** The workflow. */
        private final BasicETLWorkflow workflow;

        /**
         * Instantiates a new ETL workflow builder.
         *
         * @param input input
         * @param batchId batchId
         */
        public ETLWorkflowBuilder(final DataExtractor<?> input, final long batchId) {

            this.workflow = new BasicETLWorkflow(input);
            this.workflow.batchId = batchId;
        }

        /**
         * Next step.
         *
         * @param step step
         * @return the ETL workflow builder
         */
        public ETLWorkflowBuilder nextStep(final ETLStep<?, ?> step) {

            this.workflow.steps.add(step);
            return this;
        }

        /**
         * Builds the.
         *
         * @return the basic etl workflow
         */
        public BasicETLWorkflow build() {

            return workflow;
        }
    }

}
