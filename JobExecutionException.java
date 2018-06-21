package com.am.analytics.job.common.exception;

import com.am.analytics.common.core.exception.DataTransformationException.DataTransformationExceptionBuilder;

/**
 * The Class JobExecutionException.
 */
public class JobExecutionException extends Exception {

	/**
	 * Serialized version UID of the class.
	 */
	private static final long serialVersionUID = 1L;
	
	 /** The error code. */
    private final String errorCode;

    /**
     * Instantiates a new job execution exception.
     *
     * @param builder the builder
     */
    private JobExecutionException(final JobExecutionExceptionBuilder builder) {
        super(builder.errorMessage, builder.throwable);
        this.errorCode = builder.errorCode;
    }

    /**
     * Gets the error code.
     *
     * @return the error code
     */
    public String getErrorCode() {

        return this.errorCode;
    }

    /**
     * The Class JobExecutionExceptionBuilder.
     */
    public static class JobExecutionExceptionBuilder {

        /** The error code. */
        private String errorCode;

        /** The error message. */
        private String errorMessage;

        /** The throwable. */
        private Throwable throwable;

        /**
         * Error code.
         *
         * @param errorCode the error code
         * @return the job execution exception builder
         */
        public JobExecutionExceptionBuilder errorCode(final String errorCode) {

            this.errorCode = errorCode;
            return this;
        }

        /**
         * Error message.
         *
         * @param errorMessage the error message
         * @return the job execution exception builder
         */
        public JobExecutionExceptionBuilder errorMessage(final String errorMessage) {

            this.errorMessage = errorMessage;
            return this;
        }

        /**
         * Throwable.
         *
         * @param throwable the throwable
         * @return the job execution exception builder
         */
        public JobExecutionExceptionBuilder throwable(final Throwable throwable) {

            this.throwable = throwable;
            return this;
        }

        /**
         * Creates the.
         *
         * @return the job execution exception builder
         */
        public static DataTransformationExceptionBuilder create() {

            return new DataTransformationExceptionBuilder();
        }

        /**
         * Builds the.
         *
         * @return the job execution exception
         */
        public JobExecutionException build() {

            return new JobExecutionException(this);
        }
    }

}
