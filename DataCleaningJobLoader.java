package com.am.analytics.job.data.cleaning.etl.loader;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import com.am.analytics.common.core.constant.CommonConstant;
import com.am.analytics.common.core.constant.FileFormat;
import com.am.analytics.common.core.constant.JobType;
import com.am.analytics.common.core.etl.LoaderStep;
import com.am.analytics.common.core.exception.DataLoadingContinueException;
import com.am.analytics.common.core.exception.DataLoadingHaltException;
import com.am.analytics.common.core.utils.Logger;
import com.am.analytics.job.dataaccess.dao.FilePersistance;
import com.am.analytics.job.dataaccess.exception.FilePersistenceException;
import com.am.analytics.job.service.staging.JobsStagingService;

/**
 * Abstract implementation of the LoaderStep for the Cleaning and Normalization job. This class is responsible to save the Parquet
 * files generated by the Cleaning job in the respective target directories.
 * 
 * @author am
 */
public abstract class DataCleaningJobLoader implements LoaderStep<Map<String, Dataset<Row>>> {

    /** Reference for JobsStagingService. */
    @Autowired
    private JobsStagingService jobsStagingService;

    /** Reference for FilePersistance. */
    @Autowired
    private FilePersistance<Dataset<Row>> parquetFilePersistance;

    /** Logger reference. */
    private static final Logger logger = Logger.getLogger(DataCleaningJobLoader.class);

    /**
     * Abstract method to get the Current JobType.
     *
     * @return jobType JobType
     */
    public abstract JobType getCurrentJobType();

    /**
     * Process.
     *
     * @param input the input
     * @param batchId the batch id
     * @return the void
     * @throws DataLoadingHaltException the data loading halt exception
     * @throws DataLoadingContinueException the data loading continue exception
     */
    @Override
    public Void process(final Map<String, Dataset<Row>> input, final long batchId) throws DataLoadingHaltException,
                                                                                   DataLoadingContinueException {

        final String targetDirectory = this.getTargetDirectory(batchId, this.getCurrentJobType());
        for (final Entry<String, Dataset<Row>> entry : input.entrySet()) {
            try {
                parquetFilePersistance.saveFile(
                    targetDirectory + CommonConstant.FORWARD_SLASH + entry.getKey() + FileFormat.PARQUET.getExtension(), entry.getValue());
            } catch (final FilePersistenceException e) {
                logger.error(e, "FilePersistenceException Exception Occured : ");
            }
        }
        return null;
    }

    /**
     * Method is used to fetch the Target directory for the Cleaning and Normalization job. In this target directory resultant
     * Parquet files (generated by Cleaning and Normalization job) will be saved.
     * 
     * @param batchId {@link Long} - batch id of the job being executed
     * @param jobType {@link JobType} - current job type
     * @return targetDirecotry {@link String} - target directory in which the resultant Parquet files (generated by Cleaning and
     *         Normalization job) will be saved.
     */
    private String getTargetDirectory(final long batchId, final JobType jobType) {

        return jobsStagingService.getTargetDirectory(batchId, jobType);
    }
}