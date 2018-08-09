package com.am.analytics.job.data.cleaning.etl.extractor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import com.am.analytics.common.core.constant.JobType;
import com.am.analytics.common.core.etl.DataExtractor;
import com.am.analytics.common.core.exception.DataExtractionException;
import com.am.analytics.common.core.exception.DataExtractionException.DataExtractionExceptionBuilder;
import com.am.analytics.common.core.utils.CollectionUtils;
import com.am.analytics.common.core.utils.Logger;
import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.common.service.hdfs.am;
import com.am.analytics.job.data.cleaning.exception.DataCleaningJobException;
import com.am.analytics.job.data.cleaning.exception.DataCleaningJobException.DataCleaninJobExceptionBuilder;
import com.am.analytics.job.dataaccess.dao.FileReaderService;
import com.am.analytics.job.dataaccess.dao.impl.FileReaderServiceImpl;
import com.am.analytics.job.dataaccess.exception.FileReaderException;
import com.am.analytics.job.service.staging.JobsStagingService;
import com.am.analytics.job.service.staging.impl.JobsStagingServiceImpl;

/**
 * Abstract implementation of the DataExtractor layer for the Cleaning and Normalization job. This class is responsible to read all
 * Parquet files (Created by Mapping job) present in a staging folder and generate a Map having table name as key and
 * respective Parquet file content as value in the Dataset format.
 * 
 * @author am
 */
public abstract class CleaningJobDataExtractor implements DataExtractor<Map<String, Dataset<Row>>> {

    /** Reference for class {@link JobsStagingServiceImpl}. */
    @Autowired
    private JobsStagingService stagingService;

    /** Reference for class {@link FileReaderServiceImpl}. */
    @Autowired
    private FileReaderService fileReaderService;

    /** Reference for class {@link am}. */
    @Autowired
    private am am;

    /**
     * Abstract method to be implemented by all concrete class implementations. This method to return the current JobType being
     * executed.
     * 
     * @return jobType {@link JobType}
     */
    public abstract JobType getCurrentJobType();

    /**
     * Logger reference.
     */
    private static final Logger LOGGER = Logger.getLogger(CleaningJobDataExtractor.class);

    /**
     * This method is responsible to read all Parquet files (Created by Mapping job) present in a staging folder and generate a
     * Map having table name as key and respective Parquet file content as value in the Dataset format.
     *
     * @param batchId batchId
     * @return the map
     * @throws DataExtractionException the data extraction exception
     */
    @Override
    public Map<String, Dataset<Row>> extract(final long batchId) throws DataExtractionException {

        final Map<String, Dataset<Row>> fileDatasetMap = new HashMap<>();
        try {
            final String stagingDirectory = stagingService.getTargetDirectory(batchId, this.getCurrentJobType().getPreviousJob());
            final List<String> fileNames = this.fetchFileNames(stagingDirectory);
            if (!CollectionUtils.isEmpty(fileNames)) {
                for (final String fileName : fileNames) {
                        final Dataset<Row> dataset = getDataset(stagingDirectory, fileName);
                        fileDatasetMap.put(fileName.replace(".parquet", ""), dataset);
                }
            }
        } catch (final FileReaderException | DataCleaningJobException e) {
            throw DataExtractionExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
        }

        return fileDatasetMap;
    }

	/**
	 * Use to get actual dataset for specific file.
	 *
	 * @param stagingDirectory - location where file exist
	 * @param fileName - name of the file
	 * @return the dataset
	 * @throws FileReaderException the file reader exception
	 */
	protected Dataset<Row> getDataset(final String stagingDirectory, final String fileName) throws FileReaderException {
		return fileReaderService.read(stagingDirectory + "/" + fileName, null, null);
	}

    /**
     * Method to fetch the names of all the files present in a directory on HDFS.
     *
     * @param stagingDirectory {@link String} Fully qualified path of folder in which input files are placed
     * @return list {@link List} Names of files present in the stagingDirectory
     * @throws DataCleaningJobException the data cleaning job exception
     */
    private final List<String> fetchFileNames(final String stagingDirectory) throws DataCleaningJobException {

        List<String> fileNames = null;
        if (!StringUtils.isBlank(stagingDirectory)) {
            try {
                fileNames = am.getParquetFileNames(stagingDirectory);
            } catch (final IOException e) {
                LOGGER.error("IOException occurred while cleaning data for dataDirectory " + stagingDirectory);
                throw DataCleaninJobExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
            }
        }
        return fileNames;
    }
}
