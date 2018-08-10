package com.am.analytics.job.data.cleaning.service.impl;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.am.analytics.common.core.constant.BeanConstants;
import com.am.analytics.common.core.etl.ETLWorkflow;
import com.am.analytics.common.core.etl.LoaderStep;
import com.am.analytics.common.core.etl.TransformationStep;
import com.am.analytics.common.core.etl.impl.BasicETLWorkflow;
import com.am.analytics.common.core.exception.ETLWorkflowExecutionException;
import com.am.analytics.common.core.exception.WorkflowHaltException;
import com.am.analytics.common.core.utils.Logger;
import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.job.common.exception.JobExecutionException;
import com.am.analytics.job.data.cleaning.etl.extractor.CleaningJobDataExtractor;
import com.am.analytics.job.data.cleaning.service.CleaningJobService;

/**
 * Concrete implementation of  CleaningJobService for Sabre input data. This class is responsible to create and execute the
 *  ETLWorkflow} for Sabre cleeaning job.
 * 
 * @author am
 */
@Component(BeanConstants.SABRE_DATA_CLEANING_JOB_SERVICE_BEAN)
public class SabreCleaningJobService implements CleaningJobService {

    /**
     * Reference for class  SabreCleaningJobDataExtractor reads the .parquet file saved
     * by the Mapping job.
     */
    @Autowired
    @Qualifier("sabreCleaningJobDataExtractor")
    private CleaningJobDataExtractor sabreCleaningJobDataExtractor;

    /**
     * Reference for class  SabreColumnRemoverTransformer accept Raw Parquet file data
     * in the  Dataset} format. Then remove the unwanted columns and return the data in the  Dataset} format.
     */
    @Autowired
    @Qualifier("sabreColumnRemoverTransformer")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreColumnRemoverTransformer;

    /** The sabre row remover transformer. */
    @Autowired
    @Qualifier("sabreRowRemoverTransformer")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreRowRemoverTransformer;

    /** The res passenger doc cleaner. */
    @Autowired
    @Qualifier(BeanConstants.RESPASSENGERDOC_CLEANER_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resPassengerDocCleaner;

    /** The res passenger FT cleaner. */
    @Autowired
    @Qualifier(BeanConstants.RESPASSENGERFT_CLEANER_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resPassengerFTCleaner;
    
    /** The res passenger phone cleaner. */
    @Autowired
    @Qualifier(BeanConstants.RESPASSENGERPHONE_CLEANER_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resPassengerPhoneCleaner;

    /** The res passenger email clean transformer. */
    @Autowired
    @Qualifier(BeanConstants.RESPASSENGER_EMAIL_CLEANER_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resPassengerEmailCleanTransformer;
    
    /** The res suspense primary doc generator. */
    @Autowired
    @Qualifier(BeanConstants.RESSUSPENSE_PRIMARYDOC_GENERATOR_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resSuspensePrimaryDocGenerator;

    /** The res ssr passenger seq id zero cleaner. */
    @Autowired
    @Qualifier(BeanConstants.RESSSR_PASSENGERSEQID_CLEANER_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resSsrPassengerSeqIdZeroCleaner;

    /** The res ssr normalization. */
    @Autowired
    @Qualifier(BeanConstants.RESSSR_NORMALIZATION_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resSsrNormalization;

    /**
     * Reference for class  SabreTablesTitleRemoveFactory provides the factory
     * implementation for all TitleRemoverTransformers for Sabre data.
     */
    @Autowired
    @Qualifier("sabreTablesTitleRemoveFactory")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreTablesTitleRemoveFactory;

    /**
     * Reference for class  SabreTablesGenderRemoveFactory provides the factory
     * implementation for all TitleRemoverTransformers for Sabre data.
     */
    @Autowired
    @Qualifier("sabreTablesGenderRemoveFactory")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreTablesGenderRemoveFactory;
    
    /**
     * Reference for class  SabreTablesFirstNameRemoveFactory provides the factory
     * implementation for all FirstNameCleanerTransformers for Sabre data.
     */
    @Autowired
    @Qualifier("sabreTablesFirstNameRemoveFactory")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreTablesFirstNameRemoveFactory;
    
    /**
     * Reference for class  SabreTableNormalization This class is responsible to normalize all the Sabre Tables.
     */
    @Autowired
    @Qualifier("sabreTableNormalization")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> sabreTableNormalization;

    /**
     * Reference for class  SabreDataCleaningJobLoader This class is responsible to save the Parquet files generated by the
     * Sabre Cleaning job in the respective target directories.
     */
    @Autowired
    @Qualifier("sabreDataCleaningJobLoader")
    private LoaderStep<Map<String, Dataset<Row>>> sabreDataCleaningJobLoader;

    /** The acs pax flight nbr FT cleaner. */
    @Autowired
    @Qualifier("acsPaxFlightNbrFTCleaner")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> acsPaxFlightNbrFTCleaner;
    
    /** The tkt document currency converter. */
    @Autowired
    @Qualifier("tktDocumentCurrencyConverter")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> tktDocumentCurrencyConverter;

    /** The res passenger phone normalization. */
    @Autowired
    @Qualifier(BeanConstants.RESPASSENGERPHONE_NORMALIZATION_BEAN)
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> resPassengerPhoneNormalization;

    /** The acs pax docx cleaner. */
    @Autowired
    @Qualifier("acsPaxDocxCleaner")
    private TransformationStep<Map<String, Dataset<Row>>, Map<String, Dataset<Row>>> acsPaxDocxCleaner;
    
    /**
     * Logger reference.
     */
    private static final Logger LOGGER = Logger.getLogger(SabreCleaningJobService.class);

    
    /**
     * This method will perform following steps on each sabre file: 
     * 1. Load input parquet file in the 'Dataset' format 
     * 2. Remove unwanted columns from the dataset 
     * 3. Format all date columns to a same, consistent date format 
     * 4. Remove titles from name columns 
     * 5. Clean data from required columns
     *
     * @param batchId Long
     * @throws WorkflowHaltException the workflow halt exception
     * @throws JobExecutionException the job execution exception
     */
    @Override
    public void execute(final long batchId) throws WorkflowHaltException, JobExecutionException {

        try {
            final ETLWorkflow workflow = new BasicETLWorkflow.ETLWorkflowBuilder(sabreCleaningJobDataExtractor, batchId)
                .nextStep(sabreColumnRemoverTransformer).nextStep(sabreRowRemoverTransformer).nextStep(resPassengerDocCleaner)
                .nextStep(resPassengerFTCleaner)
                .nextStep(resPassengerPhoneCleaner).nextStep(resSuspensePrimaryDocGenerator)
                .nextStep(resPassengerEmailCleanTransformer)
                .nextStep(acsPaxDocxCleaner)
                .nextStep(sabreTablesGenderRemoveFactory)
                // .nextStep(sabreSpecialCharRemoverTransformer)
                .nextStep(sabreTablesTitleRemoveFactory).nextStep(resSsrPassengerSeqIdZeroCleaner).nextStep(resSsrNormalization)
                .nextStep(sabreTableNormalization).nextStep(resPassengerPhoneNormalization).nextStep(tktDocumentCurrencyConverter)
                .nextStep(acsPaxFlightNbrFTCleaner)
                .nextStep(sabreTablesFirstNameRemoveFactory)
                .nextStep(sabreDataCleaningJobLoader).build();

            workflow.execute();
        } catch (final ETLWorkflowExecutionException e) {
            LOGGER.error(e,
                StringUtils.concatenate("ETLWorkflowExecutionException occurred while executing cleaning workflow for file "));
        }
    }

}