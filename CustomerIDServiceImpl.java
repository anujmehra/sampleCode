package com.am.analytics.job.service.db.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.am.analytics.common.core.constant.BeanConstants;
import com.am.analytics.common.core.utils.ArrayUtils;
import com.am.analytics.common.core.utils.DigestUtils;
import com.am.analytics.common.core.utils.Pair;
import com.am.analytics.job.common.constant.CustomerIDConstants;
import com.am.analytics.job.common.constant.HBaseTargetTableConstant;
import com.am.analytics.job.common.constant.JobConstants;
import com.am.analytics.job.common.constant.SabreColumnConstant;
import com.am.analytics.job.common.model.CustomerIDAttributes;
import com.am.analytics.job.common.model.CustomerIDAttributesList;
import com.am.analytics.job.common.model.CustomerIDsDOBModel;
import com.am.analytics.job.common.model.PNRPassenger;
import com.am.analytics.job.common.utils.Bytes;
import com.am.analytics.job.common.utils.RowUtil;
import com.am.analytics.job.common.utils.TableFieldMappingUtil;
import com.am.analytics.job.dataaccess.dao.DatasetDao;
import com.am.analytics.job.dataaccess.dao.JobsDao;
import com.am.analytics.job.dataaccess.exception.JobDaoException;
import com.am.analytics.job.model.HBaseColumnField;
import com.am.analytics.job.model.TableFieldMapping;
import com.am.analytics.job.service.db.CustomerIDService;
import com.am.analytics.job.service.db.MasterSchemaService;
import com.am.analytics.job.service.exception.CustomerIDException;
import com.am.analytics.job.service.exception.CustomerIDException.CustomerIDExceptionBuilder;
import com.am.analytics.job.service.exception.MasterSchemaAccessException;
import com.am.analytics.job.service.model.CustomerIDHistoryModel;

import scala.Tuple2;

/**
 * Default implementation of the {@link CustomerIDService} which use HBase as the underlying repository to map the records.
 */
@Service
public final class CustomerIDServiceImpl implements CustomerIDService {

    /**
     * Serialized vesion UID of the class.
     */
    private static final long serialVersionUID = 1L;

    /** The jobs dao. */
    @Autowired
    @Qualifier(BeanConstants.RECORD_DAO_BEAN)
    private JobsDao<Result, Put> jobsDao;

    /** The schema service. */
    @Autowired
    private MasterSchemaService schemaService;

    /** The dao. */
    @Autowired
    private DatasetDao<HBaseColumnField> dao;

    /** The Constant CUSTOMER_ID_INDEX_COLUMN. */
    private static final String CUSTOMER_ID_INDEX_COLUMN = "model";

    /** The Constant CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME. */
    private static final String CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME = "pnr_passengers";

    /** The Constant CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME. */
    private static final String CUSTOMER_ID_COLUMN_NAME = "customer_ids";

    /** The Constant PNR_LOCATOR_ID_COLUMN. */
    private static final String PNR_LOCATOR_ID_COLUMN = "pnrlocatorid";

    /** The Constant PNR_CREATE_DATE_COLUMN. */
    private static final String PNR_CREATE_DATE_COLUMN = "pnrcreatedate";

    /** The Constant PNR_PASSENGER_SEQ_ID_COLUMN. */
    private static final String PNR_PASSENGER_SEQ_ID_COLUMN = "pnrpassengerseqid";

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getCustomerIDHistory(java.lang.String)
     */
    @Override
    public CustomerIDHistoryModel getCustomerIDHistory(final String key) throws CustomerIDException {

        CustomerIDHistoryModel history = null;
        try {
            final Result result = jobsDao.getDataRecord(
                HBaseTargetTableConstant.CUSTOMER_ID_SECONDARY_INDEX_TABLE.getHbaseTableName(),
                HBaseTargetTableConstant.CUSTOMER_ID_SECONDARY_INDEX_TABLE.getColumnFamily(), key, false);
            if (null != result) {
                final byte[] value = result.getValue(
                    Bytes.toBytes(HBaseTargetTableConstant.CUSTOMER_ID_SECONDARY_INDEX_TABLE.getColumnFamily()),
                    Bytes.toBytes(CUSTOMER_ID_INDEX_COLUMN));
                if (!ArrayUtils.isEmpty(value)) {
                    history = Bytes.toObject(value);
                }
            }
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
        }
        return history;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#saveCustomerIDHistory(java.lang.String,
     * com.am.analytics.job.service.model.CustomerIDHistoryModel)
     */
    @Override
    public void saveCustomerIDHistory(final String key, final CustomerIDHistoryModel model) throws CustomerIDException {

        final byte[] modelBytes = Bytes.toBytes(model);
        final Put put = new Put(Bytes.toBytes(key));
        put.addColumn(Bytes.toBytes(HBaseTargetTableConstant.CUSTOMER_ID_SECONDARY_INDEX_TABLE.getColumnFamily()),
            Bytes.toBytes(CUSTOMER_ID_INDEX_COLUMN), modelBytes);
        try {
            jobsDao.save(HBaseTargetTableConstant.CUSTOMER_ID_SECONDARY_INDEX_TABLE.getHbaseTableName(), put);
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getPNRPassengersByCustomerIDs(org.apache.spark.sql.Dataset)
     */
    @Override
    public JavaPairRDD<String, CustomerIDAttributesList> getPNRPassengersByCustomerIDs(final Dataset<String> customerIDs) throws CustomerIDException {

        try {
            return dao.getObjectsPair(customerIDs,
                HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName(),
                new HBaseColumnField(CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME,
                    HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getColumnFamily()),
                CustomerIDAttributesList.class);
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().errorMessage("Error occured while fetching PNR passengers for customer ID")
                .throwable(e).build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getCustomerIDsByDOB(org.apache.spark.sql.Dataset)
     */
    public JavaPairRDD<String, CustomerIDsDOBModel> getCustomerIDsByDOB(final Dataset<String> dobDataset) throws CustomerIDException {

        try {
            return dao
                .getObjectsPair(dobDataset, HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getHbaseTableName(),
                    new HBaseColumnField(CUSTOMER_ID_COLUMN_NAME,
                        HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getColumnFamily()),
                    CustomerIDsDOBModel.class);
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().errorMessage("Error occured while fetching PNR passengers for customer ID")
                .throwable(e).build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#save(org.apache.spark.sql.Dataset)
     */
    @Override
    public void save(final Dataset<Row> input) throws CustomerIDException {

        if (null != input) {
            try {
                input.persist(StorageLevel.DISK_ONLY_2());
                saveSecondaryIndex(input);
                saveRecords(input);

            } catch (final JobDaoException e) {
                throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
            }
        }
    }

    /**
     * Save records.
     *
     * @param input the input
     * @throws JobDaoException the job dao exception
     */
    private void saveRecords(final Dataset<Row> input) throws JobDaoException {

        final StructType schema = input.schema()
            .add(new StructField(SabreColumnConstant.PNR_PASSENGER_KEY, DataTypes.StringType, true, Metadata.empty()));
        final Dataset<Row> finalReservationRows = input.map(row -> addPnrPassengerKey(row, schema), RowEncoder.apply(schema));
        final TableFieldMapping<HBaseColumnField> mapping = TableFieldMappingUtil.getTableFieldMapping(schema,
            HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName(), SabreColumnConstant.PNR_PASSENGER_KEY,
            HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getColumnFamily());
        dao.save(finalReservationRows, mapping, false);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#update(org.apache.spark.sql.Dataset)
     */
    @Override
    public void update(final Dataset<Row> input) throws CustomerIDException {

        if (null != input) {
            try {
                input.persist(StorageLevel.DISK_ONLY_2());
                saveRecords(input);
                updateSecondaryIndex(input);

            } catch (final JobDaoException e) {
                throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
            }
        }
    }

    /**
     * Update secondary index.
     *
     * @param input the input
     * @throws JobDaoException the job dao exception
     * @throws CustomerIDException the customer ID exception
     */
    private void updateSecondaryIndex(final Dataset<Row> input) throws JobDaoException, CustomerIDException {

        updateCustomerIDPNRIndex(input);
        updateCustomerIDDOBIndex(input);
    }

    /**
     * Update customer IDDOB index.
     *
     * @param input the input
     * @throws CustomerIDException the customer ID exception
     * @throws JobDaoException the job dao exception
     */
    private void updateCustomerIDDOBIndex(final Dataset<Row> input) throws CustomerIDException, JobDaoException {

        final Dataset<String> dobDataset = input.filter(row -> row.getAs(CustomerIDConstants.DOB_COLUMN) != null)
            .map(row -> row.getAs(CustomerIDConstants.DOB_COLUMN).toString(), Encoders.STRING()).distinct();
        JavaPairRDD<String, CustomerIDsDOBModel> customerIDsByDOB = this.getCustomerIDsByDOB(dobDataset);
        customerIDsByDOB = customerIDsByDOB.filter(t -> t._2 != null);

        final JavaPairRDD<String, CustomerIDsDOBModel> pairRDD = input.toJavaRDD()
            .filter(row -> row.getAs(CustomerIDConstants.DOB_COLUMN) != null).mapToPair(row -> {
                final Date dob = row.getAs(CustomerIDConstants.DOB_COLUMN);
                final String dobHash = DigestUtils.generateHash(dob.toString());
                final List<String> list = new ArrayList<>();
                list.add(row.getAs(JobConstants.CUSTOMER_ID_COLUMN));
                final CustomerIDsDOBModel customers = new CustomerIDsDOBModel(dob, list);
                return new Tuple2<>(dobHash, customers);
            }).reduceByKey((p1, p2) -> {
                p1.getCustomerIDs().addAll(p2.getCustomerIDs());
                return p1;
            });

        final JavaPairRDD<String, Tuple2<CustomerIDsDOBModel, Optional<CustomerIDsDOBModel>>> leftOuterJoin = pairRDD
            .leftOuterJoin(customerIDsByDOB);
        final JavaPairRDD<String, CustomerIDsDOBModel> mapToPair = leftOuterJoin.mapToPair(tuple -> {

            final CustomerIDsDOBModel list = tuple._2._1;
            if (tuple._2._2.isPresent()) {
                final CustomerIDsDOBModel list1 = tuple._2._2.get();
                list.getCustomerIDs().addAll(list1.getCustomerIDs());
            }
            return new Tuple2<>(list.getDocBirthdate().toString(), list);
        });

        dao.save(mapToPair, HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getHbaseTableName(),
            new HBaseColumnField(null, HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getColumnFamily(),
                CUSTOMER_ID_COLUMN_NAME));
    }

    /**
     * Update customer IDPNR index.
     *
     * @param input the input
     * @throws CustomerIDException the customer ID exception
     * @throws JobDaoException the job dao exception
     */
    private void updateCustomerIDPNRIndex(final Dataset<Row> input) throws CustomerIDException, JobDaoException {

        final Dataset<String> customerIDs = input.map(row -> row.getAs(JobConstants.CUSTOMER_ID_COLUMN), Encoders.STRING())
            .distinct();
        JavaPairRDD<String, CustomerIDAttributesList> pnrPassengersByCustomerIDs = this.getPNRPassengersByCustomerIDs(customerIDs);

        pnrPassengersByCustomerIDs = pnrPassengersByCustomerIDs.filter(t -> t._2 != null);

        final JavaPairRDD<String, CustomerIDAttributesList> pairRDD = input.toJavaRDD().mapToPair(row -> {
            final String customerID = row.getAs(JobConstants.CUSTOMER_ID_COLUMN);
            
            final String customerIDHash = DigestUtils.generateHash(customerID);
           
            final List<CustomerIDAttributes> list = new ArrayList<>();
            list.add(new CustomerIDAttributes(customerID, getPNRPassenger(row)));
            final CustomerIDAttributesList customers = new CustomerIDAttributesList(list);
            return new Tuple2<>(customerIDHash, customers);
        }).reduceByKey((p1, p2) -> {
            p1.getCustomersList().addAll(p2.getCustomersList());
            return p1;
        });

        final JavaPairRDD<String, Tuple2<CustomerIDAttributesList, Optional<CustomerIDAttributesList>>> leftOuterJoin = pairRDD
            .leftOuterJoin(pnrPassengersByCustomerIDs);
        final JavaPairRDD<String, CustomerIDAttributesList> mapToPair = leftOuterJoin.mapToPair(tuple -> {

            final CustomerIDAttributesList list = tuple._2._1;
            if (tuple._2._2.isPresent()) {
                final CustomerIDAttributesList list1 = tuple._2._2.get();
                list.getCustomersList().addAll(list1.getCustomersList());
            }
            return new Tuple2<>(list.getCustomersList().get(0).getCustomerID(), list);
        });

        dao.save(mapToPair, HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName(),
            new HBaseColumnField(null, HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getColumnFamily(),
                CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME));
    }

    /**
     * Method to generate PNRPassenger key while saving reservation master data into database.
     *
     * @param row the row
     * @param finalSchema the final schema
     * @return the row
     */
    private static Row addPnrPassengerKey(final Row row, final StructType finalSchema) {

        final PNRPassenger pnrPassenger = getPNRPassenger(row);
        return pnrPassenger == null ? null : RowUtil.addValues(row, finalSchema, pnrPassenger.getKey());
    }

    /**
     * Method to get PNRPassenger for a Reservation Master row.
     *
     * @param row the row
     * @return the PNR passenger
     */
    private static PNRPassenger getPNRPassenger(final Row row) {

        PNRPassenger pnrPassenger = null;
        if (null != row) {
            final String locatorID = row.getAs(PNR_LOCATOR_ID_COLUMN);
            final Date createDate = row.getAs(PNR_CREATE_DATE_COLUMN);
            final Integer sequenceID = row.getAs(PNR_PASSENGER_SEQ_ID_COLUMN);
            pnrPassenger = new PNRPassenger(locatorID, sequenceID, createDate);
        }
        return pnrPassenger;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getCustomerIDs(org.apache.spark.api.java.JavaPairRDD)
     */
    @Override
    public JavaPairRDD<PNRPassenger, String> getCustomerIDs(final JavaPairRDD<String, PNRPassenger> pnrPassengerKeyPair) throws CustomerIDException {

        JavaPairRDD<PNRPassenger, String> pnrPassengerCustomerIDPair = null;
        if (null != pnrPassengerKeyPair) {
            final JavaPairRDD<String, Pair<String, PNRPassenger>> mappedObjectsPair = dao.getMappedObjectsPair(pnrPassengerKeyPair,
                HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName(), new HBaseColumnField(
                    JobConstants.CUSTOMER_ID_COLUMN, HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getColumnFamily()));
            pnrPassengerCustomerIDPair = mappedObjectsPair.mapToPair(tuple -> new Tuple2<>(tuple._2.getValue(), tuple._2.getKey()));
        }
        return pnrPassengerCustomerIDPair;
    }

    /**
     * Save secondary index.
     *
     * @param input the input
     * @throws JobDaoException the job dao exception
     */
    private void saveSecondaryIndex(final Dataset<Row> input) throws JobDaoException {

        // saveCustomerIDDOBIndex(input);
        saveCustomerIDPNRIndex(input);
    }

    /**
     * Save customer IDPNR index.
     *
     * @param input the input
     * @throws JobDaoException the job dao exception
     */
    private void saveCustomerIDPNRIndex(final Dataset<Row> input) throws JobDaoException {

        jobsDao.deleteTable(HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName());
        jobsDao.create(HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName(),
            HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getSplits(),
            HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getColumnFamily());
        final JavaPairRDD<String, CustomerIDAttributesList> pairRDD = input.toJavaRDD().mapToPair(row -> {
            final String customerID = row.getAs(JobConstants.CUSTOMER_ID_COLUMN);
            final List<CustomerIDAttributes> list = new ArrayList<>();
            list.add(new CustomerIDAttributes(customerID, getPNRPassenger(row)));
            final CustomerIDAttributesList customers = new CustomerIDAttributesList(list);
            return new Tuple2<>(customerID, customers);
        }).reduceByKey((p1, p2) -> {
            p1.getCustomersList().addAll(p2.getCustomersList());
            return p1;
        });
        dao.save(pairRDD, HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName(),
            new HBaseColumnField(null, HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getColumnFamily(),
                CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME));
    }

    /**
     * Save customer IDDOB index.
     *
     * @param input the input
     * @throws JobDaoException the job dao exception
     */
    private void saveCustomerIDDOBIndex(final Dataset<Row> input) throws JobDaoException {

        final JavaPairRDD<String, CustomerIDsDOBModel> dobCustomerIDPair = input.toJavaRDD()
            .filter(row -> row.getAs(CustomerIDConstants.DOB_COLUMN) != null).mapToPair(row -> {
                final Date dob = row.getAs(CustomerIDConstants.DOB_COLUMN);
                final List<String> customerIDs = new ArrayList<>();
                customerIDs.add(row.getAs(JobConstants.CUSTOMER_ID_COLUMN));
                final CustomerIDsDOBModel dobModel = new CustomerIDsDOBModel(dob, customerIDs);
                return new Tuple2<>(dob.toString(), dobModel);
            }).reduceByKey((p1, p2) -> {
                p1.getCustomerIDs().addAll(p2.getCustomerIDs());
                return p1;
            });
        dao.save(dobCustomerIDPair, HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getHbaseTableName(),
            new HBaseColumnField(null, HBaseTargetTableConstant.CUSTOMER_ID_DOB_SECONDARY_INDEX_TABLE.getColumnFamily(),
                CUSTOMER_ID_COLUMN_NAME));
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getCustomerIDPNRPassengersPairs()
     */
    @Override
    public JavaPairRDD<String, CustomerIDAttributesList> getCustomerIDAttributesPairs() throws CustomerIDException {

        try {
            return dao.getAll(HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getHbaseTableName(),
                new HBaseColumnField(CUSTOMER_ID_PNR_PASSENGERS_COLUMN_NAME,
                    HBaseTargetTableConstant.CUSTOMER_ID_PNRPASSENGERS_INDEX_TABLE.getColumnFamily()),
                CustomerIDAttributesList.class);
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getAllRecords()
     */
    @Override
    public Dataset<Row> getAllRecords() throws CustomerIDException {

        final HBaseTargetTableConstant targetTable = HBaseTargetTableConstant.CUSTOMER_ID_TABLE;
        try {
            final StructType schema = schemaService.get(HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName());
            return dao.getAll(targetTable.getHbaseTableName(), schema);
        } catch (final JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e)
                .errorMessage("Error occured while fetching records of customerID table").build();
        } catch (final MasterSchemaAccessException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e)
                .errorMessage("Error occured while accessing schema of customerID table.").build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.ReservationService#getAllPassengersRecords(java.lang.String[])
     */
    @Override
    public JavaPairRDD<String, Row> getAllRecords(final String... columns) throws CustomerIDException {

        final HBaseTargetTableConstant targetTable = HBaseTargetTableConstant.CUSTOMER_ID_TABLE;
        try {
            StructType schema = schemaService.get(HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName());
            return dao.getAll(targetTable.getHbaseTableName(), targetTable.getColumnFamily(), schema, columns);
        } catch (MasterSchemaAccessException | JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e)
                .errorMessage("Error occured while accessing schema of customerID table.").build();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.CustomerIDService#getByPNRPassenger(org.apache.spark.sql.Dataset)
     */
    @Override
    public Dataset<Row> getByPNRPassenger(final Dataset<PNRPassenger> dataset) throws CustomerIDException {

        Dataset<Row> outputDS = null;
        try {
            final StructType schema = schemaService.get(HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName());
            final Dataset<String> keys = dataset.map(key -> key.getKey(), Encoders.STRING());
            outputDS = dao.getRows(keys, HBaseTargetTableConstant.CUSTOMER_ID_TABLE.getHbaseTableName(), schema).na().drop(1);
        } catch (final MasterSchemaAccessException | JobDaoException e) {
            throw CustomerIDExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
        }
        return outputDS;
    }

}
