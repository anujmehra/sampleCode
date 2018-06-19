package com.hbase.poc.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.am.analytics.common.core.utils.ApplicationContextUtil;

import com.hbase.poc.utils.ArrayUtils;
import com.hbase.poc.utils.CollectionUtils;
import com.hbase.poc.utils.DigestUtils;
import com.hbase.poc.utils.Bytes;
import com.hbase.poc.utils.StringUtils;

import com.am.analytics.common.core.utils.Pair;
import com.am.analytics.job.common.model.ExistingRow;
import com.am.analytics.job.common.model.NewRow;
import com.am.analytics.job.common.model.RowMerger;

import com.am.analytics.job.common.utils.Function1Utils;
import com.am.analytics.job.common.utils.RowUtil;
import com.am.analytics.job.dataaccess.dao.DatasetDao;
import com.am.analytics.job.dataaccess.dao.JobsDao;
import com.am.analytics.job.dataaccess.exception.JobDaoException;
import com.am.analytics.job.dataaccess.exception.JobDaoException.JobDaoExceptionBuilder;
import com.am.analytics.job.model.HBaseColumnField;
import com.am.analytics.job.model.TableFieldMapping;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * HBase specific implementation of <code>DatasetDao</code>.
 *
 * @see DatasetDao
 */
@Repository
public class DatasetDaoImpl implements DatasetDao<HBaseColumnField> {

    /**
     * Serialized version UID of the class.
     */
    private static final long serialVersionUID = 1L;

    /** The dao. */
    @Autowired
    @Qualifier("hBaseDaoBean")
    private JobsDao<Result, Put> dao;

    /** The session. */
    @Autowired
    private transient SparkSession session;

    /** The hdfs home. */
    @Value("${ama.hdfs.home.path}")
    private String amHome;

    /** The Constant DUMP_FILE_TEMP_DIRECTORY. */
    private static final String DUMP_FILE_TEMP_DIRECTORY = "/tmp/dump/";

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#save(org.apache.spark.sql.Dataset,
     * com.am.analytics.job.model.TableFieldMapping)
     */
    @Override
    public void save(final Dataset<Row> dataset, final TableFieldMapping<HBaseColumnField> mapping) throws JobDaoException {

        this.save(dataset, mapping, true);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#save(org.apache.spark.sql.Dataset,
     * com.am.analytics.job.model.TableFieldMapping, boolean)
     */
    @Override
    public void save(final Dataset<Row> dataset,
                     final TableFieldMapping<HBaseColumnField> mapping,
                     final boolean saveKey) throws JobDaoException {

        if (null != dataset && null != mapping) {
            try {
                final String tableName = mapping.getTableName();
                final Configuration newConfig = getHBaseConfig(tableName);
                final JavaPairRDD<ImmutableBytesWritable, Put> rdd = getHBaseRDD(dataset, mapping, saveKey);
                if (rdd != null) {
                    rdd.saveAsNewAPIHadoopDataset(newConfig);
                }
            } catch (final IOException e) {
                throw new JobDaoException.JobDaoExceptionBuilder().errorMessage("Error occured while fetching job configuration")
                    .throwable(e).build();
            }
        }
    }

    /**
     * Gets the new dump file path.
     *
     * @return the new dump file path
     */
    private String getNewDumpFilePath() {

        final long time = System.currentTimeMillis();
        return StringUtils.concatenate(amHome, DUMP_FILE_TEMP_DIRECTORY, time);
    }

    /**
     * Gets the HBase config.
     *
     * @param tableName the table name
     * @return the HBase config
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private Configuration getHBaseConfig(final String tableName) throws IOException {

        final Configuration hbaseConf = ApplicationContextUtil.getBean(BeanConstants.HBASE_CONFIGURATION);
        final Job newAPIJobConfiguration = Job.getInstance(hbaseConf);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
        return newAPIJobConfiguration.getConfiguration();
    }

    /**
     * Gets the HBase RDD.
     *
     * @param dataset the dataset
     * @param mapping the mapping
     * @param saveKey the save key
     * @return the h base RDD
     */
    private JavaPairRDD<ImmutableBytesWritable, Put> getHBaseRDD(final Dataset<Row> dataset,
                                                                 final TableFieldMapping<HBaseColumnField> mapping,
                                                                 final boolean saveKey) {

        JavaPairRDD<ImmutableBytesWritable, Put> hBaseRDD = null;
        final String keyField = mapping.getKeyField();
        final Collection<HBaseColumnField> columnFields = mapping.getColumnFields();
        if (!CollectionUtils.isEmpty(columnFields)) {
            hBaseRDD = dataset.javaRDD().mapToPair(row -> {

                final byte[] bytes = bytesToHex(getBytes(row.getAs(keyField))).getBytes("UTF-8");
                final Put put = new Put(bytes);
                for (final HBaseColumnField field : columnFields) {
                    if (field != null) {
                        if (saveKey || !field.getColumnName().equals(keyField)) {
                            final String columnFamily = field.getColumnFamily();
                            final String columnName = field.getColumnName();
                            final String fieldName = field.getFieldName();
                            final Object fieldObject = row.getAs(fieldName);
                            if (null != fieldObject) {
                                final byte[] valueBytes = getBytes(fieldObject);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), valueBytes);
                            }
                        }
                    }
                }
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });
        }
        return hBaseRDD;
    }

    /**
     * Gets the bytes.
     *
     * @param fieldObject the field object
     * @return the bytes
     */
    private static byte[] getBytes(final Object fieldObject) {

        byte[] value = null;
        if (null != fieldObject) {
            if (fieldObject instanceof String) {
                value = Bytes.toBytes((String) fieldObject);
            } else if (fieldObject instanceof Integer) {
                value = Bytes.toBytes((int) fieldObject);
            } else if (fieldObject instanceof Double) {
                value = Bytes.toBytes((double) fieldObject);
            } else if (fieldObject instanceof Float) {
                value = Bytes.toBytes((float) fieldObject);
            } else if (fieldObject instanceof Long) {
                value = Bytes.toBytes((long) fieldObject);
            } else if (fieldObject instanceof Serializable) {
                value = Bytes.toBytes((Serializable) fieldObject);
            }
        }
        return value;
    }

    /**
     * Gets the object.
     *
     * @param value the value
     * @param clazz the clazz
     * @return the object
     */
    private Object getObject(final byte[] value, final Class<?> clazz) {

        Object valueObject = null;

        if (!ArrayUtils.isEmpty(value)) {
            if (clazz == Float.class) {
                valueObject = Bytes.toFloat(value);
            } else if (clazz == Double.class) {
                valueObject = Bytes.toDouble(value);
            } else if (clazz == String.class) {
                valueObject = Bytes.toString(value);
            } else if (clazz == Integer.class) {
                valueObject = Bytes.toInt(value);
            } else if (clazz == Long.class) {
                valueObject = Bytes.toLong(value);
            } else {
                valueObject = Bytes.toObject(value);
            }
        }
        return valueObject;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getObjects(org.apache.spark.sql.Dataset, java.lang.String,
     * com.am.analytics.job.model.ColumnField, java.lang.Class)
     */
    @Override
    public <U> Dataset<U> getObjects(final Dataset<String> keys,
                                     final String tableName,
                                     final HBaseColumnField field,
                                     final Class<U> clazz) throws JobDaoException {

        Dataset<U> dataset = null;
        if (null != field && null != keys) {

            try {
                final JavaRDD<U> javaRDD = toPairRDD(keys, tableName, field, clazz).map(tuple -> tuple._2);
                dataset = session.createDataset(javaRDD.rdd(), getEncoder(clazz));
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage("Error occured while fetching all the records ").throwable(e)
                    .build();
            }
        }
        return dataset;
    }

    /**
     * To pair RDD.
     *
     * @param <U> the generic type
     * @param keys the keys
     * @param tableName the table name
     * @param field the field
     * @param clazz the clazz
     * @return the java pair RDD
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <U> JavaPairRDD<String, U> toPairRDD(final Dataset<String> keys,
                                                 final String tableName,
                                                 final HBaseColumnField field,
                                                 final Class<U> clazz) throws IOException {

        final String dumpFilePath = getNewDumpFilePath();
        final HBaseContext context = new HBaseContext(session.sparkContext(), this.getHBaseConfig(tableName), dumpFilePath);
      
        final Function1<String, Get> getCommandFunction = Function1Utils
            .toFunction1(key -> new Get(Bytes.toBytes(com.am.analytics.common.core.utils.DigestUtils.generateHash(key))));
        
        final Function1<Result, Tuple2<String, U>> resultToObjectFunction = Function1Utils.toFunction1(result -> {
            if (result == null) {
                return null;
            }
            U object = null;
            final Cell latestCell = result.getColumnLatestCell(Bytes.toBytes(field.getColumnFamily()),
                Bytes.toBytes(field.getColumnName()));

            if (null != latestCell) {
                final byte[] value = CellUtil.cloneValue(latestCell);
                if (null != value) {
                    object = value == null ? null : (U) getObject(value, clazz);
                }
            }
            return new Tuple2<>(Bytes.toString(result.getRow()), object);
        });
        final ClassTag classTag = scala.reflect.ClassManifestFactory.fromClass(Tuple2.class);
        final ClassTag<Tuple2<String, U>> finalTag = classTag;
        final RDD<String> keysRdd = keys.persist(StorageLevel.DISK_ONLY_2()).filter(Objects::nonNull).rdd();
        final RDD<Tuple2<String, U>> scalaRDD = context.bulkGet(TableName.valueOf(tableName), 10000, keysRdd, getCommandFunction,
            resultToObjectFunction, finalTag);
        return JavaRDD.fromRDD(scalaRDD, finalTag).filter(Objects::nonNull).mapToPair(tuple -> tuple);
    }

    /**
     * Gets the encoder.
     *
     * @param <U> the generic type
     * @param clazz the clazz
     * @return the encoder
     */
    @SuppressWarnings("unchecked")
    private <U> Encoder<U> getEncoder(final Class<U> clazz) {

        Encoder<U> encoder = null;
        if (clazz == String.class) {
            encoder = (Encoder<U>) Encoders.STRING();
        } else if (clazz == Integer.class) {
            encoder = (Encoder<U>) Encoders.INT();
        } else if (clazz == Double.class) {
            encoder = (Encoder<U>) Encoders.DOUBLE();
        } else {
            encoder = Encoders.bean(clazz);
        }
        return encoder;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getRows(org.apache.spark.sql.Dataset, java.lang.String,
     * org.apache.spark.sql.types.StructType)
     */
    @Override
    public Dataset<Row> getRows(final Dataset<String> keys,
                                final String tableName,
                                final StructType structType) throws JobDaoException {

        return getRows(keys, tableName, structType, null, new String[]{});
    }

    /* (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#deleteRows(org.apache.spark.sql.Dataset, java.lang.String, org.apache.spark.sql.types.StructType)
     */
    @Override
    public void deleteRows(final Dataset<String> keys, final String tableName, final StructType structType) throws JobDaoException {

        if (null != keys && !StringUtils.isEmpty(tableName) && null != structType) {
            try {
                final String dumpFilePath = getNewDumpFilePath();
                final HBaseContext context = new HBaseContext(session.sparkContext(), getHBaseConfig(tableName), dumpFilePath);
                final RDD<String> keysRdd = keys.persist(StorageLevel.DISK_ONLY_2()).filter(Objects::nonNull).rdd();
                final Function1<String, Delete> function1 = Function1Utils.toFunction1(key -> new Delete(
                    Bytes.toBytes(com.am.analytics.common.core.utils.DigestUtils.generateHash(key))));
                context.bulkDelete(keysRdd, TableName.valueOf(tableName), function1, 10000);
            } catch (IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage("Error occured while deleting the records ").throwable(e)
                    .build();
            }
        }
    }

    /**
     * To pair RDD.
     *
     * @param keys the keys
     * @param tableName the table name
     * @param schema the schema
     * @param columnFamily the column family
     * @param columns the columns
     * @return the java pair RDD
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private JavaPairRDD<String, Row> toPairRDD(final Dataset<String> keys,
                                               final String tableName,
                                               final StructType schema,
                                               final String columnFamily,
                                               final String... columns) throws IOException {

        final String dumpFilePath = getNewDumpFilePath();
        final HBaseContext context = new HBaseContext(session.sparkContext(), this.getHBaseConfig(tableName), dumpFilePath);
        final Function1<String, Get> getCommandFunction = Function1Utils
            .toFunction1(key -> prepareGetObject(key,columnFamily,columns));
        final Function1<Result, Tuple2<String, Row>> resultToObjectFunction = Function1Utils.toFunction1(result -> {
            if (result == null) {
                return null;
            }
            return new Tuple2<>(Bytes.toString(result.getRow()), RowUtil.getRow(result, schema));
        });
        final ClassTag classTag = scala.reflect.ClassManifestFactory.fromClass(Tuple2.class);
        final ClassTag<Tuple2<String, Row>> finalTag = classTag;
        final RDD<String> keysRdd = keys.persist(StorageLevel.DISK_ONLY_2()).filter(Objects::nonNull).rdd();
        final RDD<Tuple2<String, Row>> scalaRDD = context.bulkGet(TableName.valueOf(tableName), 10000, keysRdd, getCommandFunction,
            resultToObjectFunction, finalTag);
        return JavaRDD.fromRDD(scalaRDD, finalTag).filter(Objects::nonNull).mapToPair(tuple -> tuple);
    }

    /**
     * Prepare get object.
     *
     * @param key the key
     * @param columnFamily the column family
     * @param columns the columns
     * @return the gets the
     */
    private Get prepareGetObject(String key,final String columnFamily,final String... columns) {
        final Get get = new Get(Bytes.toBytes(com.am.analytics.common.core.utils.DigestUtils.generateHash(key)));
        if(!ArrayUtils.isEmpty(columns) && !StringUtils.isBlank(columnFamily)){
            final byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
            for(String column : columns){
                get.addColumn(columnFamilyBytes, Bytes.toBytes(column));
            }
        }

        return get;
    }

    /**
     * Bytes to hex.
     *
     * @param in the in
     * @return the string
     */
    private static String bytesToHex(final byte[] in) {

        return DigestUtils.generateHash(in);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getObjectsPair(org.apache.spark.sql.Dataset, java.lang.String,
     * com.am.analytics.job.model.ColumnField, java.lang.Class)
     */
    @Override
    public <U> JavaPairRDD<String, U> getObjectsPair(final Dataset<String> keys,
                                                     final String tableName,
                                                     final HBaseColumnField field,
                                                     final Class<U> clazz) throws JobDaoException {

        JavaPairRDD<String, U> resultRDD = null;
        if (null != keys && !StringUtils.isEmpty(tableName) && null != field) {
            try {
                resultRDD = toPairRDD(keys, tableName, field, clazz);
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
            }
        }
        return resultRDD;
    }

    /**
     * Gets the object from database.
     *
     * @param <U> the generic type
     * @param tableName the table name
     * @param field the field
     * @param key the key
     * @return the object from database
     * @throws JobDaoException the job dao exception
     */
    private <U> U getObjectFromDatabase(final String tableName,
                                        final HBaseColumnField field,
                                        final String key) throws JobDaoException {

        final Result result = dao.getDataRecord(tableName, field.getColumnFamily(), key, true);
        U finalObject = null;
        if (null != result) {
            final Cell latestCell = result.getColumnLatestCell(Bytes.toBytes(field.getColumnFamily()),
                Bytes.toBytes(field.getColumnName()));
            if (null != latestCell) {
                final byte[] value = CellUtil.cloneValue(latestCell);
                finalObject = Bytes.toObject(value);
            }
        }
        return finalObject;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getMappedObjectsPair(org.apache.spark.api.java.JavaPairRDD,
     * java.lang.String, com.am.analytics.job.model.ColumnField)
     */
    @Override
    public <U, V> JavaPairRDD<String, Pair<U, V>> getMappedObjectsPair(final JavaPairRDD<String, V> pairRDD,
                                                                       final String tableName,
                                                                       final HBaseColumnField field) {

        JavaPairRDD<String, Pair<U, V>> finalRDD = null;

        if (null != pairRDD && !StringUtils.isEmpty(tableName) && null != field) {
            finalRDD = pairRDD.mapToPair(inputTuple -> {
                final String key = inputTuple._1;
                final U finalObject = getObjectFromDatabase(tableName, field, key);
                return new Tuple2<>(key, new Pair<>(finalObject, inputTuple._2));
            });
        }
        return finalRDD;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#mergeAndGet(org.apache.spark.sql.Dataset, java.lang.String,
     * org.apache.spark.sql.types.StructType, com.am.analytics.job.common.model.RowMerger)
     */
    @Override
    public JavaRDD<Row> mergeAndGet(final Dataset<Row> rows,
                                    final String tableName,
                                    final StructType existingRowSchema,
                                    final RowMerger rowMerger) {

        JavaRDD<Row> finalDataset = null;
        if (null != rows && !StringUtils.isEmpty(tableName) && null != existingRowSchema && null != rowMerger) {
            final StructType currentRowSchema = rows.schema();
            finalDataset = rows.javaRDD().map(row -> {
                Row finalRow = null;
                final String key = rowMerger.getKeyFunciton().apply(row);
                final Result record = dao.getDataRecord(tableName, key, true);
                if (record == null || record.isEmpty()) {
                    finalRow = new NewRow(RowUtil.getValues(row, currentRowSchema), currentRowSchema);
                } else {
                    final Row existingRow = RowUtil.getRow(record, existingRowSchema);
                    final BiFunction<Row, Row, Row> mergeFunction = rowMerger.getMergeFunction();
                    final Row mergedRow = mergeFunction.apply(existingRow, row);
                    finalRow = new ExistingRow(RowUtil.getValues(mergedRow, existingRowSchema), existingRowSchema);
                }
                return finalRow;
            });
        }
        return finalDataset;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#save(org.apache.spark.api.java.JavaPairRDD, java.lang.String,
     * com.am.analytics.job.model.ColumnField)
     */
    @Override
    public <V> void save(final JavaPairRDD<String, V> inputPairRDD,
                         final String tableName,
                         final HBaseColumnField field) throws JobDaoException {

        if (null != inputPairRDD && null != tableName && null != field) {
            try {
                final Configuration config = getHBaseConfig(tableName);
                final JavaPairRDD<ImmutableBytesWritable, Put> rdd = getHBaseRDD(inputPairRDD, field);
                if (rdd != null) {
                    rdd.saveAsNewAPIHadoopDataset(config);
                }
            } catch (final IOException e) {
                throw new JobDaoException.JobDaoExceptionBuilder().errorMessage("Error occured while fetching job configuration")
                    .throwable(e).build();
            }
        }

    }

    /**
     * Gets the h base RDD.
     *
     * @param <V> the value type
     * @param inputPairRDD the input pair RDD
     * @param field the field
     * @return the h base RDD
     */
    private <V> JavaPairRDD<ImmutableBytesWritable, Put> getHBaseRDD(final JavaPairRDD<String, V> inputPairRDD,
                                                                     final HBaseColumnField field) {

        JavaPairRDD<ImmutableBytesWritable, Put> hbaseRDD = null;
        if (null != field && null != inputPairRDD) {

            hbaseRDD = inputPairRDD.mapToPair(tuple -> {
                final String rowKey = tuple._1;
                final byte[] bytes = bytesToHex(getBytes(rowKey)).getBytes("UTF-8");
                final Put put = new Put(bytes);
                final String columnFamily = field.getColumnFamily();
                final String columnName = field.getColumnName();
                final Object fieldObject = tuple._2;
                final byte[] valueBytes = fieldObject == null ? null : getBytes(fieldObject);
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), valueBytes);
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });
        }

        return hbaseRDD;

    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getAll(java.lang.String, org.apache.spark.sql.types.StructType)
     */
    @Override
    public Dataset<Row> getAll(final String tableName, final StructType schema) throws JobDaoException {

        Dataset<Row> dataset = null;
        if (!StringUtils.isEmpty(tableName) && null != schema) {
            try {
                final Scan scan = getFullScan();
                final JavaPairRDD<String, Row> rowRDD = scanTableRows(tableName, schema, scan);
                dataset = session.createDataFrame(rowRDD.values(), schema);
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage(e.getLocalizedMessage()).throwable(e).build();
            }
        }
        return dataset;
    }

    /**
     * Scan table rows.
     *
     * @param tableName the table name
     * @param schema the schema
     * @param scan the scan
     * @return the java pair RDD
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private JavaPairRDD<String, Row> scanTableRows(final String tableName,
                                                   final StructType schema,
                                                   final Scan scan) throws IOException {

        final String dumpFilePath = getNewDumpFilePath();
        final HBaseContext context = new HBaseContext(session.sparkContext(), getHBaseConfig(tableName), dumpFilePath);
        return JavaRDD.fromRDD(context.hbaseRDD(TableName.valueOf(tableName), scan), null)
            .mapToPair(tuple -> new Tuple2<>(Bytes.toString(tuple._2.getRow()), RowUtil.getRow(tuple._2, schema)));
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getAll(java.lang.String,
     * com.am.analytics.job.model.ColumnField, java.lang.Class)
     */
    @Override
    public <U> JavaPairRDD<String, U> getAll(final String tableName,
                                             final HBaseColumnField field,
                                             final Class<U> clazz) throws JobDaoException {

        JavaPairRDD<String, U> pairRDD = null;
        if (!StringUtils.isEmpty(tableName) && null != field) {
            try {
                final Scan scan = getFullScan();
                final String dumpFilePath = getNewDumpFilePath();
                final HBaseContext context = new HBaseContext(session.sparkContext(), getHBaseConfig(tableName), dumpFilePath);
                pairRDD = JavaRDD.fromRDD(context.hbaseRDD(TableName.valueOf(tableName), scan), null)
                    .mapToPair(tuple -> getPair(tuple._2, field, clazz));
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().throwable(e).errorMessage(e.getLocalizedMessage()).build();
            }
        }
        return pairRDD;
    }

    /**
     * Gets the pair.
     *
     * @param <U> the generic type
     * @param result the result
     * @param field the field
     * @param clazz the clazz
     * @return the pair
     */
    private <U> Tuple2<String, U> getPair(final Result result, final HBaseColumnField field, final Class<U> clazz) {

        Tuple2<String, U> tuple = null;
        if (null != result) {

            final byte[] keyBytes = result.getRow();
            final String key = Bytes.toString(keyBytes);
            final byte[] value = CellUtil.cloneValue(
                result.getColumnLatestCell(Bytes.toBytes(field.getColumnFamily()), Bytes.toBytes(field.getColumnName())));
            final U object = (U) getObject(value, clazz);
            tuple = new Tuple2<>(key, object);
        }
        return tuple;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getAll(java.lang.String, java.lang.String,
     * org.apache.spark.sql.types.StructType, java.lang.String[])
     */
    public JavaPairRDD<String, Row> getAll(final String tableName,
                                           final String columnFamily,
                                           final StructType tableSchema,
                                           final String... columns) throws JobDaoException {

        JavaPairRDD<String, Row> requiredRows = null;
        if (!StringUtils.isEmpty(tableName) && !StringUtils.isEmpty(columnFamily) && null != tableSchema) {
            final Scan scan = getFullScan();
            StructType finalSchema = tableSchema;
            final byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
            if (!ArrayUtils.isEmpty(columns)) {
                final List<StructField> finalFields = new ArrayList<>();
                final StructField[] fields = tableSchema.fields();
                for (final String column : columns) {
                    scan.addColumn(columnFamilyBytes, Bytes.toBytes(column));
                    final int index = tableSchema.fieldIndex(column);
                    if (index >= 0) {
                        finalFields.add(fields[index]);
                    }
                }
                finalSchema = new StructType(finalFields.toArray(new StructField[finalFields.size()]));
            }
            try {
                requiredRows = scanTableRows(tableName, finalSchema, scan);
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage("Error occured while fetching data").throwable(e).build();
            }
        }
        return requiredRows;
    }

    /**
     * Gets the full scan.
     *
     * @return the full scan
     */
    private Scan getFullScan() {

        final Scan scan = new Scan();
        scan.setCaching(10);
        scan.setCacheBlocks(false);
        return scan;
    }

    /* (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.DatasetDao#getRows(org.apache.spark.sql.Dataset, java.lang.String, org.apache.spark.sql.types.StructType, java.lang.String, java.lang.String[])
     */
    @Override
    public Dataset<Row> getRows(Dataset<String> keys,
                                String tableName,
                                StructType structType,
                                String columnFamily,
                                String... columns) throws JobDaoException {

        Dataset<Row> finalDataset = null;
        if (null != keys && !StringUtils.isEmpty(tableName) && null != structType) {
            try {
                JavaRDD<Row> rdd = toPairRDD(keys, tableName, structType,columnFamily,columns).map(tuple -> tuple._2);
                finalDataset = session.createDataFrame(rdd, structType);
            } catch (final IOException e) {
                throw JobDaoExceptionBuilder.create().errorMessage("Error occured while fetching all the records ").throwable(e)
                    .build();
            }
        }
        return finalDataset;
    }

}
