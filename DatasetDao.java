/*
 * 
 */
package com.aeromexico.analytics.job.dataaccess.dao;

import java.io.Serializable;
import java.util.function.BiFunction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.aeromexico.analytics.common.core.utils.Pair;
import com.aeromexico.analytics.common.core.utils.Schema;
import com.aeromexico.analytics.job.common.model.ExistingRow;
import com.aeromexico.analytics.job.common.model.NewRow;
import com.aeromexico.analytics.job.common.model.RowMerger;
import com.aeromexico.analytics.job.dataaccess.exception.JobDaoException;
import com.aeromexico.analytics.job.model.BasicColumnField;
import com.aeromexico.analytics.job.model.ColumnField;
import com.aeromexico.analytics.job.model.HBaseColumnField;
import com.aeromexico.analytics.job.model.TableFieldMapping;

/**
 * The purpose of this class is to perform CRUD operations on a data repository using Spark Dataset and RDD API.
 * This class is to be used for the following main purposes:
 * <ul>
 * <li>When the bulk operations are to be performed on data repository</li>
 * <li>Data can be huge to not to fit into memory</li>
 * </ul>
 * 
 * @author Aeromexico
 * @param <T> implementation of ColumnField which can be specific to a data repository. It is the meta data to reach to a column in
 *            the specific repository.
 * @see BasicColumnField
 * @see HBaseColumnField
 */
public interface DatasetDao<T extends ColumnField> extends Serializable {

    /**
     * Saves the <code>Dataset</code> of row in the repository using the <code>mapping</code> provided to map each column with a
     * field.
     * <p>
     * The underlying mechanism stores the data assuming there is a key field in each row which can be fetched using the table field
     * mapping. This method by default saves the key field as the value parameter in the repository.
     * </p>
     * 
     * @param dataset {@link Dataset} to be saved. If <code>null</code> then request will be ignore
     * @param mapping {@link TableFieldMapping} using which the fields are mapped with table and respective columns. If
     *            <code>null</code> then request will be ignored.
     * @throws JobDaoException when the data cannot be saved
     */
    public void save(Dataset<Row> dataset, TableFieldMapping<T> mapping) throws JobDaoException;

    /**
     * Saves the <code>Dataset</code> of row in the repository using the <code>mapping</code> provided to map each column with a
     * field.
     * <p>
     * The underlying mechanism stores the data assuming there is a key field in each row which can be fetched using the table field
     * mapping. This method provides an optional parameter to save the key in the repository as the column.
     * </p>
     * 
     * @param dataset {@link Dataset} to be saved. If <code>null</code> then request will be ignore
     * @param mapping {@link TableFieldMapping} using which the fields are mapped with table and respective columns. If
     *            <code>null</code> then request will be ignored.
     * @param saveKey if <code>true</code> key will be saved as a column else not.
     * @throws JobDaoException when the data cannot be saved.
     */
    public void save(Dataset<Row> dataset, TableFieldMapping<T> mapping, boolean saveKey) throws JobDaoException;

    /**
     * Gets the rows of a table with provided <code> tableName </code> and given schema <code>structType</code>. This method fetches
     * the field(s) from repository and map them using the schema provided.
     *
     * @param keys {@link Dataset}  keys that maps the records of the primary table
     * @param tableName {@link String} name of the table. if <code>null</code> or <code>empty</code> it will be ignored.
     * @param structType {@link StructType} schema of the row which is mapped with the columns
     * @return {@link Dataset}  the records of the table.
     * @throws JobDaoException the job dao exception
     */
    public Dataset<Row> getRows(Dataset<String> keys, String tableName, StructType structType) throws JobDaoException;
    
    /**
     * Gets the rows of a table with provided <code> tableName </code> and given schema <code>structType</code>. This method fetches
     * the field(s) from repository and map them using the schema provided. It gets data for specified input columns only.
     *
     * @param keys {@link Dataset}  keys that maps the records of the primary table
     * @param tableName {@link String} name of the table. if <code>null</code> or <code>empty</code> it will be ignored.
     * @param structType {@link StructType} schema of the row which is mapped with the columns
     * @param columnFamily the column family
     * @param columns the columns
     * @return {@link Dataset}  the records of the table.
     * @throws JobDaoException the job dao exception
     */
    public Dataset<Row> getRows(Dataset<String> keys, String tableName, StructType structType,String columnFamily,String...columns) throws JobDaoException;
    
    /**
     * Delete rows.
     *
     * @param keys the keys
     * @param tableName the table name
     * @param structType the struct type
     * @throws JobDaoException the job dao exception
     */
    public void deleteRows(final Dataset<String> keys, final String tableName, final StructType structType) throws JobDaoException;

    /**
     * This method is used to merge the new rows with the old rows with the help of the given <code>rowMerger</code>. In case of
     * incremental data new rows of each table are encountered, all the rows are mapped to the master using their respective keys
     * which are provided using the rowMerger function.
     * <p>
     * In case the key is found in the repository with the given name then the rows are merged and Row is return as an instance of
     * {@link ExistingRow} and if the key is not found then Row is wrapped as an instance of {@link NewRow}.
     * </p>
     * 
     * @param rows {@link Dataset}  which are the new rows.
     * @param tableName {@link String} name of the master table with which the rows are to be searched.
     * @param structType {@link Schema} schema of the master table with which the records needs to be searched.
     * @param rowMerger {@link RowMerger} model that provides access to key function that provides keys from a row and a
     *            {@link BiFunction} which merges the old and new rows.
     * @return {@link JavaRDD}  with each instance of {@link ExistingRow} or {@link NewRow} based on if the key is
     *         found in the master table or not.
     */
    public JavaRDD<Row> mergeAndGet(Dataset<Row> rows, String tableName, StructType structType, RowMerger rowMerger);

    /**
     * This method get the records in case records are saved as serialized objects in the repository.
     *
     * @param <U> the generic type
     * @param keys {@link String} keys which are to be searched in the repository
     * @param tableName {@link String} name of the table from which the objects are to searched
     * @param field <code> T </code> the column in which the object is serialized.
     * @param clazz {@link Class}  <code> U </code>  class of the serialized object
     * @return {@link Dataset} U  where each entity is the serialized bean.
     * @throws JobDaoException the job dao exception
     */
    public <U> Dataset<U> getObjects(Dataset<String> keys, String tableName, T field, final Class<U> clazz) throws JobDaoException;

    /**
     * This method get the records in case records are saved as serialized objects in the repository. It also maps each object with
     * the key compared to the method {@link #getObjects(Dataset, String, ColumnField, Class)} which doesnot map the key with the
     * object.
     *
     * @param <U> the generic type
     * @param keys {@link String} keys which are to be searched in the repository
     * @param tableName {@link String} name of the table from which the objects are to searched
     * @param field <code> T </code> the column in which the object is serialized.
     * @param clazz the clazz
     * @return {@link JavaPairRDD}  U  where each entity is the serialized bean and {@link String} is the key mapped to each
     *         object.
     * @throws JobDaoException the job dao exception
     */
    public <U> JavaPairRDD<String, U> getObjectsPair(Dataset<String> keys,
                                                     String tableName,
                                                     T field,
                                                     Class<U> clazz) throws JobDaoException;

    /**
     * The functionality of this method is similar to {@link #getObjectsPair(Dataset, String, ColumnField, Class)} with the two major
     * differences
     * <ol>
     * <li>This method recives pair RDD in which key is already mapped to an object of type <code>U</code>.</li>
     * <li>This method return {@link Pair} U, V  which maps Key with U and V where U is the new object found in repository and V
     * is the old object.</li>
     * </ol>
     *
     * @param <U> the generic type
     * @param <V> the value type
     * @param pairRDD {@link JavaPairRDD} {@link String}, V  where key is the database key and V is the already binded object
     *            with key.
     * @param tableName {@link String} name of the table in the repository where the search is to be made.
     * @param field <code> T </code> column in which the object is serialized
     * @return {@link JavaPairRDD}  {@link String}, {@link Pair} U , V   where key is the database key, U and V are the
     *         database and already mapped objects respectively.
     * @see #getObjectsPair(Dataset, String, ColumnField, Class)
     */
    public <U, V> JavaPairRDD<String, Pair<U, V>> getMappedObjectsPair(JavaPairRDD<String, V> pairRDD, String tableName, T field);

    /**
     * Fetch all the Rows of the repository from the given table.
     * 
     * @param tableName {@link String} name of the table.
     * @param schema {@link StructType} schema of the row in the table
     * @return {@link Dataset} {@link Row} all the rows of the table in compliance with the given schema.
     * @throws JobDaoException when the records could not be fetched from the underlying repository.
     */
    public Dataset<Row> getAll(String tableName, StructType schema) throws JobDaoException;

    /**
     * Save.
     *
     * @param <V> the value type
     * @param inputPairRDD the input pair RDD
     * @param tableName the table name
     * @param Column the column
     * @throws JobDaoException the job dao exception
     */
    public <V> void save(JavaPairRDD<String, V> inputPairRDD, String tableName, T Column) throws JobDaoException;

    /**
     * Gets the all.
     *
     * @param <U> the generic type
     * @param tableName the table name
     * @param field the field
     * @param clazz the clazz
     * @return the all
     * @throws JobDaoException the job dao exception
     */
    public <U> JavaPairRDD<String, U> getAll(String tableName, T field, Class<U> clazz) throws JobDaoException;

    /**
     * Gets the all.
     *
     * @param tableName the table name
     * @param columnFamily the column family
     * @param tableSchema the table schema
     * @param columns the columns
     * @return the all
     * @throws JobDaoException the job dao exception
     */
    public JavaPairRDD<String, Row> getAll(final String tableName,
                                           final String columnFamily,
                                           StructType tableSchema,
                                           final String... columns) throws JobDaoException;

}
