package com.hbase.poc.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


/**
 * DAO to fetch the jobs data from underlying repository that can be used by the jobs.
 * <p>
 * In general this service provide CRUD operations over the customer database.
 * </p>
 * 
 * @param <T> Output i.e the results expected out of the service
 * @param <Q> input data fromat for the service.
 */
public interface JobsDao<T, Q> extends Serializable {

    /**
     * Get(s) the record from table <code> tableName </code> mapped by the <code>key </code>
     * <p>
     * It will return the record with all the columns and column families
     * </p>
     * .
     *
     * @param tableName {@link String} name of the table from which the records are to be fetched.
     * @param key {@link String} key with which records are mapped.
     * @return <code>T</code> the record
     * @throws JobDaoException In case data cannot be retrieved from the underlying repository.
     */
    public T getDataRecord(final String tableName, final String key) throws Exception;

    /**
     * Gets the data record.
     *
     * @param tableName the table name
     * @param key the key
     * @param generateHash the generate hash
     * @return the data record
     * @throws JobDaoException the job dao exception
     */
    public T getDataRecord(final String tableName, final String key, final boolean generateHash) throws Exception;

    /**
     * Get(s) the record from table <code>tableName</code> and the given column family. Choosing a column family is like a
     * projection where a set of columns from a specific columnFamily can be chosen.
     *
     * @param tableName {@link String} name of the table whose records are to be fetched.
     * @param columnFamily {@link String} name of the column family whose columns are to be picked. if null, then all the column
     *            families are returned.
     * @param key {@link String} key of the record
     * @param columns the columns
     * @return <code>T</code> type of record
     * @throws JobDaoException in case records cannot be fetched
     */
    public T getDataRecord(final String tableName,
                           final String columnFamily,
                           final String key,
                           final String... columns) throws Exception;

    /**
     * Gets the data record.
     *
     * @param tableName the table name
     * @param columnFamily the column family
     * @param key the key
     * @param generateHash the generate hash
     * @return the data record
     * @throws JobDaoException the job dao exception
     */
    public T getDataRecord(final String tableName,
                           final String columnFamily,
                           final String key,
                           final boolean generateHash) throws Exception;

    /**
     * Saves the record of type <code>Q</code> in the given table.
     * 
     * @param tableName {@link String} name of the table in which records are to be saved.
     * @param save <code>Q</code> record to be saved.
     * @throws JobDaoException when records cannot be saved.
     */
    public void save(String tableName, Q save) throws Exception;

    /**
     * Gets the data records.
     *
     * @param tableName the table name
     * @param columnFamily the column family
     * @param keys the keys
     * @return the data records
     * @throws JobDaoException the job dao exception
     */
    public List<T> getDataRecords(final String tableName,
                                  final String columnFamily,
                                  final List<String> keys) throws Exception;

    /**
     * Gets the data records.
     *
     * @param tableName the table name
     * @param columnFamily the column family
     * @param keys the keys
     * @param generateHash the generate hash
     * @return the data records
     * @throws JobDaoException the job dao exception
     */
    public List<T> getDataRecords(final String tableName,
                                  final String columnFamily,
                                  final List<String> keys,
                                  final boolean generateHash) throws Exception;

    /**
     * Creates the.
     *
     * @param tableName the table name
     * @param totalSplits the total splits
     * @param columnFamilies the column families
     * @throws JobDaoException the job dao exception
     */
    public void create(final String tableName, final int totalSplits, final String... columnFamilies) throws Exception;

    /**
     * Delete table.
     *
     * @param tableName the table name
     * @throws JobDaoException the job dao exception
     */
    public void deleteTable(final String tableName) throws Exception;
    
    /**
     * Backup table.
     *
     * @param tableName the table name
     * @param snapshotName the snapshot name
     * @throws JobDaoException the job dao exception
     */
    public void backupTable(final String tableName, final String snapshotName) throws Exception;
    
    /**
     * Checks if is table exist.
     *
     * @param tableName the table name
     * @return true, if is table exist
     * @throws JobDaoException the job dao exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public boolean isTableExist(final String tableName) throws Exception;
}
