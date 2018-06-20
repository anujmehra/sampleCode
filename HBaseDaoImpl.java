package com.hbase.poc.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.springframework.stereotype.Service;

import com.hbase.poc.utils.ArrayUtils;
import com.hbase.poc.utils.CollectionUtils;
import com.hbase.poc.utils.DigestUtils;
import com.hbase.poc.utils.StringUtils;
import com.hbase.poc.utils.Bytes;
import com.hbase.poc.service.JobsDao;


/**
 * The Class HBaseDaoImpl to access HBase
 */
@Service("hBaseDaoBean")
public class HBaseDaoImpl implements JobsDao<Result, Put> {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates the tables.
     *
     * @throws Exception the job dao exception
     */
    @PostConstruct
    public void createTables() throws Exception {

        final HBaseTargetTableConstant[] values = HBaseTargetTableConstant.values();
        System.out.println("Creating Hbase Tables.....");
        for (final HBaseTargetTableConstant value : values) {
            try {
                if (!isTableExist(value.getHbaseTableName())) {
                    this.create(value.getHbaseTableName(), value.getSplits(), value.getColumnFamily());
                }
            } catch (final IOException e) {
            	e.printStackTrace();
                throw e;
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecord(java.lang.String, java.lang.String)
     */
    @Override
    public Result getDataRecord(final String tableName, final String key) throws Exception {

    	System.out.println("Getting the data records for " + tableName + " with key  " + key);
        final String[] columns = null;
        return getDataRecord(tableName, null, key, columns);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecord(java.lang.String, java.lang.String, boolean)
     */
    public Result getDataRecord(final String tableName, final String key, final boolean generateHash) throws Exception {

    	System.out.println("Getting the data records for " + tableName + " with key  " + key);
        return getDataRecord(tableName, generateHash ? DigestUtils.generateHash(key) : key);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#create(java.lang.String, int, java.lang.String[])
     */
    @Override
    public void create(final String tableName, final int totalSplits, final String... columnFamilies) throws Exception {

    	System.out.println("Creating table " + tableName);
    	
        final Admin admin = getAdmin();
        try {
            final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            if (!ArrayUtils.isEmpty(columnFamilies)) {
                for (final String family : columnFamilies) {
                    final HColumnDescriptor familyDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                    familyDescriptor.setCompressionType(Algorithm.SNAPPY);
                    tableDescriptor.addFamily(familyDescriptor);
                }
            }
            
            System.out.println("Started creating table");
            
            final byte[][] splits = new RegionSplitter.HexStringSplit().split(totalSplits);
            admin.createTable(tableDescriptor, splits);
            
            System.out.println("Created table " +  tableName);
        } catch (final IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#deleteTable(java.lang.String)
     */
    @Override
    public void deleteTable(final String tableName) throws Exception {

        final Admin admin = getAdmin();
        try {
            admin.disableTables(tableName);
            admin.deleteTables(tableName);
        } catch (final IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecord(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String[])
     */
    @Override
    public Result getDataRecord(final String tableName,
                                final String columnFamily,
                                final String key,
                                final String... columns) throws Exception {

        Result result = null;
        System.out.println("Getting the datarecord for " + tableName + " column family is " + columnFamily + " row key "+ key);
        if (!StringUtils.isEmpty(tableName) && !StringUtils.isEmpty(key)) {

            final Table hTable = getTable(tableName);

            System.out.println("Table found : " + tableName);

            final Get get = new Get(Bytes.toBytes(key));
            if (!ArrayUtils.isEmpty(columns)) {
                for (final String column : columns) {
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                }
            } else if (!StringUtils.isEmpty(columnFamily)) {
                get.addFamily(Bytes.toBytes(columnFamily));
            }
            try {
                result = hTable.get(get);
            } catch (final IOException e) {
               e.printStackTrace();
               throw e;
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecord(java.lang.String, java.lang.String, java.lang.String,
     * boolean)
     */
    public Result getDataRecord(final String tableName,
                                final String columnFamily,
                                final String key,
                                final boolean generateHash) throws Exception {

        final String[] columns = null;
        return getDataRecord(tableName, columnFamily, generateHash ? DigestUtils.generateHash(key) : key, columns);
    }

    /**
     * Gets the table.
     *
     * @param tableName the table name
     * @return the table
     * @throws Exception the job dao exception
     */
    protected Table getTable(final String tableName) throws Exception {

    	System.out.println("Get Table " + tableName);
        final TableName table = TableName.valueOf(tableName);
        final Connection connection = com.am.analytics.job.dataaccess.factory.ConnectionFactory.get();
        try {
            return connection.getTable(table);
        } catch (final IOException e) {
        	System.out.println("Error occured while fetching table: ");
        	e.printStackTrace();
            throw e;
        }
    }

    /**
     * Checks if is table exist.
     *
     * @param tableName the table name
     * @return true, if is table exist
     * @throws Exception the job dao exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public boolean isTableExist(final String tableName) throws Exception, IOException {

        boolean isExist = false;
        if (!StringUtils.isEmpty(tableName)) {
            final TableName table = TableName.valueOf(tableName);
            final Admin admin = this.getAdmin();
            if (null != admin) {
                isExist = admin.tableExists(table);
            }
        }
        return isExist;
    }

    /**
     * Gets the admin.
     *
     * @return the admin
     * @throws Exception the job dao exception
     */
    protected Admin getAdmin() throws Exception {

        try {
            final Connection connection = com.am.analytics.job.dataaccess.factory.ConnectionFactory.get();
            return connection.getAdmin();
        } catch (final IOException e) {
        	System.out.println( "Error occured while getting Admin instance for cluster administration : ");
        	e.printStackTrace();
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#save(java.lang.String, java.lang.Object)
     */
    @Override
    public void save(final String tableName, final Put objectToSave) throws Exception {

        if (!StringUtils.isEmpty(tableName) && null != objectToSave) {
            final Table table = this.getTable(tableName);
            try {
                table.put(objectToSave);
            } catch (final IOException e) {
            	System.out.println("Error occured while saving record : ");
            	e.printStackTrace();
                throw e;
            }
        }
    }

    /**
     * Gets the data records.
     *
     * @param tableName the table name
     * @param keys the keys
     * @param generateHash the generate hash
     * @return the data records
     * @throws Exception the job dao exception
     */
    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecords(java.lang.String, java.util.List, boolean)
     */
    public List<Result> getDataRecords(final String tableName,
                                       final List<String> keys,
                                       final boolean generateHash) throws Exception {

        return this.getDataRecords(tableName, null, keys, generateHash);
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecords(java.lang.String, java.lang.String, java.util.List)
     */
    @Override
    public List<Result> getDataRecords(final String tableName,
                                       final String columnFamily,
                                       final List<String> keys) throws Exception {

        List<Result> result = null;
        
        System.out.println("Getting the datarecord for " + tableName);
        
        if (!StringUtils.isBlank(tableName) && !CollectionUtils.isEmpty(keys)) {
            final Table hTable = getTable(tableName);
            final List<Get> getList = new ArrayList<>();
            for (final String key : keys) {
                if (!StringUtils.isEmpty(key)) {
                    final Get get = new Get(Bytes.toBytes(key));
                    if (!StringUtils.isBlank(columnFamily)) {
                        get.addFamily(Bytes.toBytes(columnFamily));
                    }
                    getList.add(get);
                }
            }
            try {
                final Result[] results = hTable.get(getList);
                if (!ArrayUtils.isEmpty(results)) {
                    result = Arrays.asList(results);
                }
            } catch (final IOException e) {
            	System.out.println("Error occurred while fetching records from DB for table " + tableName);
            	e.printStackTrace();
                throw e;
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#getDataRecords(java.lang.String, java.lang.String, java.util.List,
     * boolean)
     */
    public List<Result> getDataRecords(final String tableName,
                                       final String columnFamily,
                                       final List<String> keys,
                                       final boolean generateHash) throws Exception {

        List<String> keysToSearch;
        if (generateHash) {
            keysToSearch = new ArrayList<>();
            for (final String key : keys) {
                keysToSearch.add(DigestUtils.generateHash(key));
            }
        } else {
            keysToSearch = keys;
        }
        return getDataRecords(tableName, columnFamily, keysToSearch);
    }

    /* (non-Javadoc)
     * @see com.am.analytics.job.dataaccess.dao.JobsDao#backupTable(java.lang.String, java.lang.String)
     */
    @Override
    public void backupTable(String tableName, String backupTableName) throws Exception {

        final Admin admin = getAdmin();
        try {
            final String snapshotName = "backup_snapshot";
            admin.disableTables(tableName);
            admin.snapshot(snapshotName, TableName.valueOf(tableName));
            admin.cloneSnapshot(snapshotName, TableName.valueOf(backupTableName));
            admin.deleteSnapshot(snapshotName);
            admin.enableTables(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

}
