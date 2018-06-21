package com.am.analytics.job.service.db.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.am.analytics.common.core.constant.BeanConstants;
import com.am.analytics.common.core.utils.CollectionUtils;
import com.am.analytics.common.core.utils.DataType;
import com.am.analytics.common.core.utils.Logger;
import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.job.common.constant.HBaseTargetTableConstant;
import com.am.analytics.job.common.utils.Bytes;
import com.am.analytics.job.common.utils.SchemaUtil;
import com.am.analytics.job.dataaccess.dao.JobsDao;
import com.am.analytics.job.dataaccess.exception.JobDaoException;
import com.am.analytics.job.service.db.MasterSchemaService;
import com.am.analytics.job.service.exception.MasterSchemaAccessException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The Class MasterSchemaServiceImpl is implementation for {@link MasterSchemaService}.
 */
@Service
public class MasterSchemaServiceImpl implements MasterSchemaService {

    /**
     * Serialized version UID of the class.
     */
    private static final long serialVersionUID = 1L;

    /** The job records dao. */
    @Autowired
    @Qualifier(BeanConstants.RECORD_DAO_BEAN)
    private JobsDao<Result, Put> jobRecordsDao;

    /**
     * Hash-map to cache schema's as there won't be too many schema's getting generated.
     */
    private final Map<String, StructType> schemaMap = new HashMap<>();

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MasterSchemaServiceImpl.class);

    /** The Constant SCHEMA_COLUMN_NAME. */
    private static final String SCHEMA_COLUMN_NAME = "schema";

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.MasterSchemaService#save(java.lang.String,
     * org.apache.spark.sql.types.StructType)
     */
    @Override
    public void save(final String tableName, final StructType schema) throws MasterSchemaAccessException {

        LOGGER.info("Saving schema of ", tableName);
        if (!StringUtils.isEmpty(tableName) && null != schema) {
            schemaMap.put(tableName, schema);
            final TableSchema tableSchema = new TableSchema(tableName);
            LOGGER.info("Creating the table schema");
            final StructField[] allFields = schema.fields();
            for (final StructField field : allFields) {
                tableSchema.add(field.name(), SchemaUtil.getDataType(field.dataType()));
            }
            LOGGER.info("Converting the table schema object to JSON");
            final ObjectMapper mapper = new ObjectMapper();
            try {
                final String schemaString = mapper.writeValueAsString(tableSchema);
                LOGGER.debug("Schema string is ", schemaString);
                jobRecordsDao.save(HBaseTargetTableConstant.SCHEMA_TABLE_NAME.getHbaseTableName(), this.toPut(tableName, schemaString));
                LOGGER.info("Save successful");
            } catch (final JsonProcessingException exception) {
                throw new MasterSchemaAccessException("Error occured while converting schema to json for table ", exception);
            } catch (final JobDaoException exception) {
                throw new MasterSchemaAccessException("Error occured while saving the records to master  schema ", exception);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.am.analytics.job.service.db.MasterSchemaService#get(java.lang.String)
     */
    @Override
    public StructType get(final String tableName) throws MasterSchemaAccessException {

        try {
            StructType structType = null;
            if (StringUtils.isNotEmpty(tableName)) {
                structType = schemaMap.get(tableName);
                if (null == structType) {
                    final Result result = jobRecordsDao
                        .getDataRecord(HBaseTargetTableConstant.SCHEMA_TABLE_NAME.getHbaseTableName(), tableName);
                    final Cell cell = result.getColumnLatestCell(
                        Bytes.toBytes(HBaseTargetTableConstant.SCHEMA_TABLE_NAME.getColumnFamily()),
                        Bytes.toBytes(SCHEMA_COLUMN_NAME));
                    if (null != cell) {
                        final String value = Bytes.toString(CellUtil.cloneValue(cell));
                        final ObjectMapper mapper = new ObjectMapper();
                        final TableSchema schema = mapper.readValue(value, TableSchema.class);
                        structType = toSchema(schema);
                    }
                }
            }
            return structType;
        } catch (final JobDaoException e) {
            throw new MasterSchemaAccessException("Error occured while fetching table schema", e);
        } catch (final IOException e) {
            throw new MasterSchemaAccessException("Error occured while converting schema's json string to table schema model.", e);
        }
    }

    /**
     * To put.
     *
     * @param tableName the table name
     * @param schema the schema
     * @return the put
     */
    protected Put toPut(final String tableName, final String schema) {

        LOGGER.info("Converting to put object");
        final Put put = new Put(Bytes.toBytes(tableName));
        put.addColumn(Bytes.toBytes(HBaseTargetTableConstant.SCHEMA_TABLE_NAME.getColumnFamily()),
            Bytes.toBytes(SCHEMA_COLUMN_NAME), Bytes.toBytes(schema));
        return put;
    }

    /**
     * To schema.
     *
     * @param schema the schema
     * @return the struct type
     */
    protected StructType toSchema(final TableSchema schema) {

        StructType structType = null;
        if (!CollectionUtils.isEmpty(schema.columns)) {
            final StructField[] fields = new StructField[schema.columns.size()];
            structType = new StructType(fields);
            int i = 0;
            for (final SchemaColumn schemaColumn : schema.columns) {
                if (null != schemaColumn) {
                    fields[i++] = new StructField(schemaColumn.name, SchemaUtil.getDataType(schemaColumn.dataType), true,
                        Metadata.empty());
                }
            }
        }
        return structType;
    }

    /**
     * Represents the table schema.
     * 
     */
    private static final class TableSchema {

        /** The name. */
        @JsonProperty("name")
        private String name;

        /** The columns. */
        @JsonProperty("columns")
        private List<SchemaColumn> columns;

        /**
         * Instantiates a new table schema.
         */
        /*
         * Only to initialize using reflection, to be used by Jackson object mapper.
         */
        private TableSchema() {

        }

        /**
         * Instantiates a new table schema.
         *
         * @param tableName the table name
         */
        TableSchema(final String tableName) {

            this.name = tableName;
        }

        /**
         * Adds the.
         *
         * @param name the name
         * @param type the type
         */
        public void add(final String name, final DataType type) {

            if (columns == null) {
                columns = new ArrayList<>();
            }
            columns.add(new SchemaColumn(type, name));
        }
    }

    /**
     * Schema for the column with column Name and column type, not to be used outside the class.
     * 
     */
    private static final class SchemaColumn {

        /** The name. */
        @JsonProperty("name")
        private String name;

        /** The data type. */
        @JsonProperty("type")
        private DataType dataType;

        /**
         * Instantiates a new schema column.
         *
         * @param type the type
         * @param name the name
         */
        private SchemaColumn(final DataType type, final String name) {

            this.name = name;
            this.dataType = type;
        }
        
        /**
         * Instantiates a new schema column.
         */
        /*
         * Only to initialize using reflection, to be used by Jackson object mapper.
         */
        private SchemaColumn() {

        }

    }
}
