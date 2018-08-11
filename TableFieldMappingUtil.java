package com.am.analytics.job.common.utils;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.sql.types.StructType;

import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.job.common.constant.JobConstants;
import com.am.analytics.job.model.HBaseColumnField;
import com.am.analytics.job.model.TableFieldMapping;

/**
 * The Class TableFieldMappingUtil.
 */
public final class TableFieldMappingUtil  {

    /**
     * Instantiates a new table field mapping util.
     */
    private TableFieldMappingUtil() {
        // private constructor for util class.
    }

    /**
     * Gets the table field mapping.
     *
     * @param schema the schema
     * @param tableName the table name
     * @param keyField the key field
     * @return the table field mapping
     */
    public static TableFieldMapping<HBaseColumnField> getTableFieldMapping(final StructType schema,
                                                                      final String tableName,
                                                                      final String keyField) {

        TableFieldMapping<HBaseColumnField> tableFieldMapping = null;
        if (null != schema && !StringUtils.isEmpty(tableName) && !StringUtils.isEmpty(keyField)) {
            tableFieldMapping = getTableFieldMapping(schema, tableName, keyField, JobConstants.DEFAULT_COLUMN_FAMILY_NAME);

        }
        return tableFieldMapping;
    }

    /**
     * Gets the table field mapping.
     *
     * @param schema the schema
     * @param tableName the table name
     * @param keyField the key field
     * @param columnFamily the column family
     * @return the table field mapping
     */
    public static TableFieldMapping<HBaseColumnField> getTableFieldMapping(final StructType schema,
                                                                      final String tableName,
                                                                      final String keyField,
                                                                      final String columnFamily) {

        TableFieldMapping<HBaseColumnField> tableFieldMapping = null;
        if (null != schema && !StringUtils.isEmpty(tableName) && !StringUtils.isEmpty(keyField)) {
            final Collection<HBaseColumnField> hBaseColumnFields = new ArrayList<>();
            String cf = columnFamily;
            if (StringUtils.isBlank(cf)) {
                cf = JobConstants.DEFAULT_COLUMN_FAMILY_NAME;
            }
            for (final String fieldName : schema.fieldNames()) {
                final HBaseColumnField hBaseColumnField = new HBaseColumnField(fieldName, cf);
                hBaseColumnFields.add(hBaseColumnField);
            }
            tableFieldMapping = new TableFieldMapping<HBaseColumnField>(tableName, keyField, hBaseColumnFields);
        }
        return tableFieldMapping;
    }

}
