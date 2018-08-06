package com.am.analytics.job.common.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.am.analytics.common.core.utils.BasicSchema;
import com.am.analytics.common.core.utils.DataType;
import com.am.analytics.common.core.utils.Pair;
import com.am.analytics.common.core.utils.Schema;
import com.am.analytics.job.model.ColumnMetadata;

/**
 * The Class SchemaUtil.
 *
 * @author am
 */
public class SchemaUtil {

    /**
     * Gets the schema.
     *
     * @param columnMetadataArray ColumnMetadata[]
     * @return schema Schema
     */
    public static Schema getSchema(final ColumnMetadata[] columnMetadataArray) {

        final List<Pair<String, DataType>> pairList = new ArrayList<>();

        for (final ColumnMetadata column : columnMetadataArray) {
            pairList.add(new Pair<String, DataType>(column.getName().toLowerCase(), column.getType()));
        }

        return new BasicSchema(pairList);
    }

    /**
     * Gets the schema.
     *
     * @param columnPrefix String
     * @param columnMetadataArray ColumnMetadata[]
     * @return schema Schema
     */
    public static Schema getSchema(final String columnPrefix, final ColumnMetadata[] columnMetadataArray) {

        final List<Pair<String, DataType>> pairList = new ArrayList<>();

        for (final ColumnMetadata column : columnMetadataArray) {
            pairList.add(new Pair<String, DataType>((columnPrefix + column.getName()).toLowerCase(), column.getType()));
        }

        return new BasicSchema(pairList, columnPrefix);
    }

    /**
     * Gets the struct type.
     *
     * @param columnMetadata ColumnMetadata[]
     * @return structType StructType
     */
    public static StructType getStructType(final ColumnMetadata[] columnMetadata) {

        final StructField[] fields = new StructField[columnMetadata.length];

        int i = 0;

        for (final ColumnMetadata column : columnMetadata) {
            fields[i++] = new StructField(column.getName().toLowerCase(), SchemaUtil.getDataType(column.getType()), true,
                Metadata.empty());
        }

        final StructType structType = new StructType(fields);

        return structType;
    }

    /**
     * This method accepts the com.am.analytics.common.core.utils.Schema object and returns the corresponding
     * org.apache.spark.sql.types.StructType object.
     * 
     * @param schema Schema
     * @return structType StructType
     */
    public static StructType getStructType(final Schema schema) {

        StructType structType = null;

        if (null != schema) {
            structType = new StructType();
            final String[] columns = schema.getColumnNames();

            final StructField[] fields = new StructField[columns.length];
            int i = 0;
            for (final String column : columns) {
                final StructField structField = new StructField(column.toLowerCase(), SchemaUtil.getDataType(schema
                    .getDataType(column)), true, Metadata.empty());
                fields[i] = structField;
                i++;
            }
            structType = new StructType(fields);
        }
        return structType;
    }

    /**
     * Gets the data type.
     *
     * @param type the type
     * @return the data type
     */
    public static org.apache.spark.sql.types.DataType getDataType(final DataType type) {

        org.apache.spark.sql.types.DataType dataType = DataTypes.StringType;

        if (null != type) {
            switch (type) {
            case DD_MM_YYYY:
            case YYYY_MM_DD:
            case YYYY_MM_DD_HH_MM_SS:
            case YYYY_MM_DD_HH_MM_SS_S:
            case GENERICDATE:
            case GENERICTIMESTAMP:
            case MM_DD_YYYY:
            case DATE:
            case DOBACSPAXDOCX:
                dataType = DataTypes.DateType;
                break;
            case TIMESTAMP:
                dataType = DataTypes.TimestampType;
                break;
            case INTEGER:
                dataType = DataTypes.IntegerType;
                break;
            case REAL_NUMBER:
                dataType = DataTypes.DoubleType;
                break;
            case LONG:
            case SCIENTIFIC_NUMBER:
                dataType=DataTypes.LongType;
                break;
            case HH_MM_SS_MM:
            case STRING:
            case SCIENTIFIC_STRING:    
                dataType = DataTypes.StringType;
                break;
            default:
                break;
            }
        }
        return dataType;
    }

    /**
     * Gets the data type.
     *
     * @param type the type
     * @return the data type
     */
    public static DataType getDataType(final org.apache.spark.sql.types.DataType type) {

        DataType finalType = DataType.STRING;

        if (null != finalType) {
            if (type == DataTypes.StringType) {
                finalType = DataType.STRING;
            } else if (DataTypes.IntegerType == type) {
                finalType = DataType.INTEGER;
            } else if (DataTypes.DoubleType == type) {
                finalType = DataType.REAL_NUMBER;
            } else if (DataTypes.DateType == type) {
                finalType = DataType.DATE;
            }else if (DataTypes.TimestampType == type) {
                finalType = DataType.TIMESTAMP;
            }else if(DataTypes.LongType==type){
                finalType=DataType.LONG;
            }
        }
        return finalType;
    }
    
    /**
     * Gets the merged schema.
     *
     * @param structTypes the struct types
     * @return the merged schema
     */
    public static StructType getMergedSchema(final StructType...structTypes) {
        
        List<StructField> structFields = new ArrayList<>();
        for (StructType structType : structTypes) {
            if (null != structType) {
                StructField[] fields = structType.fields();
                for (StructField structField : fields) {
                    structFields.add(structField);
                }
            }
        }
        return new StructType(structFields.toArray(new StructField[structFields.size()]));
    }
   
    /**
     * creates a schema fields array.
     *
     * @param schema the schema
     * @return the schema fields
     */
    public static String[] getSchemaFields(final StructType schema) {

        return schema.fieldNames();
    }

}
