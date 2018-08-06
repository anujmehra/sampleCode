package com.am.analytics.job.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.am.analytics.common.core.constant.Order;
import com.am.analytics.common.core.utils.ApplicationContextUtil;
import com.am.analytics.common.core.utils.ArrayUtils;
import com.am.analytics.common.core.utils.CollectionUtils;
import com.am.analytics.common.core.utils.Pair;
import com.am.analytics.common.core.utils.StringUtils;
import com.am.analytics.job.common.constant.JobConstants;
import com.am.analytics.job.common.constant.SabreTableNameColumnsEnum;
import com.google.common.base.Strings;

import scala.Tuple2;

/**
 * A utility to perform Row operations for <code>Dataset</code>. All the generic operation which belong to
 * <code> Dataset </code> should belong this utility.
 * 
 */
public final class RowUtil {

    /**
     * Instantiates a new row util.
     */
    private RowUtil() {

        // Nothing to be done Util class.
    }

    /** The Constant GROUP_BY_SEPARATOR. */
    private static final String GROUP_BY_SEPARATOR = "______";

    /**
     * This method performs the task of data normalization.
     *
     * @param inputDataset the input dataset
     * @param sortBy the sort by
     * @param order the order
     * @param groupByColumn the group by column
     * @return the dataset
     */
    public static Dataset<Row> groupAndMerge(final Dataset<Row> inputDataset,
                                             final String sortBy,
                                             final Order order,
                                             final String... groupByColumn) {

        Dataset<Row> mergedDataset = null;
        if (null != inputDataset && !ArrayUtils.isEmpty(groupByColumn)) {
            final Order sortOrder = order == null ? Order.ASC : order;
            final StructType schema = inputDataset.schema();

            final JavaPairRDD<Object, Row> mapToPair = inputDataset.javaRDD().mapToPair(row -> {
                final Object values[] = new Object[groupByColumn.length];
                for (int i = 0; i < groupByColumn.length; i++) {
                    values[i] = row.getAs(groupByColumn[i]);
                }
                return new Tuple2<>(StringUtils.concatenateUsingSeparator(GROUP_BY_SEPARATOR, values), row);
            });

            final JavaRDD<Row> cleanedRowRDD = mapToPair
                .reduceByKey((row1, row2) -> merge(row1, row2, sortOrder, sortBy, schema), 1100).values();
            final SparkSession session = ApplicationContextUtil.getBean(SparkSession.class);
            mergedDataset = session.createDataFrame(cleanedRowRDD, schema);
        }
        return mergedDataset;
    }

    /**
     * Sabre group and merge.
     *
     * @param tableName the table name
     * @param inputDataset the input dataset
     * @param sortBy the sort by
     * @param order the order
     * @param groupByColumn the group by column
     * @return the dataset
     */
    public static Dataset<Row> sabreGroupAndMerge(final String tableName,
                                                  final Dataset<Row> inputDataset,
                                                  final String sortBy,
                                                  final Order order,
                                                  final String... groupByColumn) {

        Dataset<Row> mergedDataset = null;
        if (null != inputDataset && !ArrayUtils.isEmpty(groupByColumn)) {
            final Order sortOrder = order == null ? Order.ASC : order;
            final StructType schema = inputDataset.schema();

            final JavaPairRDD<Object, Row> mapToPair = inputDataset.javaRDD().mapToPair(row -> {
                final Object values[] = new Object[groupByColumn.length];
                for (int i = 0; i < groupByColumn.length; i++) {
                    values[i] = row.getAs(groupByColumn[i]);
                }
                return new Tuple2<>(StringUtils.concatenateUsingSeparator(GROUP_BY_SEPARATOR, values), row);
            });

            if (tableName.equals(SabreTableNameColumnsEnum.ACSPAXDOCX.name())
                || tableName.equals(SabreTableNameColumnsEnum.ACSPAXFLIGHT.name())
                || tableName.equals(SabreTableNameColumnsEnum.RESPASSENGERDOC.name())
                || tableName.equals(SabreTableNameColumnsEnum.RESPASSENGER.name())) {

                final SabreTableNameColumnsEnum groupingTable = EnumUtils.getEnum(SabreTableNameColumnsEnum.class,
                    tableName.toUpperCase());
                final List<String> nameColumns = Arrays.asList(groupingTable.getNameColumns());

                final JavaPairRDD<Object, Iterable<Row>> maxColumnTopRowRDD = mapToPair.groupByKey();

                final JavaRDD<Row> cleanedRowRDD = maxColumnTopRowRDD.map(tuple -> {

                    final Iterable<Row> rows = tuple._2;

                    final Deque<Row> rowsQueue = new LinkedList<>();

                    int maxColumnFilled = 0;
                    int columnsFilled = 0;

                    for (final Row r : rows) {
                        for (final String fieldName : r.schema().fieldNames()) {

                            if (nameColumns.contains(fieldName) && null != r.getAs(fieldName)) {
                                columnsFilled++;
                            }
                        }

                        if (rowsQueue.size() == 0) {
                            rowsQueue.addFirst(r);
                            maxColumnFilled = columnsFilled;
                        } else {
                            if (columnsFilled > maxColumnFilled) {
                                maxColumnFilled = columnsFilled;
                                rowsQueue.addFirst(r);
                            } else {
                                rowsQueue.add(r);
                            }
                        }
                    }

                    return rowsQueue.stream().reduce((row1, row2) -> merge(row1, row2, sortOrder, sortBy, schema)).get();
                });

                final SparkSession session = ApplicationContextUtil.getBean(SparkSession.class);
                mergedDataset = session.createDataFrame(cleanedRowRDD, schema);
            } else {

                final JavaRDD<Row> cleanedRowRDD = mapToPair
                    .reduceByKey((row1, row2) -> merge(row1, row2, sortOrder, sortBy, schema), 1100).values();

                final SparkSession session = ApplicationContextUtil.getBean(SparkSession.class);
                mergedDataset = session.createDataFrame(cleanedRowRDD, schema);
            }

        }
        return mergedDataset;
    }

    /**
     * Group and merge by max occuring.
     *
     * @param inputDataset the input dataset
     * @param maxOccrringColumnName the max occrring column name
     * @param order the order
     * @param groupByColumn the group by column
     * @return the dataset
     */
    public static Dataset<Row> groupAndMergeByMaxOccuring(final Dataset<Row> inputDataset,
                                                          final String maxOccrringColumnName,
                                                          final Order order,
                                                          final String... groupByColumn) {

        Dataset<Row> mergedDataset = null;
        if (null != inputDataset && !ArrayUtils.isEmpty(groupByColumn)) {
            final StructType schema = inputDataset.schema();

            final JavaPairRDD<Object, Row> mapToPair = inputDataset.javaRDD().mapToPair(row -> {
                final Object values[] = new Object[groupByColumn.length];
                for (int i = 0; i < groupByColumn.length; i++) {
                    values[i] = row.getAs(groupByColumn[i]);
                }
                return new Tuple2<>(StringUtils.concatenateUsingSeparator(GROUP_BY_SEPARATOR, values), row);
            });
            final JavaRDD<Row> cleanedRowRDD = mapToPair.groupByKey().map(tuple -> {
                final List<Row> rows = new ArrayList<>(CollectionUtils.toList(tuple._2));
                return getRowWithMaxOccurringValue(rows, maxOccrringColumnName);
            });
            final SparkSession session = ApplicationContextUtil.getBean(SparkSession.class);
            mergedDataset = session.createDataFrame(cleanedRowRDD, schema);
        }

        return mergedDataset;

    }

    /**
     * Gets the row which contain max occurring value in list of row.
     *
     * @param rows the rows
     * @param maxOccrringColumnName the max occrring column name
     * @return the row which contain max occurring value
     */
    public static Row getRowWithMaxOccurringValue(final List<Row> rows, final String maxOccrringColumnName) {

        Row finalRow = null;
        if (rows.size() == 1) {
            finalRow = rows.get(0);
        } else {
            final Map<String, Integer> numberOccuringMap = new HashMap<>();
            int maxCount = 0;
            for (final Row row : rows) {
                final String columValue = row.getAs(maxOccrringColumnName);
                if (!StringUtils.isEmpty(columValue)) {
                    final Integer columnValueCount = numberOccuringMap.get(columValue);
                    final Integer updatedColumValue = null == columnValueCount ? 1 : columnValueCount.intValue() + 1;

                    numberOccuringMap.put(columValue, updatedColumValue);

                    if (updatedColumValue.intValue() > maxCount) {
                        maxCount = updatedColumValue.intValue();
                        finalRow = row;
                    }
                } else if (finalRow == null) {
                    finalRow = row;
                }
            }

        }
        return finalRow;
    }

    /**
     * This method performs the task of data normalization.
     *
     * @param inputDataset the input dataset
     * @param groupByColumn the group by column
     * @return the dataset
     */
    public static Dataset<Row> groupAndMerge(final Dataset<Row> inputDataset, final String... groupByColumn) {

        final Dataset<Row> mergedDataset = null;
        if (null != inputDataset && !ArrayUtils.isEmpty(groupByColumn)) {

            final JavaPairRDD<Object, Row> mapToPair = inputDataset.javaRDD().mapToPair(row -> {
                final Object values[] = new Object[groupByColumn.length];
                for (int i = 0; i < groupByColumn.length; i++) {
                    values[i] = row.getAs(groupByColumn[i]);
                }
                return new Tuple2<>(StringUtils.concatenateUsingSeparator(GROUP_BY_SEPARATOR, values), row);
            });

            final JavaPairRDD<Object, Iterable<Row>> pairs = mapToPair.groupByKey();
            final List<Tuple2<Object, Iterable<Row>>> rddRows = pairs.collect();

            final HashMap<Integer, HashMap<String, Integer>> columnValInfoHM = new HashMap<Integer, HashMap<String, Integer>>();

            for (int rowIdx = 0; rowIdx < rddRows.size(); ++rowIdx) {

                final Tuple2<Object, Iterable<Row>> rowIte = rddRows.get(rowIdx);
                // int minNullRow = 0;
                int preNullCounterRow = 0;
                // int minNullValRow = 0;

                do {

                    final Row row = rowIte._2().iterator().next();
                    final int size = row.size();
                    int nullCounterRow = 0;
                    int rowNum = 0;

                    for (int i = 0; i < size; i++) {
                        final Object object1 = row.get(i);
                        String colVal = "";
                        if (object1 == null) {
                            nullCounterRow = nullCounterRow + 1;
                            colVal = "NULL";

                        } else {
                            colVal = object1.toString().toUpperCase();
                        }
                        if (columnValInfoHM.containsKey(Integer.valueOf(i))) {

                            if (columnValInfoHM.get(Integer.valueOf(i)).containsKey(colVal)) {
                                final int oldCount = columnValInfoHM.get(Integer.valueOf(i)).get(colVal).intValue();
                                columnValInfoHM.get(Integer.valueOf(i)).replace(colVal, Integer.valueOf(oldCount + 1));

                            } else {
                                columnValInfoHM.get(Integer.valueOf(i)).put(colVal, Integer.valueOf(1));
                            }

                        } else {
                            final HashMap<String, Integer> valAndCountHM = new HashMap<String, Integer>();
                            valAndCountHM.put(colVal, Integer.valueOf(1));
                            columnValInfoHM.put(Integer.valueOf(i), valAndCountHM);
                        }

                    }
                    rowNum = rowNum + 1;
                    if (nullCounterRow == 0) {
                        break;
                    } else {
                        if (preNullCounterRow > nullCounterRow) {
                            preNullCounterRow = nullCounterRow;
                            // minNullValRow = rowNum;
                        }
                    }

                } while (rowIte._2().iterator().hasNext());// End of do while loop.

            } // end of for loop for rowIdx

        }
        return mergedDataset;
    }

    /**
     * Merge.
     *
     * @param row1 the row 1
     * @param row2 the row 2
     * @param orderBy the order by
     * @param sortByColumn the sort by column
     * @param schema the schema
     * @return the row
     */
    private static Row merge(final Row row1,
                             final Row row2,
                             final Order orderBy,
                             final String sortByColumn,
                             final StructType schema) {

        final Object col1 = row1.getAs(sortByColumn);
        final Object col2 = row2.getAs(sortByColumn);
        int comparison = compare(col1, col2);
        final int mul = orderBy == Order.ASC ? 1 : -1;
        comparison = comparison * mul;
        final Row primaryRow = comparison <= 0 ? row1 : row2;
        final Row secondaryRow = comparison <= 0 ? row2 : row1;
        final int totalCols = row1.length();
        final Object[] finalValues = new Object[totalCols];

        for (int i = 0; i < totalCols; i++) {
            final Object object1 = primaryRow.get(i);
            finalValues[i] = object1 == null ? secondaryRow.get(i) : object1;
        }
        return new GenericRowWithSchema(finalValues, schema);
    }

    /**
     * Compare.
     *
     * @param object1 the object 1
     * @param object2 the object 2
     * @return the int
     */
    private static int compare(final Object object1, final Object object2) {

        int comparison = 0;
        if (object1 == null) {
            comparison = -1;
        } else if (object2 == null) {
            comparison = 1;
        } else {
            if (object1 instanceof Comparable<?> && object2 instanceof Comparable<?>) {
                comparison = ((Comparable) (object1)).compareTo(object2);
            }
        }
        return comparison;
    }

    /**
     * Unique value count.
     *
     * @param inputRows the input rows
     * @param columns the columns
     * @return the long
     */
    public static long uniqueValueCount(final List<Row> inputRows, final String... columns) {

        long uniqueCount = 0;
        if (!CollectionUtils.isEmpty(inputRows) && null != columns) {
            uniqueCount = inputRows.stream().map(row -> {
                final StringBuilder builder = new StringBuilder();
                for (final String column : columns) {
                    final Object value = row.getAs(column);
                    if (value != null) {
                        builder.append(value.toString());
                    }
                }
                return builder.toString();
            }).distinct().count();
        }
        return uniqueCount;
    }

    /**
     * Select.
     *
     * @param inputDataset the input dataset
     * @param cols the cols
     * @return the dataset
     */
    public static Dataset<Row> select(final Dataset<Row> inputDataset, final String... cols) {

        Dataset<Row> outputDataset = null;
        if (null != inputDataset && !ArrayUtils.isEmpty(cols)) {
            final Column[] columnsTobeSelected = new Column[cols.length];
            int i = 0;
            for (final String col : cols) {
                columnsTobeSelected[i++] = inputDataset.col(col);
            }
            outputDataset = inputDataset.select(columnsTobeSelected);
        }
        return outputDataset;
    }

    /**
     * This method removes the rows from dataset having null value for any of column corresponding to column array.
     *
     * @param input the input
     * @param columns the columns
     * @return the dataset
     */
    public static Dataset<Row> removeRowsHavingNullColumnValue(final Dataset<Row> input, final String[] columns) {

        Dataset<Row> outputDataset = null;
        if (null != input) {
            final StructType schema = input.schema();
            final JavaRDD<Row> filteredRDD = input.toJavaRDD().filter(row -> {
                boolean invalidRow = false;
                for (final String column : columns) {
                    if (null == row.getAs(column)) {
                        invalidRow = true;
                        break;
                    }
                }
                return invalidRow;
            });
            final SparkSession session = ApplicationContextUtil.getBean(SparkSession.class);
            outputDataset = session.createDataFrame(filteredRDD, schema);
        }
        return outputDataset;
    }

    /**
     * Flatten.
     *
     * @param tuple the tuple
     * @return the object[]
     */
    public static Object[] flatten(final Object tuple) {

        return flattenTuple(tuple, new ArrayList<>()).toArray();
    }

    /**
     * Flatten tuple.
     *
     * @param object the object
     * @param keyValuepairs the key valuepairs
     * @return the list
     */
    private static List<Object> flattenTuple(final Object object, final List<Object> keyValuepairs) {

        if (object instanceof org.apache.spark.api.java.Optional) {
            final org.apache.spark.api.java.Optional<?> optional = (org.apache.spark.api.java.Optional<?>) object;
            if (optional.isPresent()) {
                flattenTuple(optional.get(), keyValuepairs);
            } else {
                flattenTuple(null, keyValuepairs);
            }
        } else if ((object instanceof Tuple2<?, ?>)) {
            flattenTuple(((Tuple2<?, ?>) object)._1, keyValuepairs);
            flattenTuple(((Tuple2<?, ?>) object)._2, keyValuepairs);
        } else {
            keyValuepairs.add(object);
        }
        return keyValuepairs;
    }

    /**
     * Map rows on keys.
     *
     * @param inputDataset inputDataset
     * @param keyColumns String[]
     * @return JavaPairRDD
     */
    public static JavaPairRDD<String, Iterable<Row>> mapRowsOnKeys(final Dataset<Row> inputDataset, final String... keyColumns) {

        return inputDataset.toJavaRDD().groupBy(v1 -> {
            final StringBuilder response = new StringBuilder("");
            for (final String keyColumn : keyColumns) {
                response.append(v1.getAs(keyColumn).toString().trim());
            }
            return response.toString();
        });
    }

    /**
     * Gets the latest row.
     *
     * @param inputRDD the input RDD
     * @param comparisonColumn the comparison column
     * @return the latest row
     */
    public static JavaRDD<Row> getLatestRow(final JavaPairRDD<String, Iterable<Row>> inputRDD, final String comparisonColumn) {

        return inputRDD.map(v1 -> {
            final Iterable<Row> rowCollection = v1._2;
            final List<Row> rowList = new ArrayList<>();
            for (final Row row : rowCollection) {
                rowList.add(row);
            }
            rowList.sort((r1, r2) -> -(r1.getAs(comparisonColumn).toString().compareTo(r2.getAs(comparisonColumn))));
            return rowList.get(0);
        });

    }

    /**
     * Gets the row based on the provided schema. This method also assumes that column names are unique i.e. for multiple column
     * families there will not be columns with same name, and if at all exist they will carry the same value.
     * 
     * @param result {@link Result} the output retrieved from HBase repo.
     * @param schema {@link StructType} stored schema of the content present
     * @return {@link Row} row that can be queried for further processing.
     */
    public static Row getRow(final Result result, final StructType schema) {

        Row row = null;

        if (null != result && null != schema) {
            final Map<String, byte[]> rowValuesMap = new HashMap<>();
            final NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
            if (map != null) {
                final Collection<NavigableMap<byte[], byte[]>> familyCollection = map.values();
                for (final NavigableMap<byte[], byte[]> family : familyCollection) {
                    final Set<Entry<byte[], byte[]>> entrySet = family.entrySet();
                    entrySet.forEach(entry -> {
                        final String colName = Bytes.toString(entry.getKey());
                        rowValuesMap.put(colName, entry.getValue());
                    });
                }
            }
            final StructField[] fields = schema.fields();
            final Object[] values = new Object[fields.length];
            int i = 0;
            for (final StructField field : fields) {
                final DataType dataType = field.dataType();
                final String columnName = field.name();
                final byte[] value = rowValuesMap.get(columnName);
                Object valueObject = null;
                if (null != value) {
                    valueObject = getValueObject(dataType, value);
                }
                values[i++] = valueObject;
            }
            row = new GenericRowWithSchema(values, schema);
        }
        return row;
    }

    /**
     * Gets the value object.
     *
     * @param dataType the data type
     * @param value the value
     * @return the value object
     */
    private static Object getValueObject(final DataType dataType, final byte[] value) {

        // TODO correct the mistake here..
        Object valueObject = null;
        if (dataType == DataTypes.DateType) {
            valueObject = Bytes.toObject(value);
        } else if (dataType == DataTypes.FloatType) {
            valueObject = Bytes.toFloat(value);
        } else if (dataType == DataTypes.DoubleType) {
            valueObject = Bytes.toDouble(value);
        } else if (dataType == DataTypes.StringType) {
            valueObject = Bytes.toString(value);
        } else if (dataType == DataTypes.IntegerType) {
            valueObject = Bytes.toInt(value);
        } else if (dataType == DataTypes.LongType) {
            valueObject = Bytes.toLong(value);
        } else if (dataType == DataTypes.TimestampType) {
            valueObject = Bytes.toObject(value);
        }
        return valueObject;
    }

    /**
     * Gets the column data.
     *
     * @param <T> the generic type
     * @param resultList the result list
     * @param columnFamily the column family
     * @param columnName the column name
     * @return the column data
     */
    public static <T> List<T> getColumnData(final List<Result> resultList, final String columnFamily, final String columnName) {

        final List<T> outPutColumnValueList = new ArrayList<>();
        String inputColumnFamily = columnFamily;
        if (StringUtils.isEmpty(inputColumnFamily)) {
            inputColumnFamily = JobConstants.DEFAULT_COLUMN_FAMILY_NAME;
        }
        if (!(CollectionUtils.isEmpty(resultList) || StringUtils.isEmpty(columnName) || StringUtils.isEmpty(inputColumnFamily))) {
            for (final Result result : resultList) {
                final Cell columnLatestCell = result.getColumnLatestCell(inputColumnFamily.getBytes(), columnName.getBytes());
                if (null != columnLatestCell) {
                    outPutColumnValueList.add(Bytes.toObject(CellUtil.cloneValue(columnLatestCell)));
                }
            }
        }
        return outPutColumnValueList;
    }

    /**
     * Gets the rows.
     *
     * @param results the results
     * @param schema the schema
     * @return the rows
     */
    public static List<Row> getRows(final List<Result> results, final StructType schema) {

        List<Row> rows = null;
        if (!CollectionUtils.isEmpty(results) && null != schema) {
            rows = new ArrayList<>();
            for (final Result result : results) {
                rows.add(getRow(result, schema));
            }
        }
        return rows;
    }

    /**
     * Gets the values.
     *
     * @param row the row
     * @param schema the schema
     * @return the values
     */
    public static Object[] getValues(final Row row, final StructType schema) {

        Object[] finalValues = null;
        if (null != schema && null != row) {
            final StructField[] fields = schema.fields();
            final int totalFields = fields.length;
            finalValues = new Object[totalFields];

            for (int i = 0; i < totalFields; i++) {
                finalValues[i] = row.get(i);
            }
        }
        return finalValues;
    }

    /**
     * Adds the values.
     *
     * @param row the row
     * @param schema the schema
     * @param values the values
     * @return the row
     */
    public static Row addValues(final Row row, final StructType schema, final Object... values) {

        Row finalRow = null;
        if (null != row && !ArrayUtils.isEmpty(values)) {
            final int totalColumns = row.size();
            final Object[] finalValues = new Object[values.length + totalColumns];
            int i = 0;
            for (int j = 0; j < totalColumns; j++) {
                finalValues[i++] = row.get(j);
            }
            for (final Object value : values) {
                finalValues[i++] = value;
            }
            finalRow = new GenericRowWithSchema(finalValues, schema);
        }
        return finalRow;
    }

    /**
     * This method returns the value that is occuring maximum number of times of the given column in given list of rows.
     *
     * @param <T> the generic type
     * @param rows the rows
     * @param columnName the column name
     * @return the t
     */
    public static <T> T maxOccuringValue(final List<Row> rows, final String columnName) {

        if (!CollectionUtils.isEmpty(rows) && !StringUtils.isEmpty(columnName) && rows.size() == 1) {
            return rows.get(0).getAs(columnName);
        } else {
            final Pair<T, Double> result = maxOccuringValuePercentage(rows, columnName);
            return result == null ? null : result.getKey();
        }

    }

    /**
     * Max occuring value percentage.
     *
     * @param <T> the generic type
     * @param rows the rows
     * @param columnName the column name
     * @return the pair
     */
    @SuppressWarnings("unchecked")
    public static <T> Pair<T, Double> maxOccuringValuePercentage(final List<Row> rows, final String columnName) {

        Pair<T, Double> maxPair = null;
        if (!CollectionUtils.isEmpty(rows) && !StringUtils.isEmpty(columnName)) {
            final Set<Entry<T, Long>> entrySet = rows.stream().filter(row -> row.getAs(columnName) != null)
                .collect(Collectors.toMap(row -> (T) row.getAs(columnName), row -> 1L, (o, n) -> o + n)).entrySet();

            if (!CollectionUtils.isEmpty(entrySet)) {
                if (entrySet.size() == 1) {
                    final Long sum = entrySet.stream().mapToLong(Entry::getValue).sum();
                    final Entry<T, Long> firstEntry = entrySet.iterator().next();
                    maxPair = new Pair<T, Double>(firstEntry.getKey(), ((double) firstEntry.getValue() / sum) * 100.00);
                } else {
                    final Entry<T, Long> max = Collections.max(entrySet,
                        (val1, val2) -> Long.compare(val1.getValue(), val2.getValue()));
                    final Long sum = entrySet.stream().mapToLong(Entry::getValue).sum();
                    maxPair = new Pair<>(max.getKey(), ((double) max.getValue() / sum) * 100.00);
                }
            }
        }
        return maxPair;
    }

    /**
     * Util method to return the maximum value for a column in the group of rows.
     * 
     * @param inputRows inputRows
     * @param columnName columnName
     * @return maxValue maxValue
     */
    public static Long getMaxColumnValue(final Iterable<Row> inputRows, final String columnName) {

        final Optional<Long> maxValue = CollectionUtils.toList(inputRows).stream().map(row -> (Long) row.getAs(columnName))
            .max(Long::compare);
        return maxValue.isPresent() ? maxValue.get() : null;
    }

    /**
     * This method merge the <code> rows </code> into one <code> row </code> of schema <code> schema </code> using
     * <code> fieldsMap </code>.
     * <p>
     * The fields map constains the key-value pair of columnName and the index of rows from which the given columnName is need to be
     * picked. If required column from schema is not present in <code> fieldsMap</code>, <i> null </i> is assigned to that column.
     * </p>
     *
     * @param fieldsMap the fields map
     * @param schema the schema
     * @param rows the rows
     * @return the row
     */
    public static Row mergeRows(final Map<String, Integer> fieldsMap, final StructType schema, final Row... rows) {

        Row row = null;
        if (!CollectionUtils.isEmpty(fieldsMap) && schema != null && rows != null) {
            final Object[] values = new Object[schema.fields().length];
            int i = 0;
            for (final StructField field : schema.fields()) {
                final String fieldName = field.name();
                final Integer value = fieldsMap.get(fieldName);
                if (value != null && value >= 0 && value < rows.length) {
                    final Row row2 = rows[value];
                    if (null != row2) {
                        values[i] = row2.getAs(fieldName);
                        if (field.dataType() == DataTypes.StringType && values[i] != null) {
                            values[i] = values[i].toString();
                        }
                    }
                } else {
                    values[i] = null;
                }
                i++;
            }
            row = new GenericRowWithSchema(values, schema);
        }
        return row;
    }

    /**
     * This method merge the <code> rows </code> into one <code> row </code> of schema <code> schema </code> using
     * <code> fieldsMap </code>.
     * <p>
     * The fields map constains the key-value pair of columnName and the index of rows from which the given columnName is need to be
     * picked. If required column from schema is not present in <code> fieldsMap</code>, <i> null </i> is assigned to that column.
     * </p>
     *
     * @param fieldsMap the fields map
     * @param schema the schema
     * @param row1 the row 1
     * @param row2 the row 2
     * @return the row
     */
    public static Row mergeRows(final Map<String, Integer> fieldsMap, final StructType schema, final Row row1, final Row row2) {

        Row row = null;
        if (!CollectionUtils.isEmpty(fieldsMap) && schema != null) {
            final Object[] values = new Object[schema.fields().length];
            int i = 0;
            for (final StructField field : schema.fields()) {
                final String fieldName = field.name();
                final Integer value = fieldsMap.get(fieldName);
                Row temp = null;
                if (value != null) {
                    if (value == 0) {
                        temp = row1;
                    } else {
                        temp = row2;
                    }
                }
                if (temp != null) {
                    values[i] = temp.getAs(fieldName);
                } else {
                    values[i] = null;
                }
                i++;
            }
            row = new GenericRowWithSchema(values, schema);
        }
        return row;
    }

    /**
     * This method generates the row from <code> map </code> and <code> scheam </code>.
     *
     * @param map contains the columnName of row as key and value of that column as its value.
     * @param schema is the schema of the row that need to be created.
     * @return the row
     */
    public static Row generateRow(final Map<String, Object> map, final StructType schema) {

        Row row = null;
        if (schema != null && !CollectionUtils.isEmpty(map)) {
            final String[] fields = schema.fieldNames();
            final Object[] values = new Object[fields.length];
            int index = 0;
            for (final String field : fields) {
                values[index++] = map.get(field);
            }
            row = new GenericRowWithSchema(values, schema);
        }
        return row;
    }

    /**
     * Replace column.
     *
     * @param row the row
     * @param column the column
     * @param value the value
     * @return the row
     */
    public static Row replaceColumn(final Row row, final String column, final Object value) {

        Row finalRow = row;
        if (!StringUtils.isEmpty(column)) {
            final StructType schema = row.schema();
            final StructField[] schemaFields = schema.fields();
            final Object[] values = new Object[schemaFields.length];
            int i = 0;
            for (final StructField field : schemaFields) {
                if (field.name().equalsIgnoreCase(column)) {
                    values[i] = value;
                } else {
                    values[i] = row.getAs(i);
                }
                i++;
            }
            finalRow = new GenericRowWithSchema(values, schema);
        }
        return finalRow;
    }

    /**
     * To schema.
     *
     * @param row the row
     * @param structType the struct type
     * @return the row
     */
    public static Row toSchema(final Row row, final StructType structType) {

        Row finalRow = null;
        if (null != row && null != structType) {
            final Set<String> rowFieldNames = ArrayUtils.toSet(row.schema().fieldNames());
            int i = 0;
            final String[] totalFields = structType.fieldNames();
            final Object values[] = new Object[totalFields.length];
            for (final String fieldName : totalFields) {
                if (rowFieldNames.contains(fieldName)) {
                    values[i] = row.getAs(fieldName);
                } else {
                    values[i] = null;
                }
                i++;
            }
            finalRow = new GenericRowWithSchema(values, structType);
        }
        return finalRow;
    }

    /**
     * Normalize table.
     *
     * @param tableName the table name
     * @param separator - by which columns concatenate
     * @param key - key on the basis of rows merged
     * @param inputDataset the input dataset
     * @param isDuplicateAllowedBySeprator the is duplicate allowed by seprator
     * @param tobeMergedColumnNames the tobe merged column names
     * @return the dataset
     */
    public static Dataset<Row> normalizeTable(final String tableName,
                                              final String separator,
                                              final String key,
                                              final Dataset<Row> inputDataset,
                                              final Boolean isDuplicateAllowedBySeprator,
                                              final String... tobeMergedColumnNames) {

        final StructType schema = inputDataset.schema();
        final JavaPairRDD<String, Row> mapToPair = inputDataset.toJavaRDD()
            .mapToPair(row -> new Tuple2<String, Row>(row.getAs(key), row));
        final JavaRDD<Row> map = mapToPair.groupByKey().map(tuple -> {
            final List<Row> rows = new ArrayList<>(CollectionUtils.toList(tuple._2));
            Row finalRow = null;
            if (rows.size() == 1) {
                finalRow = rows.get(0);
            } else {
                finalRow = rows.get(0);
                for (int i = 1; i < rows.size(); i++) {
                    finalRow = merge(rows.get(i), finalRow, separator, schema, isDuplicateAllowedBySeprator, tobeMergedColumnNames);
                }
            }
            return finalRow;
        });
        return inputDataset.sparkSession().createDataFrame(map, schema);
    }
    
    /**
     * Normalize table.
     *
     * @param tableName the table name
     * @param separator - by which columns concatenate
     * @param key - key on the basis of rows merged
     * @param inputDataset the input dataset
     * @param tobeMergedColumnNames the tobe merged column names
     * @return the dataset
     */
    public static Dataset<Row> normalizeTable(final String tableName,
                                              final String separator,
                                              final String key,
                                              final Dataset<Row> inputDataset,
                                              final Map<String, Boolean> tobeMergedColumnNames) {

        final StructType schema = inputDataset.schema();
        final JavaPairRDD<String, Row> mapToPair = inputDataset.toJavaRDD()
            .mapToPair(row -> new Tuple2<String, Row>(row.getAs(key), row));
        final JavaRDD<Row> map = mapToPair.groupByKey().map(tuple -> {
            final List<Row> rows = new ArrayList<>(CollectionUtils.toList(tuple._2));
            Row finalRow = null;
            if (rows.size() == 1) {
                finalRow = rows.get(0);
            } else {
                finalRow = rows.get(0);
                for (int i = 1; i < rows.size(); i++) {
                    finalRow = merge(rows.get(i), finalRow, separator, schema, tobeMergedColumnNames);
                }
            }
            return finalRow;
        });
        return inputDataset.sparkSession().createDataFrame(map, schema);
    }

    /**
     * Combine rows.
     *
     * @param row1 the row 1
     * @param row2 the row 2
     * @return the row
     */
    public static Row combineRows(final Row row1, final Row row2) {

        List<StructField> finalSchemaFields = null;
        Object[] values = null;
        if (null != row1 && null != row2) {
            finalSchemaFields = new ArrayList<>();
            finalSchemaFields.addAll(Arrays.asList(row1.schema().fields()));
            finalSchemaFields.addAll(Arrays.asList(row2.schema().fields()));
            values = new Object[finalSchemaFields.size()];
            for (int i = 0; i < row1.length(); i++) {
                values[i] = row1.getAs(i);
            }
            int j = 0;
            for (int i = row1.length(); i < finalSchemaFields.size(); i++) {
                values[i] = row2.getAs(j);
                j++;
            }
        }
        return new GenericRowWithSchema(values, DataTypes.createStructType(finalSchemaFields));
    }

    /**
     * Creates the new row.
     *
     * @param fieldNames the field names
     * @param row the row
     * @param fieldName the field name
     * @param schema the schema
     * @return the row
     */
    public static Row createNewRowWithNullColumn(final String[] fieldNames,
                                                 final Row row,
                                                 final String fieldName,
                                                 final StructType schema) {

        final Object[] newRowValues = new Object[fieldNames.length];
        int index = 0;
        for (final String field : fieldNames) {
            if (fieldName.equalsIgnoreCase(field)) {
                newRowValues[index] = null;
            } else {
                newRowValues[index] = row.getAs(field);
            }
            index++;
        }
        return new GenericRowWithSchema(newRowValues, schema);
    }

    /**
     * This method is responsible to trim the length of the desired column in the row to the specific length. If input length is
     * more than the desired length then last 'n' characters are taken from the column. If input length is less than the desired
     * length then 'm' 0's are prepended to the column value to make its length equal to the desired length.
     *
     * @param fieldNames {@link String[]} - names of all the fields in the schema
     * @param inputRow {@link Row} - input row that needs to be modified
     * @param columnName (@link String} - impacted column
     * @param schema {@link StructType} - Schema of the resultant row
     * @param columnLength int - length of the column
     * @return {@link Row}
     */
    public static Row correctLengthColumn(final String[] fieldNames,
                                          final Row inputRow,
                                          final String columnName,
                                          final StructType schema,
                                          final int columnLength) {

        final Object[] values = new Object[fieldNames.length];

        int index = 0;

        for (final String fieldName : fieldNames) {
            if (fieldName.equalsIgnoreCase(columnName)) {

                final String val = inputRow.getAs(fieldName);

                if (null != val) {
                    values[index] = RowUtil.cleanColumn(val.trim(), columnLength);
                } else {
                    values[index] = inputRow.getAs(fieldName);
                }
            } else {
                values[index] = inputRow.getAs(fieldName);
            }
            index++;
        }

        return new GenericRowWithSchema(values, schema);
    }

    /**
     * Clean column.
     *
     * @param input {@link String} - String to be modified
     * @param columnLength int - length of the desired response
     * @return modifiedString {@link String}
     */
    public static String cleanColumn(final String input, final int columnLength) {

        String cleanedValue = null;

        if (input.length() == columnLength) {
            cleanedValue = input.trim();
        } else if (input.length() < columnLength) {
            cleanedValue = RowUtil.padLeadingZeros(input, columnLength);
        } else {
            cleanedValue = RowUtil.getLastNCharacters(input, columnLength);
        }

        return cleanedValue;
    }

    /**
     * Pad leading zeros.
     *
     * @param input {@link String} - String to be modified
     * @param columnLength int - length of the desired response
     * @return modifiedString {@link String}
     */
    public static String padLeadingZeros(final String input, final int columnLength) {

        return Strings.padStart(input, columnLength, '0');
    }

    /**
     * Gets the last N characters.
     *
     * @param input {@link String} - String to be modified
     * @param lengthFromRight the length from right
     * @return modifiedString {@link String}
     */
    public static String getLastNCharacters(final String input, final int lengthFromRight) {

        return input.substring(Math.max(0, input.length() - lengthFromRight));
    }

    /**
     * Merge.
     *
     * @param row1 {@link Row}
     * @param row2 {@link Row}
     * @param separator {@link String}
     * @param schema {@link StructType}
     * @param isDuplicateAllowedBySeprator {@link Boolean}
     * @param tobeMergedColumnNames {@link String[]}
     * @return the row
     */
    private static Row merge(final Row row1,
                             final Row row2,
                             final String separator,
                             final StructType schema,
                             final Boolean isDuplicateAllowedBySeprator,
                             final String[] tobeMergedColumnNames) {

        final String[] schemaFields = schema.fieldNames();
        final Object[] values = new Object[schemaFields.length];
        int index = 0;
        final List<String> columns = Arrays.asList(tobeMergedColumnNames);
        for (final String field : schemaFields) {
            if (columns.contains(field)) {
                if (null != row1.getAs(field) && null != row2.getAs(field) && row2.getAs(field) instanceof String) {
                    if (isDuplicateAllowedBySeprator || !row2.getAs(field).toString().contains(row1.getAs(field).toString())) {
                        values[index] = StringUtils.concatenateUsingSeparator(separator, row2.getAs(field), row1.getAs(field));
                    } else {
                        values[index] = row2.getAs(field);
                    }
                } else {
                    values[index] = (null != row1.getAs(field) ? row1.getAs(field) : row2.getAs(field));
                }
            } else {
                values[index] = (null != row1.getAs(field) ? row1.getAs(field) : row2.getAs(field));
            }
            index++;
        }
        return new GenericRowWithSchema(values, schema);
    }
    
    /**
     * Merge.
     *
     * @param row1 {@link Row}
     * @param row2 {@link Row}
     * @param separator {@link String}
     * @param schema {@link StructType}
     * @param tobeMergedColumnNames {@link Map}
     * @return the row
     */
    private static Row merge(final Row row1,
                             final Row row2,
                             final String separator,
                             final StructType schema,
                             final Map<String, Boolean> tobeMergedColumnNames) {

        final String[] schemaFields = schema.fieldNames();
        final Object[] values = new Object[schemaFields.length];
        int index = 0;
        for (final String field : schemaFields) {
            if (tobeMergedColumnNames.containsKey(field)) {
                if (null != row1.getAs(field) && null != row2.getAs(field) && row2.getAs(field) instanceof String) {
                    if (tobeMergedColumnNames.get(field) || !row2.getAs(field).toString().contains(row1.getAs(field).toString())) {
                        values[index] = StringUtils.concatenateUsingSeparator(separator, row2.getAs(field), row1.getAs(field));
                    } else {
                        values[index] = row2.getAs(field);
                    }
                } else {
                    values[index] = (null != row1.getAs(field) ? row1.getAs(field) : row2.getAs(field));
                }
            } else {
                values[index] = (null != row1.getAs(field) ? row1.getAs(field) : row2.getAs(field));
            }
            index++;
        }
        return new GenericRowWithSchema(values, schema);
    }

}
