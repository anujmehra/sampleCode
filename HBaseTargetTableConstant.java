package com.hbase.poc.service;


public enum HBaseTargetTableConstant {

    My_Test_Table_1("My_Test_Table_1", "cf1", 10),

    My_Test_Table_2("My_Test_Table_2", "cf1", 10);
    
    private String hbaseTableName;

    private String columnFamily;

    private int splits;

    private String[] key;

    private HBaseTargetTableConstant(final String hbaseTableName, final String columnFamily, final int splits) {

        this.hbaseTableName = hbaseTableName;
        this.columnFamily = columnFamily;
        this.splits = splits;
    }

    public String getHbaseTableName() {

        return hbaseTableName;
    }

    public String[] getKey() {

        return key;
    }

    public String getColumnFamily() {

        return columnFamily;
    }

    public static HBaseTargetTableConstant getTable(String inputTable) {

        HBaseTargetTableConstant outputTable = null;
        if (null != inputTable && inputTable.length() >0) {
            for (HBaseTargetTableConstant table : HBaseTargetTableConstant.values()) {
                if (table.name().equalsIgnoreCase(inputTable.toUpperCase())) {
                    outputTable = table;
                }
            }
        }
        return outputTable;
    }

    public int getSplits() {
    
        return splits;
    }
    
}
