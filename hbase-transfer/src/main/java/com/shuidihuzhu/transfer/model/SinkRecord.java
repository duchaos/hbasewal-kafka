package com.shuidihuzhu.transfer.model;

import java.util.Map;

/**
 * Created by sunfu on 2018/12/29.
 */
public class SinkRecord {

    // 表名
    private String table;
    // 行键
    private String rowKey;
    // 列族
    private String family;
    // 列名
    private String qualifier;
    // 值
    private String value;
    // 时间戳
    private long timestamp;
    // 什么鬼？
    private String payload;

    private String column;

    private Map<String, Object> keyValues;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Map<String, Object> getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Map<String, Object> keyValues) {
        this.keyValues = keyValues;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "table='" + table + '\'' +
                ", rowKey='" + rowKey + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                ", payload='" + payload + '\'' +
                ", column='" + column + '\'' +
                ", keyValues=" + keyValues +
                '}';
    }

    public static String getText(SinkRecord record) {
        return record.getTable() + "|" +
                record.getRowKey() + "|" +
                //record.getFamily() + "|" +
                //record.getQualifier() + "|" +
                record.getColumn() + "|" +
                record.getValue() + "|" +
                record.getTimestamp() + "|" +
                record.getPayload();
    }
}
