package com.shuidihuzhu.transfer.model;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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

    /**
     * Getter method for property table.
     *
     * @return property value of table
     **/
    public String getTable() {
        return table;
    }

    /**
     * Setter method for property table.
     *
     * @param table value to be assigned to property table
     **/
    public SinkRecord setTable(String table) {
        this.table = table;
        return this;
    }

    /**
     * Getter method for property rowKey.
     *
     * @return property value of rowKey
     **/
    public String getRowKey() {
        return rowKey;
    }

    /**
     * Setter method for property rowKey.
     *
     * @param rowKey value to be assigned to property rowKey
     **/
    public SinkRecord setRowKey(String rowKey) {
        this.rowKey = rowKey;
        return this;
    }

    /**
     * Getter method for property family.
     *
     * @return property value of family
     **/
    public String getFamily() {
        return family;
    }

    /**
     * Setter method for property family.
     *
     * @param family value to be assigned to property family
     **/
    public SinkRecord setFamily(String family) {
        this.family = family;
        return this;
    }

    /**
     * Getter method for property qualifier.
     *
     * @return property value of qualifier
     **/
    public String getQualifier() {
        return qualifier;
    }

    /**
     * Setter method for property qualifier.
     *
     * @param qualifier value to be assigned to property qualifier
     **/
    public SinkRecord setQualifier(String qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    /**
     * Getter method for property value.
     *
     * @return property value of value
     **/
    public String getValue() {
        return value;
    }

    /**
     * Setter method for property value.
     *
     * @param value value to be assigned to property value
     **/
    public SinkRecord setValue(String value) {
        this.value = value;
        return this;
    }

    /**
     * Getter method for property timestamp.
     *
     * @return property value of timestamp
     **/
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Setter method for property timestamp.
     *
     * @param timestamp value to be assigned to property timestamp
     **/
    public SinkRecord setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Getter method for property payload.
     *
     * @return property value of payload
     **/
    public String getPayload() {
        return payload;
    }

    /**
     * Setter method for property payload.
     *
     * @param payload value to be assigned to property payload
     **/
    public SinkRecord setPayload(String payload) {
        this.payload = payload;
        return this;
    }

    /**
     * Getter method for property column.
     *
     * @return property value of column
     **/
    public String getColumn() {
        return column;
    }

    /**
     * Setter method for property column.
     *
     * @param column value to be assigned to property column
     **/
    public SinkRecord setColumn(String column) {
        this.column = column;
        return this;
    }

    /**
     * Getter method for property keyValues.
     *
     * @return property value of keyValues
     **/
    public Map<String, Object> getKeyValues() {
        return keyValues;
    }

    /**
     * Setter method for property keyValues.
     *
     * @param keyValues value to be assigned to property keyValues
     **/
    public SinkRecord setKeyValues(Map<String, Object> keyValues) {
        this.keyValues = keyValues;
        return this;
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
        return JSON.toJSONString(record);
    }

    public static SinkRecord parseFromConsumerRecord(ConsumerRecord<String, String> consumerRecord) {

        if (consumerRecord == null) {
            return null;
        }
        return JSON.parseObject(consumerRecord.value(), SinkRecord.class);
    }
}
