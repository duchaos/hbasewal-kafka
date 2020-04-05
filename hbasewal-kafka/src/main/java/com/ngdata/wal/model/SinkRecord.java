package com.ngdata.wal.model;


import lombok.Data;

import java.util.Map;

@Data
public class SinkRecord {

    /**
     * 表名
     */
    private String table;

    /**
     * 行键
     */
    private String rowKey;

    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 是否是删除操作
     */
    private boolean isDelete;

    /**
     * 原始的family:column 作为key，值为value
     */
    private Map<String, Object> keyValues;


}
