package com.ngdata.wal.sink.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.ngdata.wal.model.SinkRecord;
import com.ngdata.wal.sink.Sink;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author duchao
 * @version : SinkRecordProxy.java, v 0.1 2019-11-27 22:51 duchao Exp $
 */
@Slf4j
public abstract class SinkRecordProxy implements Sink {
    private static final String ID ="id";

    protected String checkRowKey(SinkRecord sinkRecord) {
        String rowKey = sinkRecord.getRowKey();
        if (!rowKey.contains(":") || rowKey.split(":").length != 2) {
            String message = "Discarded --- rowKey=" + rowKey;
            log.error(message);
            return null;
        }
        return rowKey;
    }

    protected void parseKeyValues(SinkRecord sinkRecord, String id) {
        Map<String, Object> keyValues = Maps.newHashMap();
        Map<String, Object> recordKeyValues = sinkRecord.getKeyValues();
        for (Map.Entry<String, Object> entry : recordKeyValues.entrySet()) {
            String key = entry.getKey();
            keyValues.put(key, entry.getValue());
        }
        keyValues.put(ID,id);
        sinkRecord.setKeyValues(keyValues);
    }
}