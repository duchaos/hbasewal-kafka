package com.ngdata.wal.sink.hbase;

import com.alibaba.fastjson.JSON;
import com.ngdata.wal.model.SinkRecord;
import com.ngdata.wal.sink.kafka.SinkRecordProxy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author duchao
 */
@Slf4j
@Component
public class Demo1Sink extends SinkRecordProxy {
    @Value("${hbasewal-kafka.hbase.demo1.topic}")
    private String topic;
    @Value("${hbasewal-kafka.hbase.demo1.table}")
    private String demoTable;

    @Override
    public ProducerRecord<String, String> parse(SinkRecord sinkRecord) {
        String id = sinkRecord.getRowKey();
        parseKeyValues(sinkRecord, id);
        String value = JSON.toJSONString(sinkRecord);
        log.debug("demo 详情:{}", value);
        return new ProducerRecord<>(topic, id, value);
    }
    @Override
    public String getTableName() {
        return demoTable;
    }
}