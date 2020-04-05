package com.ngdata.wal.sink;

import com.ngdata.wal.model.SinkRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author duchao
 * @version : Sink.java, v 0.1 2019-12-01 18:53 duchao Exp $
 */
public interface Sink {
    /**
     * 解析为 KafKa ProducerRecord
     */
    ProducerRecord<String, String> parse(SinkRecord sinkRecord);

    /**
     * 获取HBase 表名
     */
    String getTableName();
}
