package com.ngdata.wal.service;

import com.ngdata.wal.ha.FailMsgHandler;
import com.ngdata.wal.model.SinkRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author duchao
 */
@Slf4j
public class ProducerCallback implements Callback {
    private final SinkRecord sinkRecord;
    private final ProducerRecord producerRecord;
    @Autowired
    private FailMsgHandler msgHandler;

    public ProducerCallback(SinkRecord sinkRecord, ProducerRecord producerRecord, FailMsgHandler msgHandler) {
        this(sinkRecord, producerRecord);
        this.msgHandler = msgHandler;
    }

    public ProducerCallback(SinkRecord sinkRecord, ProducerRecord producerRecord) {
        this.sinkRecord = sinkRecord;
        this.producerRecord = producerRecord;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            long eventTms = sinkRecord.getTimestamp();
            long gapTime = System.currentTimeMillis() - eventTms;
            log.error("KafkaProducer Send Error: topic = " + metadata.topic()
                    + " table = " + sinkRecord.getTable()
                    + ", rowkey = " + sinkRecord.getRowKey()
                    + ", timestamp: " + eventTms
                    + ", elapsedTime: "
                    + gapTime + ", reason: " + e);
            msgHandler.append(metadata.topic(), producerRecord.key().toString(), producerRecord.toString());
        }
    }
}