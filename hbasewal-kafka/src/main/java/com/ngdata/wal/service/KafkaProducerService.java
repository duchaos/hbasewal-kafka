package com.ngdata.wal.service;

import com.ngdata.wal.ha.FailMsgHandler;
import com.ngdata.wal.model.SinkRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author duchao
 */
@Slf4j
@Component
public class KafkaProducerService {
    private final Producer<String, String> kafkaProducer;
    private final FailMsgHandler msgHandler;

    public KafkaProducerService(Producer<String, String> kafkaProducer, FailMsgHandler msgHandler) {
        this.kafkaProducer = kafkaProducer;
        this.msgHandler = msgHandler;
    }

    public void send(SinkRecord sinkRecord, ProducerRecord<String, String> producerRecord){
        // 发送消息
        if (producerRecord == null) {
            return;
        }
        // 若有未重试发送的消息则阻塞
        try {
            while (msgHandler.haveFail(producerRecord.topic())) {
                msgHandler.doRetrySend(producerRecord.topic());
            }
        } catch (Exception e) {
            log.error("重试失败",e);
        }
        // 异步发送
        log.debug("topic:{},partition:{},key:{}", producerRecord.topic(), producerRecord.partition(), producerRecord.key());
        kafkaProducer.send(producerRecord, new ProducerCallback(sinkRecord, producerRecord, msgHandler));
    }


}
