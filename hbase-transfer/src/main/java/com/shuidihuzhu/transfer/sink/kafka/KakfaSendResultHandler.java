package com.shuidihuzhu.transfer.sink.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Service;

/**
 * @author duchao
 * @version : KakfaSendResultHandler.java, v 0.1 2019-06-21 11:04 duchao Exp $
 */
@Slf4j
@Service
public class KakfaSendResultHandler implements ProducerListener {

    @Override
    public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
//        log.info("Message send success : " + producerRecord.toString());
    }

    @Override
    public void onError(ProducerRecord producerRecord, Exception exception) {
//        TODO 进行重试发送
        log.error("[发送失败] --- topic= " + producerRecord.topic() + ",失败原因：" + exception.getMessage());

    }
}