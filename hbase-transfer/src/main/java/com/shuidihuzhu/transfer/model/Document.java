package com.shuidihuzhu.transfer.model;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author duchao
 * @version : Document.java, v 0.1 2019-06-21 16:50 duchao Exp $
 */
public class Document {


    public static SinkRecord parseFromConsumerRecord(ConsumerRecord<String, String> consumerRecord) {

        if (consumerRecord == null || null == consumerRecord.value()) {
            return null;
        }
        return JSON.parseObject(consumerRecord.value(), SinkRecord.class);
    }
}