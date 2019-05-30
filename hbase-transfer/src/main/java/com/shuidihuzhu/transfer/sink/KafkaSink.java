package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.shuidihuzhu.transfer.listener.SepEventListener;
import com.shuidihuzhu.transfer.model.SinkRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class KafkaSink extends AbstractSink {
    private Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    @Value("${hbase-transfer.kafka.bootstrap.topic}")
    private String topic;
    Producer<String, String> procuder = null;

    @Value("${hbase-transfer.kafka.bootstrap.server}")
    private String bootstrap;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("max.request.size", 5242880);//5m
        //设置压缩类型
        String compType = "lz4";
        if(StringUtils.isNotBlank(compType)) {
            props.put("compression.type", compType);
        }
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        procuder = new KafkaProducer<String, String>(props);

        //TODO: Test38
//        topic = "hbase-tranfer-localtest";

        //TODO: online
//        topic = "hbase-tranfer-20190527";
    }

    @Override
    public void sink(SinkRecord record) {
        try {
            ProducerRecord<String, String> item = new ProducerRecord<String, String>(topic, JSON.toJSONString(record));
            procuder.send(item, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
//                    if (metadata != null) {
//                        logger.debug("[发送成功] --- topic= " + metadata.topic() + "，partiton: " + metadata.partition() + " offset: " + metadata.offset() + ", content=" + SinkRecord.getText(record));
//                    }

                    if (e != null) {
                        logger.error("[发送失败] --- topic= " + topic + ",失败原因：" + e.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            logger.error("kafka send error.", e);
            handleErrorRecord(record);
        }
    }
}
