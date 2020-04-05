package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.shuidihuzhu.transfer.listener.SepEventListener;
import com.shuidihuzhu.transfer.model.SinkRecord;
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
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        procuder = new KafkaProducer<String, String>(props);


    }

    @Override
    public void sink(SinkRecord record) {
        try {
            ProducerRecord<String,String> item = new ProducerRecord<String, String>(topic, JSON.toJSONString(record));
            procuder.send(item, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    System.out.println("message send to partition=" + metadata.partition() + ", offset=" + metadata.offset() + ", content=" + SinkRecord.getText(record));
                }
            });
        } catch (Exception e) {
            logger.error("kafka send error.",e);
            handleErrorRecord(record);
        }
    }

    public void consumer(){

    }

    @Override
    public void batchSink(List<SinkRecord> records) {

    }


}
