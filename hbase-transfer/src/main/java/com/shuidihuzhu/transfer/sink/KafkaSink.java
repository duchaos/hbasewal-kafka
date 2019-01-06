package com.shuidihuzhu.transfer.sink;

import com.shuidihuzhu.transfer.model.SinkRecord;
import org.apache.kafka.clients.producer.*;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class KafkaSink extends AbstractSink {

    String topic = "hbase-tranfer";
    Producer<String, String> procuder = null;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.100.4.2:9092,10.100.4.3:9092,10.100.4.4:9092");
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
            ProducerRecord<String,String> item = new ProducerRecord<String, String>(topic, SinkRecord.getText(record));
            procuder.send(item, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("message send to partition=" + metadata.partition() + ", offset=" + metadata.offset() + ", content=" + SinkRecord.getText(record));
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            handleErrorRecord(record);
        }
    }

    @Override
    public void batchSink(List<SinkRecord> records) {

    }


}
