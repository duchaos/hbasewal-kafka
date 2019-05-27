package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.shuidihuzhu.transfer.model.SinkRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by apple on 18/11/14.
 */
@Service
public class HKafkaConsumer implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger(HKafkaConsumer.class);
    @Autowired
    private ESSink esSink;

    @Value("${hbase-transfer.kafka.bootstrap.server}")
    private String kafkaBroker;

    @Value("${hbase-transfer.kafka.bootstrap.groupId}")
    private String groupId;
    @Value("${hbase-transfer.kafka.bootstrap.topic}")
    private String topic;
    @Value("${hbase-transfer.kafka.bootstrap.fromStart}")
    private String fromStart;

    @Override
    public void afterPropertiesSet() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                //TODO: 初始化，暂停消费kafka
                consumer(groupId, topic, fromStart);
            }
        }, "kafkaConsumerThread").start();
    }

    public void consumer(String groupId, String topic, String fromStart) {
//        //TODO: Test38
//        groupId = "hbase-tranfer_kafka_consumer_localtest";
//        topic = "hbase-tranfer-localtest";

        //TODO: online
        groupId = "hbase-tranfer_kafka_consumer_20190527";
        topic = "hbase-tranfer-20190527";

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");

//        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("onPartitionsRevoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("onPartitionsAssigned");
                if(!StringUtils.isEmpty(fromStart) && "earliest".equals(fromStart)){
                    consumer.seekToBeginning(partitions);
                }
            }
        });

        boolean dualFlag;
        while (true) {
            ConsumerRecords<String, String> records = null;
            try{
                records = consumer.poll(1000);
            }catch (Exception e){
                logger.error("kafka consumer error.",e);
            }
            if (records.count() > 0) {
                List<SinkRecord> recordList = Lists.newArrayList();
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    try {
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            SinkRecord sinkRecordObj = JSON.parseObject(record.value(), SinkRecord.class);
                            recordList.add(sinkRecordObj);
                        }
                        esSink.batchSink(recordList);
                        dualFlag = true;
                    } catch (Exception e) {
                        logger.error("handler error.", e);
                        dualFlag = false;
                    }
                    if (dualFlag) {
                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    }
                }
            }
        }
    }
}
