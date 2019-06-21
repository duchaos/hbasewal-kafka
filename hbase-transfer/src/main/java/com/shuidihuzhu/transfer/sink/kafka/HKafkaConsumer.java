package com.shuidihuzhu.transfer.sink.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.elasticsearch.ESSink;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

/**
 * Created by apple on 18/11/14.
 */
@Service
public class HKafkaConsumer implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger(HKafkaConsumer.class);

    @Resource
    private ESSink deviceInfoESSink;
    @Resource
    private ESSink userInfoESSink;

    @Resource
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
                consumer(groupId, topic, fromStart);
            }
        }, "kafkaConsumerThread").start();
    }


    public void consumer(String groupId, String topic, String fromStart) {
//        //TODO: Test38
//        groupId = "hbase-tranfer_kafka_consumer_localtest";
//        topic = "hbase-tranfer-localtest";

        //TODO: online
//        groupId = "hbase-tranfer_kafka_consumer_20190527";
//        topic = "hbase-tranfer-20190527";

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
                if (!StringUtils.isEmpty(fromStart) && "earliest".equals(fromStart)) {
                    consumer.seekToBeginning(partitions);
                }
            }
        });

        boolean dualFlag;
        while (true) {
            ConsumerRecords<String, String> records = null;
            try {
                records = consumer.poll(1000);
            } catch (Exception e) {
                logger.error("kafka consumer error.", e);
            }
            if (records.count() > 0) {
                Map<String, SinkRecord> recordMap = new HashMap();
                List<SinkRecord> recordList = Lists.newArrayList();
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    try {
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            SinkRecord sinkRecordObj = JSON.parseObject(record.value(), SinkRecord.class);

                            String id = String.valueOf(sinkRecordObj.getKeyValues().get("id"));
                            if (recordMap.containsKey(id)) {
                                recordMap.get(id).getKeyValues().putAll(sinkRecordObj.getKeyValues());
                            } else {
                                recordMap.put(id, sinkRecordObj);
                            }

                        }

                        recordList.addAll(recordMap.values());
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


    @KafkaListener(topics = "hbase-tranfer", groupId = "hbase-tranfer_kafka_consumer", errorHandler = "kafkaConsumerErrorHandler")
    public void userInfoListener(ConsumerRecord<String, String> record, Acknowledgment ack, Consumer consumer) {
        if (!StringUtils.isEmpty(fromStart) && "earliest".equals(fromStart)) {
            Collection<TopicPartition> partitions = Collections.singletonList(new TopicPartition("hbase-tranfer", record.partition()));
            consumer.seekToBeginning(partitions);
        }
        userInfoESSink.batchSink(Lists.newArrayList(SinkRecord.parseFromConsumerRecord(record)));
        ack.acknowledge();
    }


    @KafkaListener(topics = "hbase-tranfer-device", groupId = "hbase-tranfer_kafka_consumer-device", errorHandler = "kafkaConsumerErrorHandler")
    public void deviceInfoListener(ConsumerRecord<String, String> record, Acknowledgment ack, Consumer consumer) {
        if (!StringUtils.isEmpty(fromStart) && "earliest".equals(fromStart)) {
            Collection<TopicPartition> partitions = Collections.singletonList(new TopicPartition("hbase-tranfer-device", record.partition()));
            consumer.seekToBeginning(partitions);
        }
        deviceInfoESSink.batchSink(Lists.newArrayList(SinkRecord.parseFromConsumerRecord(record)));
        ack.acknowledge();
    }

}
