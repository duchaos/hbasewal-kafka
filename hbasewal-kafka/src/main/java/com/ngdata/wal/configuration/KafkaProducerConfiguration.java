package com.ngdata.wal.configuration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * @author duchao
 */
@Configuration
public class KafkaProducerConfiguration {

    private final KafkaConfig kafkaConfig;

    public KafkaProducerConfiguration(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public Producer<String, String> newProducer() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.RETRIES_CONFIG, String.valueOf(3));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32768));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.LZ4.name);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(1));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServer());
        return new KafkaProducer<>(props);
    }
}
