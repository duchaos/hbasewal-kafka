package com.ngdata.wal.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author duchao
 */
@Data
@Configuration
@ConfigurationProperties("hbasewal-kafka.kafka")
public class KafkaConfig {
     String bootstrapServer;
}
