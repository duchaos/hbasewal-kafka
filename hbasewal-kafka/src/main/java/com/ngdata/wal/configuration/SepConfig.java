package com.ngdata.wal.configuration;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author duchao
 */
@Data
@Configuration
@ConfigurationProperties("hbasewal-kafka.sep")
public class SepConfig {

    String zkServer;

    int sessionTimeout;

    Subscription subscription;

    @Data
    public static class Subscription {
        private List<String> nameList;
    }
}
