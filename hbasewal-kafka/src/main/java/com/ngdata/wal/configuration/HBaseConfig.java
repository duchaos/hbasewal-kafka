package com.ngdata.wal.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


/**
 * @author duchao
 */
@Data
@Configuration
@ConfigurationProperties("hbasewal-kafka.hbase")
public class HBaseConfig {
    private Client client;

    @Data
    public static class Client {
        private Operation operation;

        @Data
        public static class Operation {
            private long timeout;
        }
    }

    private Rpc rpc;

    @Data
    public static class Rpc {
        private long timeout;
    }

    private Zookeeper zookeeper;

    @Data
    public static class Zookeeper {
        private String servers;
    }
}
