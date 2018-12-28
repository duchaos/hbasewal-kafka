package com.shuidihuzhu.transfer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class
})
@EnableDiscoveryClient
@EnableFeignClients(basePackages = {"com.shuidihuzhu.transfer", "com.shuidihuzhu.data"})
@ComponentScan(basePackages = {"com.shuidihuzhu.client.dataservice.ds.v1","com.shuidihuzhu.baseservice.hawkeye", "com.shuidihuzhu.data"})
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
