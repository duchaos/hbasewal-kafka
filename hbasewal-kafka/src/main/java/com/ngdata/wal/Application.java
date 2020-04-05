package com.ngdata.wal;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @author duchao
 */
@SpringBootApplication(scanBasePackages = "com.ngdata.*")
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
