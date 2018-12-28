package com.shuidihuzhu.transfer;


@SpringBootApplication(scanBasePackages = {
        "com.shuidihuzhu.transfer"
})
public class Application {
    public static void main(String[] args) {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        SpringApplication.run(Application.class, args);
    }

}
