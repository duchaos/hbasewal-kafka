package com.shuidihuzhu.transfer;


import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.BasePayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.shuidihuzhu.transfer.listener.SepEventListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@SpringBootApplication(scanBasePackages = {
        "com.shuidihuzhu.transfer"
})
public class Application {
    public static void main(String[] args) {


        System.setProperty("es.set.netty.runtime.available.processors", "false");

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean("hbase.replication", true);

            ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
            SepModel sepModel = new SepModelImpl(zk, conf);

            final String subscriptionName = "logger";

            if (!sepModel.hasSubscription(subscriptionName)) {
                sepModel.addSubscriptionSilent(subscriptionName);
            }

            PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes("sep-user-demo"), Bytes.toBytes("info"), Bytes.toBytes("payload"));

            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, new SepEventListener(), 1, "localhost", zk, conf, payloadExtractor);

            sepConsumer.start();
            System.out.println("Started");

            /*while (true) {
                Thread.sleep(Long.MAX_VALUE);
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }

        SpringApplication.run(Application.class, args);


    }

}
