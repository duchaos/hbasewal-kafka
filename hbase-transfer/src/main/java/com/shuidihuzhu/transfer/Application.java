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


@SpringBootApplication(scanBasePackages = {
        "com.shuidihuzhu.transfer"
})
public class Application {

    static final String zookeeperConnectors = "localhost";
    static final String subscriptionName = "logger";
    static final String hbaseTable = "sep-user-demo";
    static final String hbaseColumnFamily = "info";



    public static void main(String[] args) {

        // todo 配置接入configserver





        try {
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean("hbase.replication", true);

            ZooKeeperItf zk = ZkUtil.connect(zookeeperConnectors, 20000);
            SepModel sepModel = new SepModelImpl(zk, conf);

            if (!sepModel.hasSubscription(subscriptionName)) {
                sepModel.addSubscriptionSilent(subscriptionName);
            }

            PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes(hbaseTable), Bytes.toBytes(hbaseColumnFamily), Bytes.toBytes("payload"));

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
