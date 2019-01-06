package com.shuidihuzhu.transfer.task;

import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.BasePayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.shuidihuzhu.transfer.listener.SepEventListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Created by sunfu on 2018/12/29.
 */
@Component
@Order(1)
public class TransferTask implements CommandLineRunner{

//    static String zookeeperConnectors = "10.100.4.2,10.100.4.3,10.100.4.4";
    @Value("${hbase-transfer.hbase.zookeeper.servers}")
    private String zookeeperConnectors;
    @Value("${hbase-transfer.subscription.name}")
    private String subscriptionName;
    @Value("${hbase-transfer.hbase.table}")
    private String hbaseTable;
//    static String hbaseColumnFamily = "data";
//    static String columnQualifier = "payload";

    @Value("${spring.cloud.client.ip-address}")
    private String ip;


    @Autowired
    SepEventListener sepEventListener;

    @Override
    public void run(String... strings) throws Exception {

        try {
            // 连接zk
            ZooKeeperItf zk = ZkUtil.connect(zookeeperConnectors, 20000);

            // 访问zk，add peers
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean("hbase.replication", true);
            conf.set("hbase.zookeeper.quorum",zookeeperConnectors);
            SepModel sepModel = new SepModelImpl(zk, conf);
            if (!sepModel.hasSubscription(subscriptionName)) {
                sepModel.addSubscriptionSilent(subscriptionName);
            }

            PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes(hbaseTable), "".getBytes(),"".getBytes());

            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, sepEventListener, 1, ip, zk, conf, payloadExtractor);
            sepConsumer.start();

            System.out.println("hbase transfer started...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
