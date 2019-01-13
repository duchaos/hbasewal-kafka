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
@Order(2)
public class TransferTask implements CommandLineRunner{

    @Value("${hbase-transfer.hbase.zookeeper.servers}")
    private String hbaseZookeeper;
    @Value("${hbase-transfer.sep.zookeeper.servers}")
    private String sepZookeeper;
    @Value("${hbase-transfer.subscription.name}")
    private String subscriptionName;

    @Value("${spring.cloud.client.ip-address}")
    private String ip;

    @Autowired
    private SepEventListener sepEventListener;

    @Override
    public void run(String... strings) throws Exception {

        try {
            // 连接zk
            ZooKeeperItf sepZk = ZkUtil.connect(sepZookeeper, 2000000);
            // 访问zk，add peers
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean("hbase.replication", true);
            conf.set("hbase.zookeeper.quorum",hbaseZookeeper);
            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, sepEventListener, 1, ip, sepZk, conf, null);
            sepConsumer.start();

            System.out.println("hbase transfer started...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
