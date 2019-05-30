package com.shuidihuzhu.transfer.task;

import com.shuidihuzhu.sep.SepModel;
import com.shuidihuzhu.sep.impl.SepModelImpl;
import com.shuidihuzhu.sep.util.zookeeper.ZkUtil;
import com.shuidihuzhu.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Created by apple on 19/1/7.
 */
@Component
@Order(1)
public class PeerRegisterTask implements CommandLineRunner {
    @Value("${hbase-transfer.hbase.zookeeper.servers}")
    private String hbaseZookeeper;
    @Value("${hbase-transfer.sep.zookeeper.servers}")
    private String sepZookeeper;
    @Value("${hbase-transfer.subscription.name}")
    private String subscriptionName;

    @Override
    public void run(String... strings) throws Exception {
        //TODO: Test38
//        String test_zk = "10.100.4.2,10.100.4.3,10.100.4.4";
//        hbaseZookeeper = test_zk;
//        sepZookeeper = test_zk;
//        subscriptionName = "logger";

        //TODO: online
//        subscriptionName = "logger20190517";

        addPeer();
    }

    public void addPeer() throws Exception {
        // 连接zk
        ZooKeeperItf sepZk = ZkUtil.connect(sepZookeeper, 20000);
        // 访问zk，add peers
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        conf.set("hbase.zookeeper.quorum",hbaseZookeeper);
        SepModel sepModel = new SepModelImpl(sepZk, conf);
        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }
    }

    public void removePeer() throws Exception {
        // 连接zk
        ZooKeeperItf sepZk = ZkUtil.connect(sepZookeeper, 20000);
        // 访问zk，add peers
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        conf.set("hbase.zookeeper.quorum",hbaseZookeeper);
        SepModel sepModel = new SepModelImpl(sepZk, conf);
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.removeSubscriptionSilent(subscriptionName);
        }
    }

    public void disablePeer() throws Exception {
        // 连接zk
        ZooKeeperItf sepZk = ZkUtil.connect(sepZookeeper, 20000);
        // 访问zk，add peers
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        conf.set("hbase.zookeeper.quorum",hbaseZookeeper);
        SepModel sepModel = new SepModelImpl(sepZk, conf);
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.disableSubscriptionSilent(subscriptionName);
        }
    }

    public void enablePeer() throws Exception {
        // 连接zk
        ZooKeeperItf sepZk = ZkUtil.connect(sepZookeeper, 20000);
        // 访问zk，add peers
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        conf.set("hbase.zookeeper.quorum",hbaseZookeeper);
        SepModel sepModel = new SepModelImpl(sepZk, conf);
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.enableSubscriptionSilent(subscriptionName);
        }
    }
}
