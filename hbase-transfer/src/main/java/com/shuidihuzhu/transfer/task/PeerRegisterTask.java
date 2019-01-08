package com.shuidihuzhu.transfer.task;

import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
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
}
