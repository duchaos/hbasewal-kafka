package com.shuidihuzhu.transfer.task;

import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.BasePayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.shuidihuzhu.flink.util.config.ConfigServerConfig;
import com.shuidihuzhu.flink.util.config.ConfigServerUtil;
import com.shuidihuzhu.flink.util.config.EnvEnum;
import com.shuidihuzhu.transfer.listener.SepEventListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Created by sunfu on 2018/12/29.
 */
@Component
@Order(1)
public class TransferTask implements CommandLineRunner{

    static String zookeeperConnectors = "localhost";
    static String subscriptionName = "logger";
    static String hbaseTable = "sep-user-demo";
    static String hbaseColumnFamily = "info";
    static String columnQualifier = "payload";

    @Override
    public void run(String... strings) throws Exception {
        /*ConfigServerConfig.env = "develop";
        ConfigServerUtil configServerUtils = ConfigServerUtil.getInstance("hbase-transfer", EnvEnum.valueOf(ConfigServerConfig.env));

        String zookeepers = configServerUtils.get("zookeeper.servers");
        String subscription = configServerUtils.get("subscription.list");
        String table = configServerUtils.get("hbase.list");

        System.out.println(zookeepers + "---" + subscription + "---" + table);*/

        /**
         * 如果需要同步多张表，这里面应该怎么写，for循环么？
         */

        try {
            // 连接zk
            ZooKeeperItf zk = ZkUtil.connect(zookeeperConnectors, 20000);

            // 访问zk，add peers
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean("hbase.replication", true);
            SepModel sepModel = new SepModelImpl(zk, conf);
            if (!sepModel.hasSubscription(subscriptionName)) {
                sepModel.addSubscriptionSilent(subscriptionName);
            }

            // 什么鬼？
            PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes(hbaseTable), Bytes.toBytes(hbaseColumnFamily), Bytes.toBytes(columnQualifier));

            // 创建sep rpc server
            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, new SepEventListener(), 1, "localhost", zk, conf, payloadExtractor);
            sepConsumer.start();

            System.out.println("hbase transfer started...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
