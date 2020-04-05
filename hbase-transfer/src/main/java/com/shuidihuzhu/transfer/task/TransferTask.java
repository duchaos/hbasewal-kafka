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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by sunfu on 2018/12/29.
 */
@Component
@Order(1)
public class TransferTask implements CommandLineRunner{

    static String zookeeperConnectors = "10.100.4.2,10.100.4.3,10.100.4.4";
    static String subscriptionName = "logger";
    static String hbaseTable = "sdhz_user_info_realtime";
    static String hbaseColumnFamily = "data";
    static String columnQualifier = "payload";

    @Value("${spring.cloud.client.ip-address}")
    private String ip;
    /**
     *
     configserver配置文件格式如下，支持多个表的复制
     ---
     zookeeper:
        servers: localhost
     subscription:
         list:
         - {name: logger}
         - {name: logger1}
     hbase:
         list:
         - {table: sep-user-demo, column-family: info, column-qualifier: payload}
         - {table: sep-user-demo1, column-family: info1, column-qualifier: payload}

     *
     */

    @Autowired
    SepEventListener sepEventListener;

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
            String host = null;
            try {
                host = InetAddress.getLocalHost().getHostAddress();
                System.out.println("===================="+ip);
            } catch (UnknownHostException e) {
//                log.error("get server host Exception e:", e);
            }
            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, sepEventListener, 1, ip, zk, conf, payloadExtractor);
            sepConsumer.start();

            System.out.println("hbase transfer started...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
