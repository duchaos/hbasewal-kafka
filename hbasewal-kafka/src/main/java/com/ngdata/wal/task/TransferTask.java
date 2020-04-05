package com.ngdata.wal.task;

import com.google.common.collect.Maps;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.wal.configuration.HBaseConfig;
import com.ngdata.wal.configuration.SepConfig;
import com.ngdata.wal.listener.SepEventListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author duchao
 */
@Slf4j
@Component
public class TransferTask {

    @Value("${spring.cloud.client.ip-address}")
    private String ip;
    private final SepConfig sepConfig;
    private final HBaseConfig hBaseConfig;
    private final SepEventListener sepEventListener;
    private Map<String, SepConsumer> peerListenerMap = Maps.newConcurrentMap();

    public TransferTask(SepConfig sepConfig, HBaseConfig hBaseConfig, SepEventListener sepEventListener) {
        this.sepConfig = sepConfig;
        this.hBaseConfig = hBaseConfig;
        this.sepEventListener = sepEventListener;
    }

    public boolean addListener(String subscriptionName) {
        try {
            String hbaseZookeeper = hBaseConfig.getZookeeper().getServers();
            String sepConfigZkServer = sepConfig.getZkServer();
            log.info("peer:{} addListener HBaseZookeeper:{},sepZookeeper:{},ip:{}", subscriptionName, hbaseZookeeper, sepConfigZkServer, ip);
            // 连接zk
            ZooKeeperItf sepZk = ZkUtil.connect(sepConfigZkServer, sepConfig.getSessionTimeout());
            // 访问zk，add peers
            Configuration conf = HBaseConfiguration.create();
            conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, Boolean.TRUE);
            conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZookeeper);
            SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, sepEventListener, 1, ip, sepZk, conf);
            sepConsumer.start();
            peerListenerMap.put(subscriptionName, sepConsumer);
        } catch (Exception e) {
            log.error("peer:{} addListener error", subscriptionName, e);
            return false;
        }
        return true;
    }

    public boolean removeListener(String subscriptionName) {
        try {
            if (peerListenerMap.containsKey(subscriptionName)) {
                SepConsumer sepConsumer = peerListenerMap.get(subscriptionName);
                sepConsumer.stop();
            }
        } catch (Exception e) {
            log.error("peer:{} addListener error", subscriptionName, e);
            return false;
        }
        return true;
    }
}
