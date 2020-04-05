package com.ngdata.wal.task;

import com.ngdata.sep.SepModel;
import com.ngdata.wal.configuration.HBaseConfig;
import com.ngdata.wal.configuration.SepConfig;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author duchao
 */
@Slf4j
@Order(1)
@Component
public class PeerRegisterTask implements InitializingBean, CommandLineRunner {

    private SepModel sepModel;

    private final SepConfig sepConfig;

    private final HBaseConfig hBaseConfig;

    private final TransferTask transferTask;

    public PeerRegisterTask(SepConfig sepConfig, HBaseConfig hBaseConfig, TransferTask transferTask) {
        this.sepConfig = sepConfig;
        this.hBaseConfig = hBaseConfig;
        this.transferTask = transferTask;
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        // 连接zk
        ZooKeeperItf sepZk = ZkUtil.connect(sepConfig.getZkServer(), sepConfig.getSessionTimeout());
        // 访问zk，add peers
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, Boolean.TRUE);
        conf.set(HConstants.ZOOKEEPER_QUORUM, hBaseConfig.getZookeeper().getServers());
        sepModel = new SepModelImpl(sepZk, conf);
    }


    @Override
    public void run(String... strings) throws Exception {
        List<String> peers = sepConfig.getSubscription().getNameList();
        if (CollectionUtils.isEmpty(peers)) {
            log.error(" peers is empty!");
            return;
        }
        log.info(" init list_peers :{}", peers);
        for (String peer : peers) {
            addPeer(peer);
        }

    }

    public void addPeer(String subscriptionName) throws Exception {
        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }
        transferTask.addListener(subscriptionName);
    }

    public void removePeer(String subscriptionName) throws Exception {
        transferTask.removeListener(subscriptionName);
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.removeSubscriptionSilent(subscriptionName);
        }
    }

    public void enablePeer(String subscriptionName) throws Exception {
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.enableSubscriptionSilent(subscriptionName);
        }
        transferTask.addListener(subscriptionName);
    }

    public void disablePeer(String subscriptionName) throws Exception {
        transferTask.removeListener(subscriptionName);
        if (sepModel.hasSubscription(subscriptionName)) {
            sepModel.disableSubscriptionSilent(subscriptionName);
        }
    }


}
