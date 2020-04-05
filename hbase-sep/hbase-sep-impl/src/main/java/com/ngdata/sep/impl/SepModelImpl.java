/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class SepModelImpl implements SepModel {

    // Replace '-' with unicode "CANADIAN SYLLABICS HYPHEN" character in zookeeper to avoid issues
    // with HBase replication naming conventions
    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';
    private final Admin admin;
    private final Connection connection;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private final String baseZkPath;
    private final String zkQuorumString;
    private final int zkClientPort;
    private final List<String> subscriptionNameList;
    private Log log = LogFactory.getLog(getClass());

    public SepModelImpl(ZooKeeperItf zk, Configuration hbaseConf) throws IOException {

        this.zkQuorumString = hbaseConf.get("hbase.zookeeper.quorum");
        if (zkQuorumString == null) {
            throw new IllegalStateException("hbase.zookeeper.quorum not supplied in configuration");
        }
        if (zkQuorumString.contains(":")) {
            throw new IllegalStateException("hbase.zookeeper.quorum should not include port number, got " + zkQuorumString);
        }
        try {
            this.zkClientPort = Integer.parseInt(hbaseConf.get("hbase.zookeeper.property.clientPort"));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Non-numeric zookeeper client port", e);
        }

        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.connection = ConnectionFactory.createConnection(hbaseConf);
        this.admin = connection.getAdmin();
        this.baseZkPath = hbaseConf.get(ZK_ROOT_NODE_CONF_KEY, DEFAULT_ZK_ROOT_NODE);
        this.subscriptionNameList = admin.listReplicationPeers().stream().map(ReplicationPeerDescription::getPeerId).collect(Collectors.toList());
    }

    @Override
    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (!addSubscriptionSilent(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }
    }

    @Override
    public boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException {
        try {
            String internalName = toInternalSubscriptionName(name);

            if (subscriptionNameList.contains(internalName)) {
                return false;
            }

            String basePath = baseZkPath + "/" + internalName;
            UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
            ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
            ZkUtil.createPath(zk, basePath + "/rs");


            try {
                ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                        .setClusterKey(zkQuorumString+":" + zkClientPort + ":" + basePath).build();
                admin.addReplicationPeer(internalName,  peerConfig );
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot add existing peer")) {
                    return false;
                }
                throw e;
            } catch (Exception e) {
                // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                if (e instanceof KeeperException) {
                    throw (KeeperException)e;
                } else {
                    throw new IOException(e);
                }
            }

            return true;
        } finally {
            Closer.close(admin);
        }
    }

    @Override
    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    @Override
    public boolean removeSubscriptionSilent(String name) throws IOException {
        try {
            String internalName = toInternalSubscriptionName(name);
            if (!subscriptionNameList.contains(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                try {
                    admin.removeReplicationPeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            String basePath = baseZkPath + "/" + internalName;
            try {
                ZkUtil.deleteNode(zk, basePath + "/hbaseid");
                for (String child : zk.getChildren(basePath + "/rs", false)) {
                    ZkUtil.deleteNode(zk, basePath + "/rs/" + child);
                }
                ZkUtil.deleteNode(zk, basePath + "/rs");
                ZkUtil.deleteNode(zk, basePath);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (KeeperException ke) {
                log.error("Cleanup in zookeeper failed on " + basePath, ke);
            }
            return true;
        } finally {
            Closer.close(admin);
        }
    }


    @Override
    public boolean disableSubscriptionSilent(String name) throws IOException {
        try {
            String internalName = toInternalSubscriptionName(name);
            if (!subscriptionNameList.contains(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                try {
                    admin.disableReplicationPeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            return true;
        } finally {
            Closer.close(admin);
        }
    }

    @Override
    public boolean enableSubscriptionSilent(String name) throws IOException {
        try {
            String internalName = toInternalSubscriptionName(name);
            if (!subscriptionNameList.contains(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                try {
                    admin.enableReplicationPeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            return true;
        } finally {
            Closer.close(admin);
        }
    }

    @Override
    public boolean hasSubscription(String name) throws IOException {
        try {
            String internalName = toInternalSubscriptionName(name);
            return subscriptionNameList.contains(internalName);
        } finally {
            Closer.close(admin);
        }
    }

    static String toInternalSubscriptionName(String subscriptionName) {
        if (subscriptionName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U1400");
        }
        return subscriptionName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }
}
