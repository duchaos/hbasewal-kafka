/*
 * Copyright 2013 NGDATA nv
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
package com.shuidihuzhu.sep.tools.monitoring;

import com.google.common.collect.ImmutableList;

import com.google.common.collect.Lists;
import com.shuidihuzhu.sep.util.io.Closer;
import com.shuidihuzhu.sep.util.zookeeper.ZkUtil;
import com.shuidihuzhu.sep.util.zookeeper.ZooKeeperItf;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import static com.shuidihuzhu.sep.tools.monitoring.ReplicationStatus.Status;

/**
 * Tool to wait until HBase replication is done, assuming there is no external write activity on HBase
 * (there might be write activity by SEP consumers, but it needs to die out eventually). Note that this
 * tool is far from perfect (it may say replication is done while it is not, though only in case of
 * problematic situations), we'd need HBase to expose more information to make it better.
 */
public class ReplicationWaitCli {
    private Log log = LogFactory.getLog(getClass());
    private static long MINIMAL_STABLE_STATUS_AGE = 120000L;

    public static void main(String[] args) throws Exception {
        new ReplicationWaitCli().run(args);
    }

    public void run(String[] args) throws Exception {
        //LogManager.resetConfiguration();
        //PropertyConfigurator.configure(getClass().getResource("log4j.properties"));

        OptionParser parser =  new OptionParser();
        OptionSpec<String> zkOption = parser
                .acceptsAll(Lists.newArrayList("z"), "ZooKeeper connection string, defaults to localhost")
                .withRequiredArg().ofType(String.class)
                .defaultsTo("localhost");
        OptionSpec verboseOption = parser
                .acceptsAll(Lists.newArrayList("verbose"), "Enable debug logging");
        
        OptionSpec<Integer> hbaseMasterPortOption = parser
                .acceptsAll(ImmutableList.of("hbase-master-port"), "HBase Master web ui port number")
                .withRequiredArg().ofType(Integer.class)
                .defaultsTo(60010);

        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            System.out.println("This tool does a best effort to wait until replication is done,");
            System.out.println("assuming no external write activity on HBase happens.");
            System.out.println();
            System.out.println("Error parsing command line options:");
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        boolean verbose = options.has(verboseOption);
        if (verbose) {
            //Logger.getLogger(getClass().getPackage().getName()).setLevel(Level.DEBUG);
        }

        String zkConnectString = options.valueOf(zkOption);

        System.out.println("Connecting to Zookeeper " + zkConnectString + "...");
        ZooKeeperItf zk = ZkUtil.connect(zkConnectString, 30000);
        waitUntilReplicationDone(zk, options.valueOf(hbaseMasterPortOption));

        Closer.close(zk);
    }

    public void waitUntilReplicationDone(ZooKeeperItf zk, int hbaseMasterPort) throws Exception {
        ReplicationStatusRetriever retriever = new ReplicationStatusRetriever(zk, hbaseMasterPort);

        DateTime startedAt = new DateTime();
        ReplicationStatus prevReplicationStatus = null;
        long statusStableSince = 0;

        System.out.println("Waiting for replication to be done. This will take at least "
                + MINIMAL_STABLE_STATUS_AGE + "ms.");

        while (true) {
            System.out.print(".");
            ReplicationStatus replicationStatus = retriever.collectStatusFromZooKeepeer();
            retriever.addStatusFromJmx(replicationStatus);

            // About the noQueue: we can't know if replication is done, and if there is only one (the current)
            // HLog file, we don't know how far it is in that hlog file. Obviously, if there are hlog files queued
            // (beyond the one currently processed), replication is not finished and we need to wait more.
            boolean noQueue = true;
            // Recovered queues disappear once finished, so definitely not finished as long as those are around
            if (replicationStatus.getRecoverdQueues().isEmpty()) {
                for (String peerId : replicationStatus.getPeers()) {
                    for (String server : replicationStatus.getServers(peerId)) {
                        Status status = replicationStatus.getStatus(peerId, server);
                        if (!(status.getHLogCount() == 1
                                /* sometimes it stays with two hlog files, with the second one 100% processed. */
                                || (status.getHLogCount() == 2 && Math.abs(status.getProgressOnCurrentHLog() - 1) < 0.000000001f))) {
                            noQueue = false;
                            break;
                        }
                    }
                }
            } else {
                log.debug("There are still recovered queues");
                noQueue = false;
            }

            // If no hlog files are queued (beyond the current one), wait until there is no replication activity
            // during MINIMAL_STABLE_STATUS_AGE, after which we assume replication is done
            // (there might also be no activity due to some other reason, e.g. that all peer servers are down and
            // hence that hbase can't replication anything, or that the processing of a batch of events takes longer
            // than the MINIMAL_STABLE_STATUS_AGE).
            if (noQueue) {
                log.debug("No hlog files queued, will check if replication status is stable");
                boolean statusStable = true;
                if (prevReplicationStatus != null) {
                    for (String peerId : replicationStatus.getPeers()) {
                        for (String server : replicationStatus.getServers(peerId)) {
                            Status status = replicationStatus.getStatus(peerId, server);
                            Status prevStatus = prevReplicationStatus.getStatus(peerId, server);
                            if (prevStatus == null) {
                                statusStable = false;
                                log.debug("No previous status for peer " + peerId + ", server " + server);
                                break;
                            }
                            if (prevStatus.ageOfLastShippedOp == null
                                    || !prevStatus.ageOfLastShippedOp.equals(status.ageOfLastShippedOp)) {
                                statusStable = false;
                                log.debug("Status still changing for peer " + peerId + ", server " + server);
                                break;
                            }
                        }
                    }
                } else {
                    log.debug("No previous status to compare with");
                }

                if (statusStable) {
                    if (statusStableSince == 0) {
                        log.debug("Status did not change (initial occurrence)");
                        statusStableSince = System.currentTimeMillis();
                    } else {
                        long age = System.currentTimeMillis() - statusStableSince;
                        log.debug("Status did not change compared to previous state of age " + age);
                        if (age > MINIMAL_STABLE_STATUS_AGE) {
                            System.out.println("Replication queues are of minimal size and no activity during "
                                    + age + " ms, assuming replication is finished.");
                            System.out.println("Started waiting at " + startedAt + ", now it is " + new DateTime());
                            return;
                        }
                    }
                } else {
                    log.debug("Status changed, need to wait more.");
                    statusStableSince = 0;
                }

                prevReplicationStatus = replicationStatus;
            } else {
                log.debug("Queues not empty, need to wait more.");
                statusStableSince = 0;
                prevReplicationStatus = null;
            }

            Thread.sleep(5000L);
        }
    }
}
