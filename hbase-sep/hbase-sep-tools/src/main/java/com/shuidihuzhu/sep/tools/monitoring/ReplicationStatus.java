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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Contains information on the current replication status.
 */
public class ReplicationStatus {
    private Map<String, Map<String, Status>> statusByPeerAndServer;

    public ReplicationStatus(Map<String, Map<String, Status>> statusByPeerAndServer) {
        this.statusByPeerAndServer = statusByPeerAndServer;
    }

    public Map<String, Map<String, Status>> getStatusByPeerAndServer() {
        return statusByPeerAndServer;
    }

    public Collection<String> getPeersAndRecoveredQueues() {
        return statusByPeerAndServer.keySet();
    }

    public boolean isRecoveredQueue(String peerId) {
        return RECOVERED_QUEUE_PREDICATE.apply(peerId);
    }

    public Collection<String> getPeers() {
        return Collections2.filter(statusByPeerAndServer.keySet(), Predicates.not(RECOVERED_QUEUE_PREDICATE));
    }

    public Collection<String> getRecoverdQueues() {
        return Collections2.filter(statusByPeerAndServer.keySet(), RECOVERED_QUEUE_PREDICATE);
    }

    public Collection<String> getServers(String peerId) {
        return statusByPeerAndServer.get(peerId).keySet();
    }

    public Status getStatus(String peerId, String server) {
        Map<String, Status> statusByServer = statusByPeerAndServer.get(peerId);
        if (statusByServer == null) {
            return null;
        }
        return statusByServer.get(server);
    }

    public static Predicate<String> RECOVERED_QUEUE_PREDICATE = new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String peerId) {
            // For recovered queues, there is a zk node with the format "peerId(-servername)+"
            return peerId.contains("-");
        }
    };

    /**
     * Status of a replication queue for one peer and regionserver.
     */
    public static class Status {
        List<HLogInfo> hlogs = new ArrayList<HLogInfo>();
        Long ageOfLastShippedOp;
        Long timestampOfLastShippedOp;
        Integer selectedPeerCount;
        String sleepReason;
        Integer sleepMultiplier;
        Long timestampLastSleep;

        int getHLogCount() {
            int count = 0;
            for (HLogInfo hlog : hlogs) {
                count++;
                if (hlog.position != -1) {
                    // we arrived at the current hlog file
                    // Apparently, HBase (0.94) keeps one more older hlog file around, that is already fully processed,
                    // and we can ignore that one.
                    break;
                }
            }
            return count;
        }

        long getTotalHLogSize() {
            long totalSize = 0;
            for (HLogInfo hlog : hlogs) {
                if (hlog.size == -1) {
                    // if the size of one of the hlog's could not be read from hdfs,
                    // report -1 (unknown) as size.
                    return -1;
                }
                totalSize += hlog.size;
                if (hlog.position != -1) {
                    // we arrived at the current hlog
                    break;
                }
            }
            return totalSize;
        }

        float getProgressOnCurrentHLog() {
            for (HLogInfo hlog : hlogs) {
                if (hlog.position != -1) {
                    if (hlog.size > 0) {
                        return (float)hlog.position / (float)hlog.size;
                    } else {
                        return Float.NaN;
                    }
                }
            }
            return Float.NaN;
        }
    }

    /**
     * Information on the processing of one hlog file for one peer.
     */
    public static class HLogInfo {
        String name;
        /** Currently reached position in the file, -1 if unstarted. */
        long position;
        /** Size of the HLog. Note that for files being written, this only includes the size of completed blocks. */
        long size;

        public HLogInfo(String name) {
            this.name = name;
        }
    }
}
