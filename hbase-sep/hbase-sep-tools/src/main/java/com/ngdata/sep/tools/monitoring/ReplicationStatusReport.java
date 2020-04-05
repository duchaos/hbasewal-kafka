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
package com.ngdata.sep.tools.monitoring;

import org.joda.time.DateTime;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Generate a text report of the replication status.
 */
public class ReplicationStatusReport {
    public static void printReport(ReplicationStatus replicationStatus, PrintStream out) {
        if (replicationStatus.getPeersAndRecoveredQueues().size() == 0) {
            System.out.println("There are no peer clusters.");
            return;
        }

        String columnFormat = "  | %1$-50.50s | %2$-15.15s | %3$-15.15s | %4$-15.15s | %5$-15.15s | %6$-30.30s | %7$-5.5s |\n";
        String columnFormatWide = "  | %1$-50.50s | %2$-110.110s |\n";

        out.println();
        out.println();
        out.println("How to interpret the output:");
        out.println(" * this shows the progress from the HBase point of view, i.e. how far");
        out.println("   each regionserver is in shipping its events to the peers.");
        out.println(" * each time the 'Current HLog progress' percentage goes to 100%, the");
        out.println("   'queue size' will drop by one.");
        out.println(" * if the 'queue size' is 1, no 'Current HLog progress' is shown,");
        out.println("   but at least you there is no delay beyond the current hlog, which");
        out.println("   means you're in a good situation.");
        out.println(" * 'last slept' information (not always available): is only relevant");
        out.println("   if it is recent.");
        out.println(" * age of last shipped op: this is the age of the last shipped wal entry,");
        out.println("   at the time it was shipped. If there is no further activity on HBase,");
        out.println("   this value will stay constant.");
        out.println(" * Peer count is only updated when edits are being shipped, i.e. when there");
        out.println("   is activity.");
        out.println(" * Recovered queues appear each time regionservers are restarted, they");
        out.println("   will disappear once processed.");
        out.println();

        out.format(columnFormat, "Host", "Queue size",      "Size all HLogs",  "Current HLog", "Age last",   "TS last",    "Peer");
        out.format(columnFormat, "",     "(incl. current)", "(excl. current)", "progress",     "shipped op", "shipped op", "count");

        for (String peerId : sort(replicationStatus.getPeersAndRecoveredQueues())) {
            out.println();
            if (replicationStatus.isRecoveredQueue(peerId)) {
                out.println("Recovered queue: " + peerId);
            } else {
                out.println("Peer cluster: " + peerId);
            }
            out.println();
            for (String server : sort(replicationStatus.getServers(peerId))) {
                ReplicationStatus.Status status = replicationStatus.getStatus(peerId, server);
                out.format(columnFormat, server,
                        String.valueOf(status.getHLogCount()), formatAsMB(status.getTotalHLogSize()),
                        formatProgress(status.getProgressOnCurrentHLog()), formatDuration(status.ageOfLastShippedOp),
                        formatTimestamp(status.timestampOfLastShippedOp), formatInt(status.selectedPeerCount));
                if (status.timestampLastSleep != null) {
                    long sleepAge = System.currentTimeMillis() - status.timestampLastSleep;
                    out.format(columnFormatWide, "", "Last slept " + formatDuration(sleepAge) + " ago (muliplier: "
                            + status.sleepMultiplier + "): " + status.sleepReason);
                }
            }
        }
        out.println();
    }

    private static List<String> sort(Collection<String> items) {
        List<String> things = new ArrayList<String>(items);
        Collections.sort(things);
        return things;
    }

    private static String formatAsMB(long size) {
        if (size == -1) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("#.# MB");
            return format.format((double)size / 1000d / 1000d);
        }
    }

    private static String formatProgress(float progress) {
        if (Float.isNaN(progress)) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("0 %");
            return format.format(progress);
        }
    }

    private static String formatDuration(Long millis) {
        if (millis == null) {
            return "(enable jmx)";
        }

        long millisOverflow = millis % 1000;
        long seconds = (millis - millisOverflow) / 1000;
        long secondsOverflow = seconds % 60;
        long minutes = (seconds - secondsOverflow) / 60;
        long minutesOverflow = minutes % 60;
        long hours = (minutes - minutesOverflow) / 60;
        int days = (int)Math.floor((double)hours / 24d);

        return String.format("%1$sd %2$02d:%3$02d:%4$02d.%5$03d",
                days, hours, minutesOverflow, secondsOverflow, millisOverflow);
    }

    private static String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return "unknown";
        }

        if (timestamp <= 0) {
            return "no activity yet";
        }

        return new DateTime(timestamp).toString();
    }

    private static String formatInt(Integer value) {
        if (value == null) {
            return "unknown";
        }

        return String.valueOf(value);
    }
}
