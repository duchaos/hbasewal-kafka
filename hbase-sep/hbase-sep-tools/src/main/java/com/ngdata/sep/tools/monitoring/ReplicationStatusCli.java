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

import com.google.common.collect.ImmutableList;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ReplicationStatusCli {

    public static void main(String[] args) throws Exception {
        new ReplicationStatusCli().run(args);
    }

    public void run(String[] args) throws Exception {
        //LogManager.resetConfiguration();
        //PropertyConfigurator.configure(getClass().getResource("log4j.properties"));

        OptionParser parser =  new OptionParser();
        OptionSpec enableJmxOption = parser.accepts("enable-jmx",
                "use JMX to retrieve info from HBase regionservers (port " + ReplicationStatusRetriever.HBASE_JMX_PORT + ")");
        OptionSpec<String> zkOption = parser
                .acceptsAll(ImmutableList.of("z"), "ZooKeeper connection string, defaults to localhost")
                .withRequiredArg().ofType(String.class)
                .defaultsTo("localhost");
        
        OptionSpec<Integer> hbaseMasterPortOption = parser
                .acceptsAll(ImmutableList.of("hbase-master-port"), "HBase Master web ui port number")
                .withRequiredArg().ofType(Integer.class)
                .defaultsTo(60010);

        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            System.out.println("Error parsing command line options:");
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        boolean enableJmx = options.has(enableJmxOption);
        String zkConnectString = options.valueOf(zkOption);

        System.out.println("Connecting to Zookeeper " + zkConnectString + "...");
        ZooKeeperItf zk = ZkUtil.connect(zkConnectString, 30000);

        ReplicationStatusRetriever retriever = new ReplicationStatusRetriever(zk, options.valueOf(hbaseMasterPortOption));
        ReplicationStatus replicationStatus = retriever.collectStatusFromZooKeepeer();

        if (enableJmx) {
            retriever.addStatusFromJmx(replicationStatus);
        } else {
            System.out.println();
            System.out.println("Hint: use --enable-jmx to retrieve more info from HBase regionservers");
            System.out.println("      For this, you need to start HBase regionservers with:");
            System.out.println("      -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10102");
            System.out.println();
        }

        ReplicationStatusReport.printReport(replicationStatus, System.out);

        Closer.close(zk);
    }
}
