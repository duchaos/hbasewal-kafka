hbase-sep-tools
===============

Monitoring tool for the HBase SEP or HBase replication in general. It is based on reading
information from ZooKeeper, HDFS (to get hlog file sizes) and HBase JMX (optional, some
things only available when using the sep's ForkedReplicationSource).

## Monitor replication progress

Run (from the root of the hbase-indexer tree, or its binary dist):

    ./bin/hbase-indexer replication-status

Use the -z option to specify the zookeeper host.

## Wait for replication to be finished

There is also a tool to wait until replication (sep processing) is done:

    ./bin/hbase-indexer replication-wait

or to see what it is doing

    ./target/sep-replication-wait --verbose

Again, use the -z option to specify the zookeeper host.

Note that:

 * this assumes there is no ongoing hbase write activity, otherwise replication
   will never be finished. There can be updates to SEP listeners, but those
   should eventually die out.

 * this tool can't know for sure replication is done, it is based on some
   heuristics which can fail.

