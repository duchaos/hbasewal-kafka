# hbase-transfer

hbase-transfer is tool used for transfer hbase data into es and kafka
follow those step to use hbase-transfer

1. generate jar
mvn clean install -DskipTests
hbase-sep-api-1.0-SNAPSHOT.jar
hbase-sep-impl-1.0-SNAPSHOT.jar

2. put 2 jar files to hbase lib
/opt/cloudera/parcels/CDH/lib/hbase/lib

3. open hbase cluster replication feature

4. open hbase table replication feature
alter "test_table", {REPLICATION_SCOPE='1'}

5. restart habase cluster

6. start hbase-transfer

note:
hbase version must appreciate to cdh







