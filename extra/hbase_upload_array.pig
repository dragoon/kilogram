REGISTER /opt/cloudera/parcels/CDH/lib/zookeeper/zookeeper.jar
REGISTER /opt/cloudera/parcels/CDH/lib/hbase/hbase-client.jar
REGISTER /opt/cloudera/parcels/CDH/lib/hive/lib/guava-14.0.1.jar

data = LOAD '$path' USING PigStorage('\t') AS (value:chararray, cnt:chararray);
data = FOREACH data GENERATE value as value, cnt;
STORE data INTO 'hbase://$table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('ngram:value ngram:cnt');