REGISTER /opt/cloudera/parcels/CDH/lib/zookeeper/zookeeper.jar
REGISTER /opt/cloudera/parcels/CDH/lib/hbase/hbase-client.jar
REGISTER /opt/cloudera/parcels/CDH/lib/hive/lib/guava-11.0.2.jar

data = LOAD '/data/2grams/*' USING PigStorage('\t') AS (value:chararray, cnt:int);
data = FOREACH data GENERATE value as kk, value, cnt;
STORE data INTO 'hbase://ngrams2' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('ngram:value ngram:cnt');