hadoop-mini-clusters
====================
Collection of Hadoop Mini Clusters for Hortonworks Data Platform (HDP) 2.2.0.0

Includes:
---------
*   HDFS - Mini HDFS Cluster
*   Zookeeper - Curator based Local Cluster
*   HiveServer2 - Local HiveServer2 instance
*   HiveMetaStore - Derby backed HiveMetaStore
*   Storm - Storm LocalCluster
*   Kafka - Local Kafka Broker
*   MongoDB - I know... not Hadoop
*   ActiveMQ - Thanks Vladimir Zlatkin!

Tests:
------
Tests are included to show how to configure and use each of the mini clusters.

Using:
------
**IMPORTANT NOTE: APIs are still stabilizing, expect changes**
**Starting with 0.0.10, all mini clusters have moved to the builder pattern**

*  Maven Central - latest release

```XML
	<dependency>
		<groupId>com.github.sakserv</groupId>
		<artifactId>hadoop-mini-clusters</artifactId>
		<version>0.0.10</version>
	</dependency>
```

*  HDFS Example
```Java
        HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(12345)
                .setHdfsTempDir("embedded_hdfs")
                .setHdfsNumDatanodes(1)
                .setHdfsEnablePermissions(false)
                .setHdfsFormat(true)
                .setHdfsConfig(new Configuration())
                .build();
```