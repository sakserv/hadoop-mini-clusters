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

Tests:
------
Tests are included to show how to configure and use each of the mini clusters.

Using:
------
**IMPORTANT NOTE: APIs are still stabilizing, expect changes

*  Maven Central - latest release


	<dependency>
		<groupId>com.github.sakserv</groupId>
		<artifactId>hadoop-mini-clusters</artifactId>
		<version>0.0.8</version>
	</dependency>
