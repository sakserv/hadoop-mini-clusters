hadoop-mini-clusters
====================
Collection of Hadoop Mini Clusters

Includes:
---------
*   HDFS - Mini HDFS Cluster
*   YARN - Mini YARN Cluster (no MR)
*   MapReduce - Mini MapReduce Cluster
*   HBase - Mini HBase Cluster
*   Zookeeper - Curator based Local Cluster
*   HiveServer2 - Local HiveServer2 instance
*   HiveMetaStore - Derby backed HiveMetaStore
*   Storm - Storm LocalCluster
*   Kafka - Local Kafka Broker
*   Oozie - Local Oozie Server - Thanks again Vladimir
*   MongoDB - I know... not Hadoop
*   ActiveMQ - Thanks Vladimir Zlatkin!
*   HyperSQL DB - For testing various databases

Tests:
------
Tests are included to show how to configure and use each of the mini clusters.

<p align="center">
  <img src="https://travis-ci.org/sakserv/hadoop-mini-clusters.svg?branch=master"/>
</p>


Using:
------
*  Maven Central - latest release

```XML
	<dependency>
		<groupId>com.github.sakserv</groupId>
		<artifactId>hadoop-mini-clusters</artifactId>
		<version>0.0.14</version>
	</dependency>
```

Examples:
---------

*  HDFS Example
```Java
        HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster.Builder()
            .setHdfsNamenodePort(12345)
            .setHdfsTempDir("embedded_hdfs")
            .setHdfsNumDatanodes(1)
            .setHdfsEnablePermissions(false)
            .setHdfsFormat(true)
            .setHdfsEnableRunningUserAsProxyUser(true)
            .setHdfsConfig(new Configuration())
            .build();
                
        hdfsLocalCluster.start();
```

* YARN Example
```Java
        YarnLocalCluster yarnLocalCluster = new YarnLocalCluster.Builder()
            .setNumNodeManagers(1)
            .setNumLocalDirs(Integer.parseInt(1)
            .setNumLogDirs(Integer.parseInt(1)
            .setResourceManagerAddress("localhost")
            .setResourceManagerHostname("localhost:37001")
            .setResourceManagerSchedulerAddress("localhost:37002")
            .setResourceManagerResourceTrackerAddress("localhost:37003")
            .setResourceManagerWebappAddress("localhost:37004")
            .setUseInJvmContainerExecutor(false)
            .setConfig(new Configuration())
            .build();
   
        yarnLocalCluster.start();
```

* MapReduce Example
```Java
        MRLocalCluster mrLocalCluster = new MRLocalCluster.Builder()
            .setNumNodeManagers(1)
            .setJobHistoryAddress("localhost:37005")
            .setResourceManagerAddress("localhost")
            .setResourceManagerHostname("localhost:37001")
            .setResourceManagerSchedulerAddress("localhost:37002")
            .setResourceManagerResourceTrackerAddress("localhost:37003")
            .setResourceManagerWebappAddress("localhost:37004")
            .setUseInJvmContainerExecutor(false)
            .setConfig(new Configuration())
            .build();

        mrLocalCluster.start();
```

* HBase Example
```Java
        HbaseLocalCluster hbaseLocalCluster = new HbaseLocalCluster.Builder()
            .setHbaseMasterPort(25111)
            .setHbaseMasterInfoPort(-1)
            .setNumRegionServers(1)
            .setHbaseRootDir("embedded_hbase")
            .setZookeeperPort(12345)
            .setZookeeperConnectionString("localhost:12345")
            .setZookeeperZnodeParent("/hbase-unsecure")
            .setHbaseWalReplicationEnabled(false)
            .setHbaseConfiguration(new Configuration())
            .build();
            
        hbaseLocalCluster.start();
```

*  Zookeeper Example
```Java
        ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
            .setPort(12345)
            .setTempDir("embedded_zookeeper")
            .setZookeeperConnectionString("localhost:12345")
            .build();
        zookeeperLocalCluster.start();
```

*  HiveServer2 Example
```Java
        HiveLocalServer2 hiveLocalServer2 = new HiveLocalServer2.Builder()
            .setHiveServer2Hostname("localhost")
            .setHiveServer2Port(12348)
            .setHiveMetastoreHostname("localhost")
            .setHiveMetastorePort(12347)
            .setHiveMetastoreDerbyDbDir("metastore_db")
            .setHiveScratchDir("hive_scratch_dir")
            .setHiveWarehouseDir("warehouse_dir")
            .setHiveConf(new HiveConf())
            .setZookeeperConnectionString("localhost:12345")
            .build();
        hiveLocalServer2.start();
```

*  HiveMetastore Example
```Java
        HiveLocalMetaStore hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
            .setHiveMetastoreHostname("localhost")
            .setHiveMetastorePort(12347)
            .setHiveMetastoreDerbyDbDir("metastore_db")
            .setHiveScratchDir("hive_scratch_dir")
            .setHiveWarehouseDir("warehouse_dir")
            .setHiveConf(new HiveConf())
            .build();
        hiveLocalMetaStore.start();
```

*  Storm Example
```Java
        StormLocalCluster stormLocalCluster = new StormLocalCluster.Builder()
            .setZookeeperHost("localhost")
            .setZookeeperPort(12345)
            .setEnableDebug(true)
            .setNumWorkers(1)
            .setStormConfig(new Config())
            .build();
        stormLocalCluster.start();
```

*  Kafka Example
```Java
        KafkaLocalBroker kafkaLocalBroker = new KafkaLocalBroker.Builder()
            .setKafkaHostname("localhost")
            .setKafkaPort(11111)
            .setKafkaBrokerId(0)
            .setKafkaProperties(new Properties())
            .setKafkaTempDir("embedded_kafka")
            .setZookeeperConnectionString("localhost:12345")
            .build();
        kafkaLocalBroker.start();
```

*  Oozie Example
```Java
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir("embedded_oozie")
                .setOozieHomeDir("oozie_home")
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname("testgroup")
                .setOozieYarnResourceManagerAddress("localhost")
                .setOozieHdfsDefaultFs("hdfs://localhost:8020/)
                .setOozieConf(new Configuration())
                .build();
        oozieLocalServer.start();
```

*  MongoDB Example
```Java
        MongodbLocalServer mongodbLocalServer = new MongodbLocalServer.Builder()
            .setIp("127.0.0.1")
            .setPort(11112)
            .build();
        mongodbLocalServer.start();
```

*  ActiveMQ Example
```Java
        ActivemqLocalBroker amq = new ActivemqLocalBroker.Builder()
            .setHostName("localhost")
            .setPort(11113)
            .setQueueName("defaultQueue")
            .setStoreDir("activemq-data")
            .setUriPrefix("vm://")
            .setUriPostfix("?create=false")
            .build();
        amq.start();
```

*  HyperSQL DB
```Java
        hsqldbLocalServer = new HsqldbLocalServer.Builder()
            .setHsqldbHostName("127.0.0.1")
            .setHsqldbPort("44111")
            .setHsqldbTempDir("embedded_hsqldb")
            .setHsqldbDatabaseName("testdb")
            .setHsqldbCompatibilityMode("mysql")
            .setHsqldbJdbcDriver("org.hsqldb.jdbc.JDBCDriver")
            .setHsqldbJdbcConnectionStringPrefix("jdbc:hsqldb:hsql://")
            .build();
        hsqldbLocalServer.start();
```

Modifying Properties
--------------------
To change the defaults used to construct the mini clusters, modify src/main/java/resources/default.properties as needed.


Intellij Testing
----------------

If you desire running the full test suite from Intellij, make sure Fork Mode is set to method (Run -> Edit Configurations -> fork mode)


InJvmContainerExecutor
----------------------
YarnLocalCluster now supports Oleg Z's InJvmContainerExecutor. See [Oleg Z's Github](https://github.com/hortonworks/mini-dev-cluster/wiki/Core-Features) for more.
