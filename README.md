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
*   HyperSQL DB - For testing various databases

Tests:
------
Tests are included to show how to configure and use each of the mini clusters.

Using:
------
*  Maven Central - latest release

```XML
	<dependency>
		<groupId>com.github.sakserv</groupId>
		<artifactId>hadoop-mini-clusters</artifactId>
		<version>0.0.12</version>
	</dependency>
```

Examples:
---------
**Starting with 0.0.11, all mini clusters have moved to the builder pattern**

**Some mini clusters depend on others (i.e. KafkaLocalBroker depends on ZookeeperLocalCluster), see tests for examples**

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
        hdfsLocalCluster.start();
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
