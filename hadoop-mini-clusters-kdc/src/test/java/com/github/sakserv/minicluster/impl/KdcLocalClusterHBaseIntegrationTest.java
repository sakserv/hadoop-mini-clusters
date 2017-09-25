/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.auth.Jaas;
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KdcLocalClusterHBaseIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterHBaseIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static KdcLocalCluster kdcLocalCluster;
    private static HbaseLocalCluster hbaseLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        //System.setProperty("sun.security.krb5.debug", "true");

        // Force clean
        FileUtils.deleteFolder(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY));
        FileUtils.deleteFolder(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY));
        FileUtils.deleteFolder(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY));

        // KDC
        kdcLocalCluster = new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
        kdcLocalCluster.start();

        Configuration baseConf = kdcLocalCluster.getBaseConf();

        // Zookeeper
        Jaas jaas = new Jaas()
                .addServiceEntry("Server", kdcLocalCluster.getKrbPrincipal("zookeeper"), kdcLocalCluster.getKeytabForPrincipal("zookeeper"), "zookeeper");
        javax.security.auth.login.Configuration.setConfiguration(jaas);

        Map<String, Object> properties = new HashMap<>();
        properties.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        properties.put("requireClientAuthScheme", "sasl");
        properties.put("sasl.serverconfig", "Server");
        properties.put("kerberos.removeHostFromPrincipal", "true");
        properties.put("kerberos.removeRealmFromPrincipal", "true");

        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setCustomProperties(properties)
                .build();
        zookeeperLocalCluster.start();

        // HBase
        UserGroupInformation.setConfiguration(baseConf);

        System.setProperty("zookeeper.sasl.client", "true");
        System.setProperty("zookeeper.sasl.clientconfig", "Client");
        javax.security.auth.login.Configuration.setConfiguration(new Jaas()
                .addEntry("Client", kdcLocalCluster.getKrbPrincipalWithRealm("hbase"), kdcLocalCluster.getKeytabForPrincipal("hbase")));

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperLocalCluster.getZookeeperConnectionString(),
                new ExponentialBackoffRetry(1000, 3))) {
            client.start();

            List<ACL> perms = new ArrayList<>();
            perms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
            perms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

            client.create().withMode(CreateMode.PERSISTENT).withACL(perms).forPath(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY));
        }

        Jaas jaasHbaseClient = new Jaas()
                .addEntry("Client", kdcLocalCluster.getKrbPrincipalWithRealm("hbase"), kdcLocalCluster.getKeytabForPrincipal("hbase"));
        javax.security.auth.login.Configuration.setConfiguration(jaasHbaseClient);
        File jaasHbaseClientFile = new File(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY), "hbase-client.jaas");
        org.apache.commons.io.FileUtils.writeStringToFile(jaasHbaseClientFile, jaasHbaseClient.toFile());

        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.addResource(baseConf);

        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(hbaseConfig)
                .build();
        hbaseLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseLocalCluster.stop();
        zookeeperLocalCluster.stop();
        kdcLocalCluster.stop();
    }

    @Test
    public void testHBase() throws Exception {
        UserGroupInformation.loginUserFromKeytab(kdcLocalCluster.getKrbPrincipalWithRealm("hbase"), kdcLocalCluster.getKeytabForPrincipal("hbase"));

        assertTrue(UserGroupInformation.isSecurityEnabled());
        assertTrue(UserGroupInformation.isLoginKeytabBased());

        Configuration configuration = hbaseLocalCluster.getHbaseConfiguration();
        configuration.set("hbase.client.retries.number", "1");
        configuration.set("hbase.client.pause", "1000");
        configuration.set("zookeeper.recovery.retry", "1");

        // Write data
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf("test-kdc");
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("cf")));

            try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
                mutator.mutate(new Put(Bytes.toBytes("key")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col1"), Bytes.toBytes("azerty")));
            }
        }

        // Log out
        LOG.info("Logout...");
        UserGroupInformation.getLoginUser().logoutUserFromKeytab();
        UserGroupInformation.reset();

        try {
            Configuration unauthenticatedConfiguration = HBaseConfiguration.create();
            hbaseLocalCluster.configure(unauthenticatedConfiguration);
            unauthenticatedConfiguration.set("hbase.client.retries.number", "1");
            unauthenticatedConfiguration.set("hbase.client.pause", "1000");
            unauthenticatedConfiguration.set("zookeeper.recovery.retry", "1");

            UserGroupInformation.setConfiguration(unauthenticatedConfiguration);
            try (Connection connection = ConnectionFactory.createConnection(unauthenticatedConfiguration)) {
                Admin admin = connection.getAdmin();

                TableName tableName = TableName.valueOf("test-kdc2");
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }

                try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
                    mutator.mutate(new Put(Bytes.toBytes("key")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col1"), Bytes.toBytes("azerty")));
                }
            }
            fail();
        } catch (RetriesExhaustedException e) {
            LOG.info("Alright, this is expected!", e);
            assertTrue(e.getCause() instanceof IOException);
            System.out.println("Not authenticated!");
        }
    }
}