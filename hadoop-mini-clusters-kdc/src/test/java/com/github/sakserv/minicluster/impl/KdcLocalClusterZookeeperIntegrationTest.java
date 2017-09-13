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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class KdcLocalClusterZookeeperIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterZookeeperIntegrationTest.class);

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
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        //System.setProperty("sun.security.krb5.debug", "true");

        // Force clean
        FileUtils.deleteFolder(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY));

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
    }

    @AfterClass
    public static void tearDown() throws Exception {
        zookeeperLocalCluster.stop();
        kdcLocalCluster.stop();
    }

    @Test
    public void testZookeeper() throws Exception {

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperLocalCluster.getZookeeperConnectionString(),
                new ExponentialBackoffRetry(1000, 3))) {
            client.start();
            client.getChildren().forPath("/");
            fail();
        } catch (KeeperException.AuthFailedException e) {
            LOG.debug("Not authenticated!");
        }

        System.setProperty("zookeeper.sasl.client", "true");
        System.setProperty("zookeeper.sasl.clientconfig", "Client");
        javax.security.auth.login.Configuration.setConfiguration(new Jaas()
                .addEntry("Client", kdcLocalCluster.getKrbPrincipalWithRealm("guest"), kdcLocalCluster.getKeytabForPrincipal("guest")));

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperLocalCluster.getZookeeperConnectionString(),
                new ExponentialBackoffRetry(1000, 3))) {
            client.start();
            client.getChildren().forPath("/").forEach(LOG::debug);

            List<ACL> perms = new ArrayList<>();
            perms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
            perms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

            client.create().withMode(CreateMode.PERSISTENT).withACL(perms).forPath(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY));
        }
    }
}
