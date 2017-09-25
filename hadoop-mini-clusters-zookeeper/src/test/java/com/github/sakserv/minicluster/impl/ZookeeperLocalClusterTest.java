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

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ZookeeperLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLocalClusterTest.class);

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

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws IOException {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setMaxClientCnxns(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_MAX_CLIENT_CNXNS_KEY)))
                .setElectionPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_ELECTION_PORT_KEY)))
                .setQuorumPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_QUORUM_PORT_KEY)))
                .setDeleteDataDirectoryOnClose(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.ZOOKEEPER_DELETE_DATA_DIRECTORY_ON_CLOSE_KEY)))
                .setServerId(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_SERVER_ID_KEY)))
                .setTickTime(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TICKTIME_KEY)))
                .build();
    }

    @Test
    public void testPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)),
                zookeeperLocalCluster.getPort());
    }

    @Test
    public void testMissingPort() {
        exception.expect(IllegalArgumentException.class);
        ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
    }

    @Test
    public void testTempDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY), zookeeperLocalCluster.getTempDir());
    }

    @Test
    public void testMissingTempDir() {
        exception.expect(IllegalArgumentException.class);
        ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
    }

    @Test
    public void testZookeeperConnectionString() {
        assertEquals(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                zookeeperLocalCluster.getZookeeperConnectionString());
    }

    @Test
    public void testMissingZookeeperConnectionString() {
        exception.expect(IllegalArgumentException.class);
        ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .build();
    }

    @Test
    public void testEmptyConfigure() throws Exception {
        ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.configure();
    }

    @Test
    public void testMaxClientCnxns() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_MAX_CLIENT_CNXNS_KEY)),
                zookeeperLocalCluster.getMaxClientCnxns());
    }

    @Test
    public void testElectionPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_ELECTION_PORT_KEY)),
                zookeeperLocalCluster.getElectionPort());
    }

    @Test
    public void testQuorumPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_QUORUM_PORT_KEY)),
                zookeeperLocalCluster.getQuorumPort());
    }

    @Test
    public void testDeleteDataDirectoryOnClose() {
        assertEquals(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.ZOOKEEPER_DELETE_DATA_DIRECTORY_ON_CLOSE_KEY)),
                zookeeperLocalCluster.getDeleteDataDirectoryOnClose());
    }

    @Test
    public void testServerId() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_SERVER_ID_KEY)),
                zookeeperLocalCluster.getServerId());
    }

    @Test
    public void testTicktime() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TICKTIME_KEY)),
                zookeeperLocalCluster.getTickTime());
    }


}
