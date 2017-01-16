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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

public class HbaseLocalClusterTest {
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalClusterTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HbaseLocalCluster hbaseLocalCluster;

    @Before
    public void setUp(){
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
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseMasterPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseMasterPort());
    }

    @Test
    public void testMissingHbaseMasterPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
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
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseMasterInfoPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseMasterInfoPort());
    }

    @Test
    public void testMissingHbaseMasterInfoPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseNumRegionServers() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)),
                (int) hbaseLocalCluster.getNumRegionServers());
    }

    @Test
    public void testMissingHbaseNumRegionsServers() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseRootDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY), hbaseLocalCluster.getHbaseRootDir());
    }

    @Test
    public void testMissingHbaseRootDir() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testZookeeperPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)),
                (int) hbaseLocalCluster.getZookeeperPort());
    }

    @Test
    public void testMissingZookeeperPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testZookeeperConnectionString() {
        assertEquals(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                hbaseLocalCluster.getZookeeperConnectionString());
    }

    @Test
    public void testMissingZookeeperConnectionString() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testZookeeperZnodeParent() {
        assertEquals(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY),
                hbaseLocalCluster.getZookeeperZnodeParent());
    }

    @Test
    public void testMissingZookeeperZnodeParent() {
        exception.expect(IllegalArgumentException.class);
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
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseWalReplicationEnabled() {
        assertEquals(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)),
                (boolean) hbaseLocalCluster.getHbaseWalReplicationEnabled());
    }

    @Test
    public void testMissingHbaseWalReplicationEnabled() {
        exception.expect(IllegalArgumentException.class);
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
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHdfsConf() {
        assertTrue(hbaseLocalCluster.getHbaseConfiguration() instanceof org.apache.hadoop.conf.Configuration);

    }

    @Test
    public void testMissingHdfsConf() {
        exception.expect(IllegalArgumentException.class);
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
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }


    @Test
    public void testHbaseRestPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestPort());
    }

    @Test
    public void testMissingHbaseRestPort() {
        exception.expect(IllegalArgumentException.class);
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestInfoPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
    }

    @Test
    public void testHbaseRestInfoPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_REST_INFO_PORT_KEY)),
                (int) hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestInfoPort());
    }

    @Test
    public void testHbaseRestHost() {
        assertEquals(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestHost());
    }

    @Test
    public void testHbaseRestReadOnly() {
        assertEquals(Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestReadOnly());
    }

    @Test
    public void testHbaseRestThreadMax() {
        assertEquals(Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestThreadMax());
    }

    @Test
    public void testHbaseRestThreadMin() {
        assertEquals(Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)),
                hbaseLocalCluster.getHbaseRestLocalCluster().getHbaseRestThreadMin());
    }

}
