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

import java.io.IOException;

import org.apache.storm.utils.NimbusClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.storm.bolt.PrinterBolt;
import com.github.sakserv.minicluster.storm.spout.RandomSentenceSpout;
import com.github.sakserv.propertyparser.PropertyParser;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

import static org.junit.Assert.assertTrue;

public class StormLocalClusterIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(StormLocalClusterIntegrationTest.class);

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

    private static ZookeeperLocalCluster zookeeperLocalCluster;
    private static StormLocalCluster stormLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.start();

        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
        stormLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testStormCluster() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomsentencespout", new RandomSentenceSpout(), 1);
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("randomsentencespout");
        stormLocalCluster.submitTopology(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY), 
                stormLocalCluster.getStormConf(), builder.createTopology());

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            LOG.info("SUCCESSFULLY COMPLETED");
        }
    }

    @Test
    public void testStormNimbusClient() throws Exception {
        Config conf = stormLocalCluster.getStormConf();
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        assertTrue(nimbusClient.getClient().getNimbusConf().length() > 0);
    }

}
