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

package com.github.sakserv.minicluster;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.StormLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.storm.bolt.PrinterBolt;
import com.github.sakserv.storm.spout.RandomSentenceSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StormLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(StormLocalClusterTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    ZookeeperLocalCluster zookeeperLocalCluster;
    StormLocalCluster stormCluster;

    static final String STORM_TEST_TOPOLOGY = "test";

    @Before
    public void setUp() throws IOException {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .build();
        zookeeperLocalCluster.start();

        stormCluster = new StormLocalCluster(zookeeperLocalCluster.getZkHostName(), 
                Long.parseLong(zookeeperLocalCluster.getZkPort()));
        stormCluster.start();
    }

    @After
    public void tearDown() {
        stormCluster.stop(STORM_TEST_TOPOLOGY);
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testStormCluster() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomsentencespout", new RandomSentenceSpout(), 1);
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("randomsentencespout");
        stormCluster.submitTopology(STORM_TEST_TOPOLOGY, new Config(), builder.createTopology());

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            LOG.info("SUCCESSFULLY COMPLETED");
        }
    }

}
