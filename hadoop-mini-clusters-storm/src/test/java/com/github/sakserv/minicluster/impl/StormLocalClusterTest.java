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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

import org.apache.storm.Config;

public class StormLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(StormLocalClusterTest.class);

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

    private static StormLocalCluster stormLocalCluster;

    @BeforeClass
    public static void setUp() {
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
    }

    @Test
    public void testZookeeperHost() {
        assertEquals(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY),
                stormLocalCluster.getZookeeperHost());
    }

    @Test
    public void testMissingZookeeperHost() {
        exception.expect(IllegalArgumentException.class);
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
    }

    @Test
    public void testZookeeperPort() {
        assertEquals(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)),
                (long) stormLocalCluster.getZookeeperPort());
    }

    @Test
    public void testMissingZookeeperPort() {
        exception.expect(IllegalArgumentException.class);
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
    }

    @Test
    public void testEnableDebug() {
        assertEquals(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                stormLocalCluster.getEnableDebug());
    }

    @Test
    public void testMissingEnableDebug() {
        exception.expect(IllegalArgumentException.class);
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
    }

    @Test
    public void testNumWorkers() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)),
                (int) stormLocalCluster.getNumWorkers());
    }

    @Test
    public void testMissingNumWorkers() {
        exception.expect(IllegalArgumentException.class);
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setStormConfig(new Config())
                .build();
    }
    
    @Test
    public void testStormConf() {
        assertTrue(stormLocalCluster.getStormConf() instanceof org.apache.storm.Config);
        
    }

    @Test
    public void testMissingStormConf() {
        exception.expect(IllegalArgumentException.class);
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOST_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .build();
    }

}
