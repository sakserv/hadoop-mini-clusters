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
import com.mycila.xmltool.XMLDoc;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Vincent Devillers
 */
public class KnoxLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KnoxLocalClusterTest.class);

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

    private static KnoxLocalCluster knoxLocalCluster;

    @BeforeClass
    public static void setUp() {
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY)))
                .setPath(propertyParser.getProperty(ConfigVars.KNOX_PATH_KEY))
                .setHomeDir(propertyParser.getProperty(ConfigVars.KNOX_HOME_DIR_KEY))
                .setCluster(propertyParser.getProperty(ConfigVars.KNOX_CLUSTER_KEY))
                .setTopology(XMLDoc.newDocument(true)
                        .addRoot("topology")
                        .addTag("service")
                        .addTag("role").addText("WEBHDFS")
                        .addTag("url").addText("http://localhost:20112/webhdfs")
                        .gotoRoot().toString())
                .build();
    }

    @Test
    public void testKnoxPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY)),
                (int) knoxLocalCluster.getPort());
    }

    @Test
    public void testMissingKnoxPort() {
        exception.expect(IllegalArgumentException.class);
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setHomeDir(propertyParser.getProperty(ConfigVars.KNOX_HOME_DIR_KEY))
                .build();
    }

    @Test
    public void testKnoxPath() {
        assertEquals(propertyParser.getProperty(ConfigVars.KNOX_PATH_KEY),
                knoxLocalCluster.getPath());
    }

    @Test
    public void testMissingKnoxPath() {
        exception.expect(IllegalArgumentException.class);
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setHomeDir(propertyParser.getProperty(ConfigVars.KNOX_HOME_DIR_KEY))
                .build();
    }

    @Test
    public void testKnoxTempDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.KNOX_HOME_DIR_KEY),
                knoxLocalCluster.getHomeDir());
    }

    @Test
    public void testMissingKnoxTempDir() {
        exception.expect(IllegalArgumentException.class);
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY)))
                .build();
    }

    @Test
    public void testKnoxCluster() {
        assertEquals(propertyParser.getProperty(ConfigVars.KNOX_CLUSTER_KEY),
                knoxLocalCluster.getCluster());
    }

    @Test
    public void testMissingKnoxCluster() {
        exception.expect(IllegalArgumentException.class);
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KNOX_CLUSTER_KEY)))
                .build();
    }
}
