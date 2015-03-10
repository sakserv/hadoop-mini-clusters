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

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.MRLocalCluster;
import com.github.sakserv.minicluster.impl.YarnLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MRLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MRLocalClusterTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static MRLocalCluster mrLocalCluster;

    @BeforeClass
    public static void setUp() throws IOException {
        mrLocalCluster = new MRLocalCluster.Builder()
                .setNumNodeManagers(Integer.parseInt(propertyParser.getProperty(ConfigVars.MR_NUM_NODE_MANAGERS_KEY)))
                .setYarnConfig(new Configuration())
                .build();
    }

    @Test
    public void testNumNodeManagers() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.MR_NUM_NODE_MANAGERS_KEY)),
                (int) mrLocalCluster.getNumNodeManagers());
    }

    @Test
    public void testYarnConf() {
        assertTrue(mrLocalCluster.getYarnConfig() instanceof Configuration);

    }
}
