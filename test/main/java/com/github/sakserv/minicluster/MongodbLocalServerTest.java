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
import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class MongodbLocalServerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongodbLocalServerTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static MongodbLocalServer mongodbLocalServer;

    @BeforeClass
    public static void setUp() {
        mongodbLocalServer = new MongodbLocalServer.Builder()
                .setIp(propertyParser.getProperty(ConfigVars.MONGO_IP_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)))
                .build();
    }
    
    @Test
    public void testIp() {
        assertEquals(propertyParser.getProperty(ConfigVars.MONGO_IP_KEY), mongodbLocalServer.getIp());
    }
    
    @Test
    public void testPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)),
                (int) mongodbLocalServer.getPort());
    }
}
