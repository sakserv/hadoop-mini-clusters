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
import com.github.sakserv.minicluster.impl.HsqldbLocalServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.assertEquals;

public class HsqldbLocalServerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HsqldbLocalServerTest.class);


    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    private static HsqldbLocalServer hsqldbLocalServer;
    
    @BeforeClass
    public static void setUp() {
        hsqldbLocalServer = new HsqldbLocalServer.Builder()
            .setHsqldbHostName(propertyParser.getProperty(ConfigVars.HSQLDB_HOSTNAME_KEY))
            .setHsqldbPort(propertyParser.getProperty(ConfigVars.HSQLDB_PORT_KEY))
            .setHsqldbTempDir(propertyParser.getProperty(ConfigVars.HSQLDB_TEMP_DIR_KEY))
            .setHsqldbDatabaseName(propertyParser.getProperty(ConfigVars.HSQLDB_DATABASE_NAME_KEY))
            .setHsqldbCompatibilityMode(propertyParser.getProperty(ConfigVars.HSQLDB_COMPATIBILITY_MODE_KEY))
            .setHsqldbJdbcDriver(propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_DRIVER_KEY))
            .setHsqldbJdbcConnectionStringPrefix(propertyParser.getProperty(
                    ConfigVars.HSQLDB_JDBC_CONNECTION_STRING_PREFIX_KEY))
            .build();
    }

    @Test
    public void testHsqldbHostName() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_HOSTNAME_KEY), hsqldbLocalServer.getHsqldbHostName());
    }

    @Test
    public void testHsqldbPort() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_PORT_KEY), hsqldbLocalServer.getHsqldbPort());
    }

    @Test
    public void testHsqldbTempDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_TEMP_DIR_KEY), hsqldbLocalServer.getHsqldbTempDir());
    }

    @Test
    public void testHsqldbDatabaseName() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_DATABASE_NAME_KEY), 
                hsqldbLocalServer.getHsqldbDatabaseName());
    }

    @Test
    public void testHsqldbCompatibilityMode() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_COMPATIBILITY_MODE_KEY), 
                hsqldbLocalServer.getHsqldbCompatibilityMode());
    }

    @Test
         public void testHsqldbJdbcDriver() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_DRIVER_KEY),
                hsqldbLocalServer.getHsqldbJdbcDriver());
    }

    @Test
    public void testHsqldbJdbcConnectionStringPrefix() {
        assertEquals(propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_CONNECTION_STRING_PREFIX_KEY),
                hsqldbLocalServer.getHsqldbJdbcConnectionStringPrefix());
    }
}
