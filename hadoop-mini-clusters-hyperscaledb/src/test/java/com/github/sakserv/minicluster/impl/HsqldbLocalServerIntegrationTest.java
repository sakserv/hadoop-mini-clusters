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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

public class HsqldbLocalServerIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HsqldbLocalServerIntegrationTest.class);

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
    
    private static HsqldbLocalServer hsqldbLocalServer;
    
    @BeforeClass
    public static void setUp() throws Exception {
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
        hsqldbLocalServer.start();
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        hsqldbLocalServer.stop();
    }
    
    @Test
    public void testHsqldbLocalServer() throws ClassNotFoundException, SQLException {

        LOG.info("HSQLDB: Running User: {}", System.getProperty("user.name"));

        LOG.info("HSQLDB: Loading the JDBC Driver: {}", propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_DRIVER_KEY));
        Class.forName(propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_DRIVER_KEY));

        // Get the connection
        Connection connection = DriverManager.getConnection(
                propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_CONNECTION_STRING_PREFIX_KEY) + 
                propertyParser.getProperty(ConfigVars.HSQLDB_HOSTNAME_KEY) + ":" +
                propertyParser.getProperty(ConfigVars.HSQLDB_PORT_KEY) + "/" +
                propertyParser.getProperty(ConfigVars.HSQLDB_DATABASE_NAME_KEY),
                "SA", "");
        assertThat(connection.getMetaData().getURL(),
                containsString(propertyParser.getProperty(ConfigVars.HSQLDB_DATABASE_NAME_KEY)));
    }
    
    @Test
    public void testHsqldbMysqlCompatibilityMode() throws SQLException {
        Connection connection = DriverManager.getConnection(
                propertyParser.getProperty(ConfigVars.HSQLDB_JDBC_CONNECTION_STRING_PREFIX_KEY) +
                        propertyParser.getProperty(ConfigVars.HSQLDB_HOSTNAME_KEY) + ":" +
                        propertyParser.getProperty(ConfigVars.HSQLDB_PORT_KEY) + "/" +
                        propertyParser.getProperty(ConfigVars.HSQLDB_DATABASE_NAME_KEY),
                "SA", "");
        Statement statement = connection.createStatement();
        statement.executeQuery(hsqldbLocalServer.getHsqldbCompatibilityModeStatement());
        
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT PROPERTY_VALUE FROM INFORMATION_SCHEMA.SYSTEM_PROPERTIES WHERE PROPERTY_NAME = 'sql.syntax_mys'");
        while(resultSet.next()) {
            assertTrue(Boolean.parseBoolean(resultSet.getString(1)));
        }
    }
}
