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

import com.github.sakserv.minicluster.impl.HiveLocalServer2;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class HiveLocalServer2Test {

    // Logger
    private static final Logger LOG = Logger.getLogger(HiveLocalServer2Test.class);

    private static final int ZOOKEEPER_PORT = 2181;

    private static final String METASTORE_URI = "";
    private static final String DERBY_DB_PATH = "metastore_db";
    private static final String HIVE_SCRATCH_DIR = "hive_scratch_dir";
    private static final int HIVESERVER2_PORT = 10000;

    private static final String HIVE_DB_NAME = "testdb";
    private static final String HIVE_TABLE_NAME = "testtable";

    ZookeeperLocalCluster zkCluster;
    HiveLocalServer2 hiveServer;

    @Before
    public void setUp() {

        zkCluster = new ZookeeperLocalCluster(ZOOKEEPER_PORT);
        zkCluster.start();

        hiveServer = new HiveLocalServer2(METASTORE_URI, DERBY_DB_PATH, HIVE_SCRATCH_DIR,
                HIVESERVER2_PORT, zkCluster.getZkConnectionString());
        hiveServer.start();
    }

    @After
    public void tearDown() {
        hiveServer.stop(true);
        zkCluster.stop(true);
    }

    @Test
    public void testHiveLocalServer2() throws ClassNotFoundException, SQLException {

        // Load the Hive JDBC driver
        LOG.info("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        //
        // Create an ORC table and describe it
        //
        // Get the connection
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveServer.getHiveServerThriftPort() + "/" + HIVE_DB_NAME, "user", "pass");

        // Create the DB
        String createDbDdl = "CREATE DATABASE IF NOT EXISTS " + HIVE_DB_NAME;
        Statement stmt = con.createStatement();
        LOG.info("HIVE: Running Create Database Statement: " + createDbDdl);
        stmt.execute(createDbDdl);

        // Drop the table incase it still exists
        String dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;
        stmt = con.createStatement();
        LOG.info("HIVE: Running Drop Table Statement: " + dropDdl);
        stmt.execute(dropDdl);

        // Create the ORC table
        String createDdl = "CREATE TABLE IF NOT EXISTS " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME + " (id INT, msg STRING) " +
            "PARTITIONED BY (dt STRING) " +
            "CLUSTERED BY (id) INTO 16 BUCKETS " +
            "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
        stmt = con.createStatement();
        LOG.info("HIVE: Running Create Table Statement: " + createDdl);
        stmt.execute(createDdl);

        // Issue a describe on the new table and display the output
        LOG.info("HIVE: Validating Table was Created: ");
        ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " + HIVE_TABLE_NAME);
        int count = 0;
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i));
            }
            System.out.println();
            count++;
        }
        assertEquals(33, count);

        // Drop the table
        dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;
        stmt = con.createStatement();
        LOG.info("HIVE: Running Drop Table Statement: " + dropDdl);
        stmt.execute(dropDdl);
    }

}
