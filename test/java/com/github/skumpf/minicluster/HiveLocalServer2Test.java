package com.github.skumpf.minicluster;


import com.github.skumpf.minicluster.impl.HiveLocalServer2;
import com.github.skumpf.minicluster.impl.ZookeeperLocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by skumpf on 12/30/14.
 */
public class HiveLocalServer2Test {

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
        zkCluster.stop();
    }

    @Test
    public void testHiveLocalServer2() throws ClassNotFoundException, SQLException {

        // Load the Hive JDBC driver
        System.out.println("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        //
        // Create an ORC table and describe it
        //
        // Get the connection
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveServer.getHiveServerThriftPort() + "/" + HIVE_DB_NAME, "user", "pass");

        // Create the DB
        String createDbDdl = "CREATE DATABASE IF NOT EXISTS " + HIVE_DB_NAME;
        Statement stmt = con.createStatement();
        System.out.println("HIVE: Running Create Database Statement: " + createDbDdl);
        stmt.execute(createDbDdl);

        // Drop the table incase it still exists
        String dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;
        stmt = con.createStatement();
        System.out.println("HIVE: Running Drop Table Statement: " + dropDdl);
        stmt.execute(dropDdl);

        // Create the ORC table
        String createDdl = "CREATE TABLE IF NOT EXISTS " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME + " (id INT, msg STRING) " +
            "PARTITIONED BY (dt STRING) " +
            "CLUSTERED BY (id) INTO 16 BUCKETS " +
            "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
        stmt = con.createStatement();
        System.out.println("HIVE: Running Create Table Statement: " + createDdl);
        stmt.execute(createDdl);

        // Issue a describe on the new table and display the output
        System.out.println("HIVE: Validating Table was Created: ");
        ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " + HIVE_TABLE_NAME);
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i));
            }
            System.out.println();
        }

        // Drop the table
        dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;
        stmt = con.createStatement();
        System.out.println("HIVE: Running Drop Table Statement: " + dropDdl);
        stmt.execute(dropDdl);
    }

}
