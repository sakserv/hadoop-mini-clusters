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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

public class HbaseLocalClusterIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalClusterIntegrationTest.class);

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
    
    private static HbaseLocalCluster hbaseLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
            .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
            .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
            .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
            .build();
        zookeeperLocalCluster.start();

        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
                .setHbaseMasterInfoPort(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
                .setNumRegionServers(
                        Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
                .setHbaseRootDir(propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
                .setZookeeperPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .setZookeeperZnodeParent(propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
                .setHbaseWalReplicationEnabled(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
                .setHbaseConfiguration(new Configuration())
                .activeRestGateway()
                    .setHbaseRestHost(propertyParser.getProperty(ConfigVars.HBASE_REST_HOST_KEY))
                    .setHbaseRestPort(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)))
                    .setHbaseRestReadOnly(
                            Boolean.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_READONLY_KEY)))
                    .setHbaseRestThreadMax(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMAX_KEY)))
                    .setHbaseRestThreadMin(
                            Integer.valueOf(propertyParser.getProperty(ConfigVars.HBASE_REST_THREADMIN_KEY)))
                    .build()
                .build();
        hbaseLocalCluster.start();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseLocalCluster.stop();
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testHbaseLocalCluster() throws Exception {

        String tableName = propertyParser.getProperty(ConfigVars.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_TEST_NUM_ROWS_TO_PUT_KEY));
        Configuration configuration = hbaseLocalCluster.getHbaseConfiguration();

        LOG.info("HBASE: Deleting table {}", tableName);
        deleteHbaseTable(tableName, configuration);

        LOG.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
        createHbaseTable(tableName, colFamName, configuration);

        LOG.info("HBASE: Populate the table with {} rows.", numRowsToPut);
        for (int i=0; i<numRowsToPut; i++) {
            putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, configuration);
        }

        LOG.info("HBASE: Fetching and comparing the results");
        for (int i=0; i<numRowsToPut; i++) {
            Result result = getRow(tableName, colFamName, String.valueOf(i), colQualiferName, configuration);
            assertEquals("row_" + i, new String(result.value()));
        }

    }

    @Test
    public void testHbaseRestLocalCluster() throws Exception {
        URL url = new URL(
                String.format("http://localhost:%s/status/cluster/",
                        propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains("2 live servers, 0 dead servers"));
        }

    }

    @Test
    public void testHbaseRestLocalClusterWithSchemaRequest() throws Exception {
        String tableName = propertyParser.getProperty(ConfigVars.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = propertyParser.getProperty(ConfigVars.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_TEST_NUM_ROWS_TO_PUT_KEY));
        Configuration configuration = hbaseLocalCluster.getHbaseConfiguration();

        LOG.info("HBASE: Deleting table {}", tableName);
        deleteHbaseTable(tableName, configuration);

        LOG.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
        createHbaseTable(tableName, colFamName, configuration);

        LOG.info("HBASE: Populate the table with {} rows.", numRowsToPut);
        for (int i = 0; i < numRowsToPut; i++) {
            putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, configuration);
        }

        URL url = new URL(String.format("http://localhost:%s/",
                propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains(tableName));
        }

        url = new URL(String.format("http://localhost:%s/%s/schema",
                propertyParser.getProperty(ConfigVars.HBASE_REST_PORT_KEY),
                tableName));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains("{ NAME=> 'hbase_test_table', IS_META => 'false', COLUMNS => [ { NAME => 'cf1', BLOOMFILTER => 'ROW'"));
        }

    }

    private static void createHbaseTable(String tableName, String colFamily,
                                         Configuration configuration) throws Exception {

        final HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);

        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }

    private static void deleteHbaseTable(String tableName, Configuration configuration) throws Exception {

        final HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private static void putRow(String tableName, String colFamName, String rowKey, String colQualifier, String value,
                                 Configuration configuration) throws Exception {
        HTable table = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    private static Result getRow(String tableName, String colFamName, String rowKey, String colQualifier,
                               Configuration configuration) throws Exception {
        Result result;
        HTable table = new HTable(configuration, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier));
        get.setMaxVersions(1);
        result = table.get(get);
        return result;
    }

}
