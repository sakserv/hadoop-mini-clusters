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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import com.github.sakserv.propertyparser.PropertyParser;

public class HiveLocalMetaStoreIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HiveLocalMetaStoreIntegrationTest.class);

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
    
    private static HiveLocalMetaStore hiveLocalMetaStore;

    @BeforeClass
    public static void setUp() throws Exception {

        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
        
        hiveLocalMetaStore.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hiveLocalMetaStore.stop();
        FileUtils.deleteFolder(new File(
                propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY)).getAbsolutePath());
    }

    public static HiveConf buildHiveConf() {

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
        return hiveConf;
    }

    @Test
    public void testHiveLocalMetaStore() {

        // Create a table and display it back
        try {
            HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveLocalMetaStore.getHiveConf());

            hiveClient.dropTable(propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY), 
                    propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY), true, true);

            // Define the cols
            List<FieldSchema> cols = new ArrayList<FieldSchema>();
            cols.add(new FieldSchema("id", serdeConstants.INT_TYPE_NAME, ""));
            cols.add(new FieldSchema("msg", serdeConstants.STRING_TYPE_NAME, ""));

            // Values for the StorageDescriptor
            String location = new File(propertyParser.getProperty(
                    ConfigVars.HIVE_TEST_TABLE_NAME_KEY)).getAbsolutePath();
            String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            int numBuckets = 16;
            Map<String,String> orcProps = new HashMap<String, String>();
            orcProps.put("orc.compress", "NONE");
            SerDeInfo serDeInfo = new SerDeInfo(OrcSerde.class.getSimpleName(), OrcSerde.class.getName(), orcProps);
            List<String> bucketCols = new ArrayList<String>();
            bucketCols.add("id");

            // Build the StorageDescriptor
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(cols);
            sd.setLocation(location);
            sd.setInputFormat(inputFormat);
            sd.setOutputFormat(outputFormat);
            sd.setNumBuckets(numBuckets);
            sd.setSerdeInfo(serDeInfo);
            sd.setBucketCols(bucketCols);
            sd.setSortCols(new ArrayList<Order>());
            sd.setParameters(new HashMap<String, String>());

            // Define the table
            Table tbl = new Table();
            tbl.setDbName(propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY));
            tbl.setTableName(propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY));
            tbl.setSd(sd);
            tbl.setOwner(System.getProperty("user.name"));
            tbl.setParameters(new HashMap<String, String>());
            tbl.setViewOriginalText("");
            tbl.setViewExpandedText("");
            tbl.setTableType(TableType.EXTERNAL_TABLE.name());
            List<FieldSchema> partitions = new ArrayList<FieldSchema>();
            partitions.add(new FieldSchema("dt", serdeConstants.STRING_TYPE_NAME, ""));
            tbl.setPartitionKeys(partitions);

            // Create the table
            hiveClient.createTable(tbl);

            // Describe the table
            Table createdTable = hiveClient.getTable(
                    propertyParser.getProperty(ConfigVars.HIVE_TEST_DATABASE_NAME_KEY),
                    propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY));
            LOG.info("HIVE: Created Table: {}", createdTable.toString());
            assertThat(createdTable.toString(), 
                    containsString(propertyParser.getProperty(ConfigVars.HIVE_TEST_TABLE_NAME_KEY)));

        } catch(MetaException e) {
            e.printStackTrace();
        } catch(TException e) {
            e.printStackTrace();
        }

    }

}
