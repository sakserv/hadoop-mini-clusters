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
import java.sql.SQLException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

public class HiveLocalMetaStoreTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HiveLocalMetaStoreTest.class);

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
    
    private static HiveLocalMetaStore hiveLocalMetaStore;

    @BeforeClass
    public static void setUp() {
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }
    
    
    public static HiveConf buildHiveConf() {
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
    public void testHiveMetastoreHostname() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY),
                hiveLocalMetaStore.getHiveMetastoreHostname());
    }

    @Test
    public void testMissingHiveMetastoreHostname() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }

    @Test
    public void testHiveMetastorePort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)),
                (int) hiveLocalMetaStore.getHiveMetastorePort());
    }

    @Test
    public void testMissingHivemetastorePort() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }

    @Test
    public void testHiveMetastoreDerbyDbDir() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY),
                hiveLocalMetaStore.getHiveMetastoreDerbyDbDir());
    }

    @Test
    public void testMissingHiveMetastoreDerbyDbDir() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }

    @Test
    public void testHiveScratchDir() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY),
                hiveLocalMetaStore.getHiveScratchDir());
    }

    @Test
    public void testMissingHiveScratchDir() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }

    @Test
    public void testHiveWarehouseDir() {
        Assert.assertEquals(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY),
                hiveLocalMetaStore.getHiveWarehouseDir());
    }

    @Test
    public void testMissingHiveWarehouseDir() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
    }
    
    @Test
    public void testHiveConf() {
        assertTrue(hiveLocalMetaStore.getHiveConf() instanceof org.apache.hadoop.hive.conf.HiveConf);

    }

    @Test
    public void testMissingHiveConf() {
        exception.expect(IllegalArgumentException.class);
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .build();
    }

    @Test
    public void testThrowableOnStart() throws Exception {
        //exception.expect(SQLException.class);
        HiveLocalMetaStore hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(99999999)
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(buildHiveConf())
                .build();
        try {
            hiveLocalMetaStore.start();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        hiveLocalMetaStore.cleanUp();
    }

}
