package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OozieLocalServerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServerTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static OozieLocalServer oozieLocalServer;
    private static HdfsLocalCluster hdfsLocalCluster;
    private static String defaultFs;

    @BeforeClass
    public static void setUp()  {


        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsConfig(new Configuration())
                .build();

        defaultFs = "hdfs://localhost:" + propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY);
        oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        oozieLocalServer.stop();
    }

    @Test
    public void testOozieTestDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY), oozieLocalServer.getOozieTestDir());
    }

    @Test
    public void testMissingOozieTestDir() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieHomeDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY), oozieLocalServer.getOozieHomeDir());
    }

    @Test
    public void testMissingOozieHomeDir() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieUsername() {
        assertEquals(System.getProperty("user.name"), oozieLocalServer.getOozieUsername());
    }

    @Test
    public void testMissingOozieUsername() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieGroupname() {
        assertEquals(System.getProperty("user.name"), oozieLocalServer.getOozieUsername());
    }

    @Test
    public void testMissingOozieGroupname() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieYarnResourceManagerAddress() {
        assertEquals(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY),
                oozieLocalServer.getOozieYarnResourceManagerAddress());
    }

    @Test
    public void testMissingOozieYarnResourceManagerAddress() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieHdfsDefaultFs() {
        assertEquals(defaultFs,
                oozieLocalServer.getOozieHdfsDefaultFs());
    }

    @Test
    public void testMissingOozieHdfsDefaultFs() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieConf(new Configuration())
                .build();
    }

    @Test
    public void testOozieConf() {
        assertTrue(oozieLocalServer.getOozieConf() instanceof Configuration);
    }

    @Test
    public void testMissingOozieConf() {
        exception.expect(IllegalArgumentException.class);
        OozieLocalServer oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(defaultFs)
                .build();
    }
}
