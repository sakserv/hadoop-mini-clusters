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

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

public class KdcLocalClusterHdfsIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterHdfsIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    private static KdcLocalCluster kdcLocalCluster;
    private static HdfsLocalCluster hdfsLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        //System.setProperty("sun.security.krb5.debug", "true");

        // KDC
        kdcLocalCluster = new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
        kdcLocalCluster.start();

        Configuration baseConf = kdcLocalCluster.getBaseConf();

        //HDFS
        Configuration hdfsConfig = new HdfsConfiguration();
        hdfsConfig.addResource(baseConf);
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsNamenodeHttpPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(hdfsConfig)
                .build();
        hdfsLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hdfsLocalCluster.stop();
        kdcLocalCluster.stop();
    }

    @Test
    public void testHdfs() throws Exception {
        FileSystem hdfsFsHandle = hdfsLocalCluster.getHdfsFileSystemHandle();

        UserGroupInformation.loginUserFromKeytab(kdcLocalCluster.getKrbPrincipalWithRealm("hdfs"), kdcLocalCluster.getKeytabForPrincipal("hdfs"));

        assertTrue(UserGroupInformation.isSecurityEnabled());
        assertTrue(UserGroupInformation.isLoginKeytabBased());

        // Write a file to HDFS containing the test string
        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY));
        reader.close();

        // Log out
        UserGroupInformation.getLoginUser().logoutUserFromKeytab();

        UserGroupInformation.reset();

        try {
            Configuration conf = new Configuration();
            UserGroupInformation.setConfiguration(conf);
            FileSystem.get(hdfsFsHandle.getUri(), conf).open(
                    new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
            fail();
        } catch (AccessControlException e) {
            LOG.info("Not authenticated!");
        }
    }
}
