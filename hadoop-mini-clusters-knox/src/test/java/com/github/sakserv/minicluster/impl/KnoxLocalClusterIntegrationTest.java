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
import com.mycila.xmltool.XMLDoc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertEquals;

/**
 * @author Vincent Devillers
 */
public class KnoxLocalClusterIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KnoxLocalClusterIntegrationTest.class);

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

    private static HdfsLocalCluster dfsCluster;
    private static KnoxLocalCluster knoxCluster;

    @BeforeClass
    public static void setUp() throws Exception {

        // We need HDFS/WEBHDFS for Knox
        dfsCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsNamenodeHttpPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(new Configuration())
                .build();
        dfsCluster.start();

        knoxCluster = new KnoxLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY)))
                .setPath(propertyParser.getProperty(ConfigVars.KNOX_PATH_KEY))
                .setHomeDir(propertyParser.getProperty(ConfigVars.KNOX_HOME_DIR_KEY))
                .setCluster(propertyParser.getProperty(ConfigVars.KNOX_CLUSTER_KEY))
                .setTopology(XMLDoc.newDocument(true)
                        .addRoot("topology")
                            .addTag("gateway")
                                .addTag("provider")
                                    .addTag("role").addText("authentication")
                                    .addTag("enabled").addText("false")
                                    .gotoParent()
                                .addTag("provider")
                                    .addTag("role").addText("identity-assertion")
                                    .addTag("enabled").addText("false")
                                    .gotoParent().gotoParent()
                            .addTag("service")
                                .addTag("role").addText("NAMENODE")
                                .addTag("url").addText("hdfs://localhost:" + propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY))
                                .gotoParent()
                            .addTag("service")
                                .addTag("role").addText("WEBHDFS")
                                .addTag("url").addText("http://localhost:" + propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY) + "/webhdfs")
                        .gotoRoot().toString())
                .build();

        knoxCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        knoxCluster.stop();
    }

    @Test
    public void testKnoxWithWebhdfs() throws Exception {

        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        try (FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)))) {
            writer.write(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY).getBytes("UTF-8"));
            writer.flush();
        }

        // Read the file throught webhdfs
        URL url = new URL(
                String.format("http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertEquals("{\"Path\":\"/user/guest\"}", line);
        }

        url = new URL(
                String.format("http://localhost:%s/webhdfs/v1%s?op=OPEN&user.name=guest",
                        propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY),
                        propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            response.close();
            assertEquals(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY), line);
        }

        // Knox clients need self trusted certificates in tests
        defaultBlindTrust();

        // Read the file throught Knox
        url = new URL(
                String.format("https://localhost:%s/gateway/mycluster/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertEquals("{\"Path\":\"/user/guest\"}", line);
        }

        url = new URL(
                String.format("https://localhost:%s/gateway/mycluster/webhdfs/v1/%s?op=OPEN&user.name=guest",
                        propertyParser.getProperty(ConfigVars.KNOX_PORT_KEY),
                        propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            response.close();
            assertEquals(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY), line);
        }
    }

    private void defaultBlindTrust() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509ExtendedTrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] xcs, String string, Socket socket) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] xcs, String string, Socket socket) throws CertificateException {

                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException {

                    }

                }
        };
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }
}
