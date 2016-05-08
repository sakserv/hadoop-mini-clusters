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

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.mapreduce.Driver;
import com.github.sakserv.propertyparser.PropertyParser;

public class MRLocalClusterIntegrationTest {
    
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MRLocalClusterIntegrationTest.class);

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

    private static HdfsLocalCluster dfsCluster;
    private static MRLocalCluster mrLocalCluster;
    private static String testFile = propertyParser.getProperty(ConfigVars.MR_TEST_DATA_FILENAME_KEY);
    private static String testDataHdfsInputDir =
            propertyParser.getProperty(ConfigVars.MR_TEST_DATA_HDFS_INPUT_DIR_KEY);
    private static String testDataHdfsOutputDir =
            propertyParser.getProperty(ConfigVars.MR_TEST_DATA_HDFS_OUTPUT_DIR_KEY);
    private static String mrOutputText = "1";
    
    @BeforeClass
    public static void setUp() throws Exception {
        dfsCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsConfig(new Configuration())
                .build();
        dfsCluster.start();

        mrLocalCluster = new MRLocalCluster.Builder()
                .setNumNodeManagers(Integer.parseInt(propertyParser.getProperty(ConfigVars.YARN_NUM_NODE_MANAGERS_KEY)))
                .setJobHistoryAddress(propertyParser.getProperty(ConfigVars.MR_JOB_HISTORY_ADDRESS_KEY))
                .setResourceManagerAddress(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setResourceManagerHostname(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_HOSTNAME_KEY))
                .setResourceManagerSchedulerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY))
                .setResourceManagerResourceTrackerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY))
                .setResourceManagerWebappAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY))
                .setUseInJvmContainerExecutor(Boolean.parseBoolean(propertyParser.getProperty(
                        ConfigVars.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY)))
                .setHdfsDefaultFs(dfsCluster.getHdfsConfig().get("fs.defaultFS"))
                .setConfig(new Configuration())
                .build();

        mrLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mrLocalCluster.stop();
        dfsCluster.stop();
    }

    @Test
    public void testMRLocalCluster() throws Exception {

        String inputFileContents = resourceFileToString(testFile);
        writeFileToHdfs(testDataHdfsInputDir + "/" + testFile, inputFileContents);
        Driver driver = new Driver();
        driver.setConfiguration(mrLocalCluster.getConfig());
        String[] args = new String[2];
        args[0] = testDataHdfsInputDir;
        args[1] = testDataHdfsOutputDir;
        Driver.main(args);
        assertEquals(mrOutputText, getCountForWord(testDataHdfsOutputDir + "/part-r-00000", "This"));

    }

    private void writeFileToHdfs(String fileName, String contents) throws Exception {
        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(new Path(fileName));
        writer.writeUTF(contents);
        writer.close();
        hdfsFsHandle.close();
    }

    private String getCountForWord(String fileName, String word) throws Exception {
        String output = readFileFromHdfs(fileName);
        for(String line: output.split("\n")) {
            if(line.contains(word)) {
                return line.split("\t")[1];
            }
        }
        return "";
    }

    private String readFileFromHdfs(String filename) throws Exception {
        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        FSDataInputStream reader = hdfsFsHandle.open(new Path(filename));
        String output = reader.readUTF();
        reader.close();
        hdfsFsHandle.close();
        return output;
    }

    private String resourceFileToString(String fileName) {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }
}
