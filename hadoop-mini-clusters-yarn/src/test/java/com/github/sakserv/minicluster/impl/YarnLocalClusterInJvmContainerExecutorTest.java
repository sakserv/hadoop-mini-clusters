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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.yarn.InJvmContainerExecutor;
import com.github.sakserv.minicluster.yarn.simpleyarnapp.Client;
import com.github.sakserv.propertyparser.PropertyParser;

public class YarnLocalClusterInJvmContainerExecutorTest {
    
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(YarnLocalClusterInJvmContainerExecutorTest.class);

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
    
    private static YarnLocalCluster yarnLocalCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        yarnLocalCluster = new YarnLocalCluster.Builder()
                .setNumNodeManagers(Integer.parseInt(propertyParser.getProperty(ConfigVars.YARN_NUM_NODE_MANAGERS_KEY)))
                .setNumLocalDirs(Integer.parseInt(propertyParser.getProperty(ConfigVars.YARN_NUM_LOCAL_DIRS_KEY)))
                .setNumLogDirs(Integer.parseInt(propertyParser.getProperty(ConfigVars.YARN_NUM_LOG_DIRS_KEY)))
                .setResourceManagerAddress(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setResourceManagerHostname(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_HOSTNAME_KEY))
                .setResourceManagerSchedulerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY))
                .setResourceManagerResourceTrackerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY))
                .setResourceManagerWebappAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY))
                .setUseInJvmContainerExecutor(true)
                .setConfig(new Configuration())
                .build();

        yarnLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // We want the cluster to be able to shut down
        System.setSecurityManager(new InJvmContainerExecutor.SystemExitAllowSecurityManager());
        yarnLocalCluster.stop();
    }

    @Test
    public void testYarnLocalClusterWithInJvmContainerExecutor() {
        
        String[] args = new String[7];
        args[0] = "whoami";
        args[1] = "1";
        args[2] = getClass().getClassLoader().getResource("simple-yarn-app-1.1.0.jar").toString();
        args[3] = yarnLocalCluster.getResourceManagerAddress();
        args[4] = yarnLocalCluster.getResourceManagerHostname();
        args[5] = yarnLocalCluster.getResourceManagerSchedulerAddress();
        args[6] = yarnLocalCluster.getResourceManagerResourceTrackerAddress();
        
        try {
            Client.main(args);
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        // simple yarn app running "whoami", 
        // validate the container contents matches the java user.name
        if (!System.getProperty("user.name").equals("travis")) {
            assertEquals(System.getProperty("user.name"), getStdoutContents());
        }
        
    }
    
    public String getStdoutContents() {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(getStdoutPath()));
            contents = new String(encoded, Charset.defaultCharset()).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contents;
    }
    
    public String getStdoutPath() {
        File dir = new File("./target/" + yarnLocalCluster.getTestName());
        String[] nmDirs = dir.list();
        for (String nmDir: nmDirs) {
            if (nmDir.contains("logDir")) {
                String[] appDirs = new File(dir.toString() + "/" + nmDir).list();
                for (String appDir: appDirs) {
                    if (appDir.contains("0001")) {
                        String[] containerDirs = new File(dir.toString() + "/" + nmDir + "/" + appDir).list();
                        for (String containerDir: containerDirs) {
                            if(containerDir.contains("000002")) {
                                return dir.toString() + "/" + nmDir + "/" + appDir + "/" + containerDir + "/stdout";
                            }
                        }
                    }
                }
            }
        }
        return "";
    }
}
