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

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = Logger.getLogger(ZookeeperLocalCluster.class);

    private static final String DEFAULT_ZK_TEMP_DIR = "embedded_zk";
    private static final int DEFAULT_ZK_PORT = 2181;

    private String zkTempDir;
    private TestingServer zkTestServer;
    private int zkPort;

    public ZookeeperLocalCluster() {
        zkPort = DEFAULT_ZK_PORT;
        zkTempDir = DEFAULT_ZK_TEMP_DIR;
        configure();
    }

    public ZookeeperLocalCluster(int zkPort) {
        this.zkPort = zkPort;
        zkTempDir = DEFAULT_ZK_TEMP_DIR;
        configure();
    }

    public ZookeeperLocalCluster(int zkPort, String zkTempDir) {
        this.zkPort = zkPort;
        this.zkTempDir = zkTempDir;
        configure();
    }

    // Curator does not leverage a configuration object
    public void configure() {}

    public void start() {
        LOG.info("ZOOKEEPER: Starting Zookeeper on port: " + zkPort);
        try {
            zkTestServer = new TestingServer(zkPort, new File(zkTempDir));
        } catch(Exception e) {
            LOG.info("ERROR: Failed to start Zookeeper");
            e.getStackTrace();
        }
    }

    public void stop()  {
        LOG.info("ZOOKEEPER: Stopping Zookeeper on port: " + zkPort);
        try {
            zkTestServer.stop();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void stop(boolean cleanUp) {
        stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(zkTempDir);
    }

    public String getZkConnectionString() {
        return zkTestServer.getConnectString();
    }

    public String getZkHostName() {
        return getZkConnectionString().split(":")[0];
    }

    public String getZkPort() {
        return getZkConnectionString().split(":")[1];
    }

    public void dumpConfig() {
        LOG.info("ZOOKEEPER CONFIG: " + zkTestServer.getTempDirectory());
    }

}
