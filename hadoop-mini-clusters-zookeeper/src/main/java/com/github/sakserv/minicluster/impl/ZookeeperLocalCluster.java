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
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLocalCluster.class);

    private final Integer port;
    private final String tempDir;
    private final String zookeeperConnectionString;
    private final int electionPort;
    private final int quorumPort;
    private final boolean deleteDataDirectoryOnClose;
    private final int serverId;
    private final int tickTime;
    private final int maxClientCnxns;
    private final Map<String, Object> customProperties;

    private TestingServer testingServer;

    private ZookeeperLocalCluster(Builder builder) {
        this.port = builder.port;
        this.tempDir = builder.tempDir;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
        this.electionPort = builder.electionPort;
        this.quorumPort = builder.quorumPort;
        this.deleteDataDirectoryOnClose = builder.deleteDataDirectoryOnClose;
        this.serverId = builder.serverId;
        this.tickTime = builder.tickTime;
        this.maxClientCnxns = builder.maxClientCnxns;
        this.customProperties = builder.customProperties;
    }

    public int getPort() {
        return port;
    }

    public String getTempDir() {
        return tempDir;
    }

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    public int getElectionPort() {
        return electionPort;
    }

    public int getQuorumPort() {
        return quorumPort;
    }

    public boolean getDeleteDataDirectoryOnClose() {
        return deleteDataDirectoryOnClose;
    }

    public int getServerId() {
        return serverId;
    }

    public int getTickTime() {
        return tickTime;
    }

    public int getMaxClientCnxns() {
        return maxClientCnxns;
    }

    public static class Builder {
        private Integer port;
        private String tempDir;
        private String zookeeperConnectionString;
        private int electionPort = -1;
        private int quorumPort = -1;
        private boolean deleteDataDirectoryOnClose = true;
        private int serverId = -1;
        private int tickTime = -1;
        private int maxClientCnxns = -1;
        private Map<String, Object> customProperties = new HashMap<>();

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTempDir(String tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;

        }

        public Builder setElectionPort(int electionPort) {
            this.electionPort = electionPort;
            return this;
        }

        public Builder setQuorumPort(int quorumPort) {
            this.quorumPort = quorumPort;
            return this;
        }

        public Builder setDeleteDataDirectoryOnClose(boolean deleteDataDirectoryOnClose) {
            this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
            return this;
        }

        public Builder setServerId(int serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder setTickTime(int tickTime) {
            this.tickTime = tickTime;
            return this;
        }

        public Builder setMaxClientCnxns(int maxClientCnxns) {
            this.maxClientCnxns = maxClientCnxns;
            return this;
        }

        public Builder setCustomProperties(Map<String, Object> customProperties) {
            this.customProperties = customProperties;
            return this;
        }

        public ZookeeperLocalCluster build() {
            ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster(this);
            validateObject(zookeeperLocalCluster);
            return zookeeperLocalCluster;
        }

        private void validateObject(ZookeeperLocalCluster zookeeperLocalCluster) {
            if (zookeeperLocalCluster.port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Port");
            }

            if (zookeeperLocalCluster.tempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Temp Dir");
            }

            if (zookeeperLocalCluster.zookeeperConnectionString == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Connection String");
            }
        }

    }

    @Override
    public void start() throws Exception {
        LOG.info("ZOOKEEPER: Starting Zookeeper on port: {}", port);
        InstanceSpec spec = new InstanceSpec(new File(tempDir), port, electionPort,
                quorumPort, deleteDataDirectoryOnClose, serverId, tickTime, maxClientCnxns, customProperties);
        testingServer = new TestingServer(spec, true);
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("ZOOKEEPER: Stopping Zookeeper on port: {}", port);
        testingServer.stop();
        if (cleanUp) {
            cleanUp();
        }

    }

    // Curator does not leverage a configuration object
    @Override
    public void configure() throws Exception {
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(tempDir);
    }
}
