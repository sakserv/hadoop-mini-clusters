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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLocalCluster.class);
    
    private Integer port;
    private String tempDir;
    private String zookeeperConnectionString;

    private TestingServer testingServer;

    private ZookeeperLocalCluster(Builder builder) {
        this.port = builder.port;
        this.tempDir = builder.tempDir;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
    }

    public int getPort() {
        return port;
    }
    
    public String getTempDir() { return tempDir; }
    
    public String getZookeeperConnectionString() { return zookeeperConnectionString; }

    public static class Builder
    {
        private Integer port;
        private String tempDir;
        private String zookeeperConnectionString;

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTempDir(String tempDir) {
            this.tempDir = tempDir;
            return this;
        }
        
        public Builder setZookeeperConnectionString(String zookeeperConnectionString){
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;
            
        }

        public ZookeeperLocalCluster build() {
            ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster(this);
            validateObject(zookeeperLocalCluster);
            return zookeeperLocalCluster;
        }

        private void validateObject(ZookeeperLocalCluster zookeeperLocalCluster) {
            if(zookeeperLocalCluster.port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Port");
            }

            if(zookeeperLocalCluster.tempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Temp Dir");
            }

            if(zookeeperLocalCluster.zookeeperConnectionString == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Connection String");
            }
        }

    }

    @Override
    public void start() throws Exception {
        LOG.info("ZOOKEEPER: Starting Zookeeper on port: {}",  port);
        testingServer = new TestingServer(port, new File(tempDir));
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
    public void configure() throws Exception {}

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(tempDir);
    }

}
