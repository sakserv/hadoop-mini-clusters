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
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLocalCluster.class);
    
    private final Integer port;
    private final String tempDir;

    private TestingServer testingServer;

    private ZookeeperLocalCluster(Builder builder) {
        this.port = builder.port;
        this.tempDir = builder.tempDir;
    }

    public int getPort() {
        return port;
    }
    
    public String getTempDir() { return tempDir; }

    public static class Builder
    {
        private Integer port;
        private String tempDir;

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTempDir(String tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        public ZookeeperLocalCluster build() throws IOException {
            ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster(this);
            validateObject(zookeeperLocalCluster);
            return zookeeperLocalCluster;
        }

        private void validateObject(ZookeeperLocalCluster zookeeperLocalCluster) throws IOException {
            PropertyParser propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);

            if(zookeeperLocalCluster.port == null) {
                this.port = Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY));
            }

            if(zookeeperLocalCluster.tempDir == null) {
                this.tempDir = propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY);
            }
        }

    }



    // Curator does not leverage a configuration object
    @Override
    public void configure() {}

    @Override
    public void start() {
        LOG.info("ZOOKEEPER: Starting Zookeeper on port: " + port);
        try {
            testingServer = new TestingServer(port, new File(tempDir));
        } catch(Exception e) {
            LOG.info("ERROR: Failed to start Zookeeper");
            e.getStackTrace();
        }
    }

    @Override
    public void stop()  {
        stop(true);
    }

    public void stop(boolean cleanUp) {
        LOG.info("ZOOKEEPER: Stopping Zookeeper on port: " + port);
        try {
            testingServer.stop();
        } catch(IOException e) {
            LOG.info("ERROR: Failed to stop Zookeeper");
            e.printStackTrace();
        }
        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(tempDir);
    }

    public String getZkConnectionString() {
        return testingServer.getConnectString();
    }

    public String getZkHostName() {
        return getZkConnectionString().split(":")[0];
    }

    public String getZkPort() {
        return getZkConnectionString().split(":")[1];
    }

}
