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
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalCluster.class);

    MiniHBaseCluster miniHBaseCluster;

    private Integer hbaseMasterPort;
    private Integer hbaseMasterInfoPort;
    private Integer numRegionServers;
    private String hbaseRootDir;
    private Integer zookeeperPort;
    private String zookeeperConnectionString;
    private String zookeeperZnodeParent;
    private Boolean hbaseWalReplicationEnabled;
    private Configuration hbaseConfiguration;
    private Boolean restActivated = false;
    private HbaseRestLocalCluster hbaseRestLocalCluster;

    public Integer getHbaseMasterPort() {
        return hbaseMasterPort;
    }

    public Integer getHbaseMasterInfoPort() {
        return hbaseMasterInfoPort;
    }

    public Integer getNumRegionServers() {
        return numRegionServers;
    }

    public String getHbaseRootDir() {
        return hbaseRootDir;
    }

    public Integer getZookeeperPort() {
        return zookeeperPort;
    }

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    public String getZookeeperZnodeParent() {
        return zookeeperZnodeParent;
    }

    public Boolean getHbaseWalReplicationEnabled() {
        return hbaseWalReplicationEnabled;
    }

    public Configuration getHbaseConfiguration() {
        return hbaseConfiguration;
    }

    public Boolean isRestActivated() {
        return restActivated;
    }

    public HbaseRestLocalCluster getHbaseRestLocalCluster() {
        return hbaseRestLocalCluster;
    }

    private HbaseLocalCluster(Builder builder) {
        this.hbaseMasterPort = builder.hbaseMasterPort;
        this.hbaseMasterInfoPort = builder.hbaseMasterInfoPort;
        this.numRegionServers = builder.numRegionServers;
        this.hbaseRootDir = builder.hbaseRootDir;
        this.zookeeperPort = builder.zookeeperPort;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
        this.zookeeperZnodeParent = builder.zookeeperZnodeParent;
        this.hbaseWalReplicationEnabled = builder.hbaseWalReplicationEnabled;
        this.hbaseConfiguration = builder.hbaseConfiguration;
        this.restActivated = builder.restActivated;
        this.hbaseRestLocalCluster = builder.hbaseRestLocalCluster;

    }

    public static class Builder {
        private Integer hbaseMasterPort;
        private Integer hbaseMasterInfoPort;
        private Integer numRegionServers;
        private String hbaseRootDir;
        private Integer zookeeperPort;
        private String zookeeperConnectionString;
        private String zookeeperZnodeParent;
        private Boolean hbaseWalReplicationEnabled;
        private Configuration hbaseConfiguration;
        private Boolean restActivated = false;
        private HbaseRestLocalCluster hbaseRestLocalCluster;

        public Builder setHbaseMasterPort(Integer hbaseMasterPort) {
            this.hbaseMasterPort = hbaseMasterPort;
            return this;
        }

        public Builder setHbaseMasterInfoPort(Integer hbaseMasterInfoPort) {
            this.hbaseMasterInfoPort = hbaseMasterInfoPort;
            return this;
        }

        public Builder setNumRegionServers(Integer numRegionServers) {
            this.numRegionServers = numRegionServers;
            return this;
        }

        public Builder setHbaseRootDir(String hbaseRootDir) {
            this.hbaseRootDir = hbaseRootDir;
            return this;
        }

        public Builder setZookeeperPort(Integer zookeeperPort) {
            this.zookeeperPort = zookeeperPort;
            return this;
        }

        public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;
        }

        public Builder setZookeeperZnodeParent(String zookeeperZnodeParent) {
            this.zookeeperZnodeParent = zookeeperZnodeParent;
            return this;
        }

        public Builder setHbaseWalReplicationEnabled(Boolean hbaseWalReplicationEnabled) {
            this.hbaseWalReplicationEnabled = hbaseWalReplicationEnabled;
            return this;
        }

        public Builder setHbaseConfiguration(Configuration hbaseConfiguration) {
            this.hbaseConfiguration = hbaseConfiguration;
            return this;
        }

        Configuration getHbaseConfiguration() {
            return hbaseConfiguration;
        }

        void setHbaseRestLocalCluster(HbaseRestLocalCluster hbaseRestLocalCluster) {
            this.hbaseRestLocalCluster = hbaseRestLocalCluster;
        }

        public HbaseRestLocalCluster.RestBuilder activeRestGateway() {
            this.restActivated = true;
            return new HbaseRestLocalCluster.RestBuilder(this);
        }

        public HbaseLocalCluster build() {
            HbaseLocalCluster hbaseLocalCluster = new HbaseLocalCluster(this);
            validateObject(hbaseLocalCluster);
            return hbaseLocalCluster;
        }

        public void validateObject(HbaseLocalCluster hbaseLocalCluster) {
            if (hbaseLocalCluster.hbaseMasterPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Master Port");
            }
            if (hbaseLocalCluster.hbaseMasterInfoPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Master Info Port");
            }
            if (hbaseLocalCluster.numRegionServers == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Number of Region Servers");
            }
            if (hbaseLocalCluster.hbaseRootDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Root Dir");
            }
            if (hbaseLocalCluster.zookeeperPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Port");
            }
            if (hbaseLocalCluster.zookeeperConnectionString == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Connection String");
            }
            if (hbaseLocalCluster.zookeeperZnodeParent == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Znode Parent");
            }
            if (hbaseLocalCluster.hbaseWalReplicationEnabled == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase WAL Replication Enabled");
            }
            if (hbaseLocalCluster.hbaseConfiguration == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Configuration");
            }
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("HBASE: Starting MiniHBaseCluster");
        configure();
        miniHBaseCluster = new MiniHBaseCluster(hbaseConfiguration, numRegionServers);
        miniHBaseCluster.startMaster();
        miniHBaseCluster.startRegionServer();
        if (isRestActivated()) {
            getHbaseRestLocalCluster().start();
        }
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("HBASE: Stopping MiniHBaseCluster");

        if (isRestActivated()) {
            getHbaseRestLocalCluster().cleanUp();
            getHbaseRestLocalCluster().stop();
        }

        miniHBaseCluster.flushcache();
        miniHBaseCluster.close();
        miniHBaseCluster.shutdown();
        miniHBaseCluster.waitUntilShutDown();
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        configure(hbaseConfiguration);

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();
    }

    public void configure(Configuration hbaseConfiguration) throws Exception {
        hbaseConfiguration.set(HConstants.MASTER_PORT, hbaseMasterPort.toString());
        hbaseConfiguration.set(HConstants.MASTER_INFO_PORT, hbaseMasterInfoPort.toString());
        hbaseConfiguration.set(HConstants.HBASE_DIR, hbaseRootDir);
        hbaseConfiguration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort.toString());
        hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperConnectionString);
        hbaseConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperZnodeParent);
        hbaseConfiguration.set(HConstants.REPLICATION_ENABLE_KEY, hbaseWalReplicationEnabled.toString());
        hbaseConfiguration.set("hbase.splitlog.manager.unassigned.timeout", "999999999");
        hbaseConfiguration.set("hbase.splitlog.manager.timeoutmonitor.period", "999999999");
        hbaseConfiguration.set("hbase.master.logcleaner.plugins", "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner");
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(hbaseRootDir);
    }
}
