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
import org.apache.hadoop.conf.Configuration;
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
    private String hdfsDefaultFs;
    private Configuration hbaseConfiguration;

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

    public String getHdfsDefaultFs() {
        return hdfsDefaultFs;
    }

    public Configuration getHbaseConfiguration() {
        return hbaseConfiguration;
    }

    private HbaseLocalCluster(Builder builder) {
        this.hbaseMasterPort = builder.hbaseMasterPort;
        this.hbaseMasterInfoPort = builder.hbaseMasterInfoPort;
        this.numRegionServers = builder.numRegionServers;
        this.hbaseRootDir = builder.hbaseRootDir;
        this.zookeeperPort = builder.zookeeperPort;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
        this.zookeeperZnodeParent = builder.zookeeperZnodeParent;
        this.hdfsDefaultFs = builder.hdfsDefaultFs;
        this.hbaseConfiguration = builder.hbaseConfiguration;
    }

    public static class Builder {
        private Integer hbaseMasterPort;
        private Integer hbaseMasterInfoPort;
        private Integer numRegionServers;
        private String hbaseRootDir;
        private Integer zookeeperPort;
        private String zookeeperConnectionString;
        private String zookeeperZnodeParent;
        private String hdfsDefaultFs;
        private Configuration hbaseConfiguration;

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

        public Builder setHdfsDefaultFs(String hdfsDefaultFs) {
            this.hdfsDefaultFs = hdfsDefaultFs;
            return this;
        }

        public Builder setHbaseConfiguration(Configuration hbaseConfiguration) {
            this.hbaseConfiguration = hbaseConfiguration;
            return this;
        }


    }

    public void start() {}
    public void stop(){}
    public void configure() {}


}
