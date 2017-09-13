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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HdfsLocalCluster.class);

    MiniDFSCluster miniDFSCluster;

    private Integer hdfsNamenodePort;
    private Integer hdfsNamenodeHttpPort;
    private String hdfsTempDir;
    private Integer hdfsNumDatanodes;
    private Boolean hdfsEnablePermissions;
    private Boolean hdfsFormat;
    private Boolean hdfsEnableRunningUserAsProxyUser;
    private Configuration hdfsConfig;

    public Integer getHdfsNamenodePort() {
        return hdfsNamenodePort;
    }

    public String getHdfsTempDir() {
        return hdfsTempDir;
    }

    public Integer getHdfsNumDatanodes() {
        return hdfsNumDatanodes;
    }

    public Boolean getHdfsEnablePermissions() {
        return hdfsEnablePermissions;
    }

    public Boolean getHdfsFormat() {
        return hdfsFormat;
    }

    public Boolean getHdfsEnableRunningUserAsProxyUser() {
        return hdfsEnableRunningUserAsProxyUser;
    }

    public Configuration getHdfsConfig() {
        return hdfsConfig;
    }

    private HdfsLocalCluster(Builder builder) {
        this.hdfsNamenodePort = builder.hdfsNamenodePort;
        this.hdfsNamenodeHttpPort = builder.hdfsNamenodeHttpPort;
        this.hdfsTempDir = builder.hdfsTempDir;
        this.hdfsNumDatanodes = builder.hdfsNumDatanodes;
        this.hdfsEnablePermissions = builder.hdfsEnablePermissions;
        this.hdfsFormat = builder.hdfsFormat;
        this.hdfsEnableRunningUserAsProxyUser = builder.hdfsEnableRunningUserAsProxyUser;
        this.hdfsConfig = builder.hdfsConfig;
    }

    public static class Builder {
        private Integer hdfsNamenodePort;
        private Integer hdfsNamenodeHttpPort;
        private String hdfsTempDir;
        private Integer hdfsNumDatanodes;
        private Boolean hdfsEnablePermissions;
        private Boolean hdfsFormat;
        private Boolean hdfsEnableRunningUserAsProxyUser;
        private Configuration hdfsConfig;


        public Builder setHdfsNamenodePort(Integer hdfsNameNodePort) {
            this.hdfsNamenodePort = hdfsNameNodePort;
            return this;
        }

        public Builder setHdfsNamenodeHttpPort(Integer hdfsNameNodeHttpPort) {
            this.hdfsNamenodeHttpPort = hdfsNameNodeHttpPort;
            return this;
        }

        public Builder setHdfsTempDir(String hdfsTempDir) {
            this.hdfsTempDir = hdfsTempDir;
            return this;
        }

        public Builder setHdfsNumDatanodes(Integer hdfsNumDatanodes) {
            this.hdfsNumDatanodes = hdfsNumDatanodes;
            return this;
        }

        public Builder setHdfsEnablePermissions(Boolean hdfsEnablePermissions) {
            this.hdfsEnablePermissions = hdfsEnablePermissions;
            return this;
        }

        public Builder setHdfsFormat(Boolean hdfsFormat) {
            this.hdfsFormat = hdfsFormat;
            return this;
        }

        public Builder setHdfsEnableRunningUserAsProxyUser(Boolean hdfsEnableRunningUserAsProxyUser) {
            this.hdfsEnableRunningUserAsProxyUser = hdfsEnableRunningUserAsProxyUser;
            return this;
        }

        public Builder setHdfsConfig(Configuration hdfsConfig) {
            this.hdfsConfig = hdfsConfig;
            return this;
        }

        public HdfsLocalCluster build() {
            HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster(this);
            validateObject(hdfsLocalCluster);
            return hdfsLocalCluster;
        }

        public void validateObject(HdfsLocalCluster hdfsLocalCluster) {
            if(hdfsLocalCluster.hdfsNamenodePort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Namenode Port");
            }

            if(hdfsLocalCluster.hdfsTempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Temp Dir");
            }

            if(hdfsLocalCluster.hdfsNumDatanodes == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Num Datanodes");
            }

            if(hdfsLocalCluster.hdfsEnablePermissions == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Enable Permissions");
            }

            if(hdfsLocalCluster.hdfsFormat == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Format");
            }

            if(hdfsLocalCluster.hdfsConfig == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HDFS Config");
            }

        }
    }

    @Override
    public void start() throws Exception {

        LOG.info("HDFS: Starting MiniDfsCluster");
        configure();
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig)
                .nameNodePort(hdfsNamenodePort)
                .nameNodeHttpPort(hdfsNamenodeHttpPort==null? 0 : hdfsNamenodeHttpPort.intValue() )
                .numDataNodes(hdfsNumDatanodes)
                .format(hdfsFormat)
                .racks(null)
                .build();

    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("HDFS: Stopping MiniDfsCluster");
        miniDFSCluster.shutdown();
        if(cleanUp) {
            cleanUp();
        }

    }

    @Override
    public void configure() throws Exception {
        if(null != hdfsEnableRunningUserAsProxyUser && hdfsEnableRunningUserAsProxyUser) {
            hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
            hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
        }

        hdfsConfig.setBoolean("dfs.permissions", hdfsEnablePermissions);
        System.setProperty("test.build.data", hdfsTempDir);

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(hdfsTempDir);
    }

    public FileSystem getHdfsFileSystemHandle() throws Exception {
        return miniDFSCluster.getFileSystem();
    }
}
