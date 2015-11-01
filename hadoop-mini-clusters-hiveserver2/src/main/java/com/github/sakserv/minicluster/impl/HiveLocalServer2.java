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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HiveLocalServer2 implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HiveLocalServer2.class);
    
    private HiveServer2 hiveServer2;
    
    private String hiveServer2Hostname;
    private Integer hiveServer2Port;
    private String hiveMetastoreHostname;
    private Integer hiveMetastorePort;
    private String hiveMetastoreDerbyDbDir;
    private String hiveScratchDir;
    private String hiveWarehouseDir;
    private HiveConf hiveConf;
    private String zookeeperConnectionString;

    public String getHiveServer2Hostname() {
        return hiveServer2Hostname;
    }

    public Integer getHiveServer2Port() {
        return hiveServer2Port;
    }

    public String getHiveMetastoreHostname() {
        return hiveMetastoreHostname;
    }

    public Integer getHiveMetastorePort() {
        return hiveMetastorePort;
    }
    
    public String getHiveMetastoreDerbyDbDir() {
        return hiveMetastoreDerbyDbDir;
    }

    public String getHiveScratchDir() {
        return hiveScratchDir;
    }

    public String getHiveWarehouseDir() {
        return hiveWarehouseDir;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }
    
    private HiveLocalServer2(Builder builder) {
        this.hiveServer2Hostname = builder.hiveServer2Hostname;
        this.hiveServer2Port = builder.hiveServer2Port;
        this.hiveMetastoreHostname = builder.hiveMetastoreHostname;
        this.hiveMetastorePort = builder.hiveMetastorePort;
        this.hiveMetastoreDerbyDbDir = builder.hiveMetastoreDerbyDbDir;
        this.hiveScratchDir = builder.hiveScratchDir;
        this.hiveWarehouseDir = builder.hiveWarehouseDir;
        this.hiveConf = builder.hiveConf;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
    }
    
    public static class Builder {
        
        private String hiveServer2Hostname;
        private Integer hiveServer2Port;
        private String hiveMetastoreHostname;
        private Integer hiveMetastorePort;
        private String hiveMetastoreDerbyDbDir;
        private String hiveScratchDir;
        private String hiveWarehouseDir;
        private HiveConf hiveConf;
        private String zookeeperConnectionString;
        
        public Builder setHiveServer2Hostname(String hiveServer2Hostname) {
            this.hiveServer2Hostname = hiveServer2Hostname;
            return this;
        }

        public Builder setHiveServer2Port(Integer hiveServer2Port) {
            this.hiveServer2Port = hiveServer2Port;
            return this;
        }

        public Builder setHiveMetastoreHostname(String hiveMetastoreHostname) {
            this.hiveMetastoreHostname = hiveMetastoreHostname;
            return this;
        }

        public Builder setHiveMetastorePort(Integer hiveMetastorePort) {
            this.hiveMetastorePort = hiveMetastorePort;
            return this;
        }

        public Builder setHiveMetastoreDerbyDbDir(String hiveMetastoreDerbyDbDir) {
            this.hiveMetastoreDerbyDbDir = hiveMetastoreDerbyDbDir;
            return this;
        }

        public Builder setHiveScratchDir(String hiveScratchDir) {
            this.hiveScratchDir = hiveScratchDir;
            return this;
        }

        public Builder setHiveWarehouseDir(String hiveWarehouseDir) {
            this.hiveWarehouseDir = hiveWarehouseDir;
            return this;
        }

        public Builder setHiveConf(HiveConf hiveConf) {
            this.hiveConf = hiveConf;
            return this;
        }

        public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;
        }
        
        public HiveLocalServer2 build() {
            HiveLocalServer2 hiveLocalServer2 = new HiveLocalServer2(this);
            validateObject(hiveLocalServer2);
            return hiveLocalServer2;
        }
        
        public void validateObject(HiveLocalServer2 hiveLocalServer2) {
            if(hiveLocalServer2.hiveServer2Hostname == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Server2 Hostname");
            }

            if(hiveLocalServer2.hiveServer2Port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Server2 Port");
            }

            if(hiveLocalServer2.hiveMetastoreHostname == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Hostname");
            }

            if(hiveLocalServer2.hiveMetastorePort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Port");
            }

            if(hiveLocalServer2.hiveMetastoreDerbyDbDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Derby Db Dir");
            }

            if(hiveLocalServer2.hiveScratchDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Scratch Dir");
            }

            if(hiveLocalServer2.hiveWarehouseDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Warehouse Dir");
            }

            if(hiveLocalServer2.hiveConf == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Hive Conf");
            }

            if(hiveLocalServer2.zookeeperConnectionString == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Quorum");
            }
        }
        
    }


    @Override
    public void start() throws Exception {
        hiveServer2 = new HiveServer2();
        LOG.info("HIVESERVER2: Starting HiveServer2 on port: {}", hiveServer2Port);
        configure();
        hiveServer2.init(hiveConf);
        hiveServer2.start();
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("HIVESERVER2: Stopping HiveServer2 on port: {}", hiveServer2Port);
        hiveServer2.stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
                "thrift://" + hiveMetastoreHostname + ":" + hiveMetastorePort);
        hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, hiveScratchDir);
        hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
                "jdbc:derby:;databaseName=" + hiveMetastoreDerbyDbDir + ";create=true");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, new File(hiveWarehouseDir).getAbsolutePath());
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, String.valueOf(hiveServer2Hostname));
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hiveServer2Port);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, zookeeperConnectionString);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, Boolean.TRUE);
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(hiveMetastoreDerbyDbDir);
        FileUtils.deleteFolder(hiveScratchDir);
        FileUtils.deleteFolder(new File("derby.log").getAbsolutePath());
    }

}
