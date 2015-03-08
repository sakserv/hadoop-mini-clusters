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
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(YarnLocalCluster.class);

    private String testName = getClass().getName();
    private Integer numResourceManagers;
    private Integer numNodeManagers;
    private Integer numLocalDirs;
    private Integer numLogDirs;
    private Boolean enableHa;
    private Configuration conf;
    
    private MiniYARNCluster miniYARNCluster;
    

    public String getTestName() {
        return testName;
    }

    public Integer getNumResourceManagers() {
        return numResourceManagers;
    }

    public Integer getNumNodeManagers() {
        return numNodeManagers;
    }

    public Integer getNumLocalDirs() {
        return numLocalDirs;
    }

    public Integer getNumLogDirs() {
        return numLogDirs;
    }

    public Boolean getEnableHa() {
        return enableHa;
    }

    public Configuration getConf() {
        return conf;
    }

    private YarnLocalCluster(Builder builder) {
        this.numResourceManagers = builder.numResourceManagers;
        this.numNodeManagers = builder.numNodeManagers;
        this.numLocalDirs = builder.numLocalDirs;
        this.numLogDirs = builder.numLogDirs;
        this.enableHa = builder.enableHa;
    }
    
    public static class Builder {
        private Integer numResourceManagers;
        private Integer numNodeManagers;
        private Integer numLocalDirs;
        private Integer numLogDirs;
        private Boolean enableHa;
        
        public Builder setNumResourceManagers(Integer numResourceManagers) {
            this.numResourceManagers = numResourceManagers;
            return this;
        }
        
        public Builder setNumNodeManagers(Integer numNodeManagers) {
            this.numNodeManagers = numNodeManagers;
            return this;
        }
        
        public Builder setNumLocalDirs(Integer numLocalDirs) {
            this.numLocalDirs = numLocalDirs;
            return this;
        }
        
        public Builder setNumLogDirs(Integer numLogDirs) {
            this.numLogDirs = numLogDirs;
            return this;
        }
        
        public Builder setEnableHa(Boolean enableHa) {
            this.enableHa = enableHa;
            return this;
        }
        
        public YarnLocalCluster build() {
            YarnLocalCluster yarnLocalCluster = new YarnLocalCluster(this);
            validateObject(yarnLocalCluster);
            return yarnLocalCluster;
        }
        
        public void validateObject(YarnLocalCluster yarnLocalCluster) {
            if (yarnLocalCluster.getNumResourceManagers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Resource Managers");
            }
            if (yarnLocalCluster.getNumNodeManagers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Node Managers");
            }
            if (yarnLocalCluster.getNumLocalDirs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Local Dirs");
            }
            if (yarnLocalCluster.getNumLogDirs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Log Dirs");
            }
            if (yarnLocalCluster.getEnableHa() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Should Enable HA");
            }
            
        }
    }

    public void configure() {
        conf = new Configuration();
    }
    
    public void stop() {
        miniYARNCluster.stop();
        
    }
    public void start() {
        configure();
        miniYARNCluster = new MiniYARNCluster(testName, numResourceManagers, numNodeManagers,
                numLocalDirs, numLogDirs, enableHa);
        try {
            miniYARNCluster.serviceInit(conf);
            miniYARNCluster.init(conf);
        } catch(Exception e) {
            e.printStackTrace();
        }
        miniYARNCluster.start();
    }
    
}
