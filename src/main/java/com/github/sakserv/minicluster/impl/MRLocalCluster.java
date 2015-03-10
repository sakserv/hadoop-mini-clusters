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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRYarnClusterAdapter;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(YarnLocalCluster.class);

    private String testName = getClass().getName();
    private Integer numNodeManagers;
    private Configuration yarnConfig;
    
    private MiniMRYarnCluster miniMRYarnCluster;

    public String getTestName() {
        return testName;
    }

    public Integer getNumNodeManagers() {
        return numNodeManagers;
    }

    public Configuration getYarnConfig() {
        return yarnConfig;
    }

    private MRLocalCluster(Builder builder) {
        this.numNodeManagers = builder.numNodeManagers;
        this.yarnConfig = builder.yarnConfig;
    }
    
    public static class Builder {
        private Integer numNodeManagers;
        private Configuration yarnConfig;
        
        public Builder setNumNodeManagers(Integer numNodeManagers) {
            this.numNodeManagers = numNodeManagers;
            return this;
        }

        public Builder setYarnConfig(Configuration yarnConfig) {
            this.yarnConfig = yarnConfig;
            return this;
        }
        
        public MRLocalCluster build() {
            MRLocalCluster mrLocalCluster = new MRLocalCluster(this);
            validateObject(mrLocalCluster);
            return mrLocalCluster;
        }

        public void validateObject(MRLocalCluster mrLocalCluster) {
            if (mrLocalCluster.getNumNodeManagers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Node Managers");
            }

            if (mrLocalCluster.getYarnConfig() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Yarn Configuration");
            }
        }
        
    }

    public void start() {
        LOG.info("MR: Starting MiniMRYarnCluster");
        configure();
        miniMRYarnCluster = new MiniMRYarnCluster(testName, numNodeManagers);
        try {
            miniMRYarnCluster.serviceInit(yarnConfig);
            miniMRYarnCluster.init(yarnConfig);
        } catch(Exception e) {
            e.printStackTrace();
        }
        miniMRYarnCluster.start();
        
    }

    public void cleanUp() {
        FileUtils.deleteFolder("target/" + testName);
    }

    public void stop(boolean cleanUp) {
        LOG.info("MR: Stopping MiniMRYarnCluster");
        miniMRYarnCluster.stop();
        if(cleanUp) {
            cleanUp();
        }

    }
    
    public void stop() {stop(true);}
    public void configure() {}
}
