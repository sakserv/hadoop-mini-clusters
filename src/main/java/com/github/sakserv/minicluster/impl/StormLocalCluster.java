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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import com.github.sakserv.minicluster.MiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(StormLocalCluster.class);
    
    private String zookeeperHost;
    private Long zookeeperPort;
    private Boolean enableDebug;
    private Integer numWorkers;
    private Config conf = new Config();
    private LocalCluster localCluster;
    
    private StormLocalCluster(Builder builder) {
        this.zookeeperHost = builder.zookeeperHost;
        this.zookeeperPort = builder.zookeeperPort;
        this.enableDebug = builder.enableDebug;
        this.numWorkers = builder.numWorkers;
    }
    
    public String getZookeeperHost() {
        return zookeeperHost;
    }
    
    public Long getZookeeperPort() {
        return zookeeperPort;
    }
    
    public Boolean getEnableDebug() {
        return enableDebug;
    }
    
    public Integer getNumWorkers() { return numWorkers;  }
    
    public Config getConf() { return conf; }
    
    public static class Builder {
        private String zookeeperHost;
        private Long zookeeperPort;
        private Boolean enableDebug;
        private Integer numWorkers;
        
        public Builder setZookeeperHost(String zookeeperHost) {
            this.zookeeperHost = zookeeperHost;
            return this;
        }
        
        public Builder setZookeeperPort(Long zookeeperPort) {
            this.zookeeperPort = zookeeperPort;
            return this;
        }
        
        public Builder setEnableDebug(Boolean enableDebug) {
            this.enableDebug = enableDebug;
            return this;
        }
        
        public Builder setNumWorkers(Integer numWorkers) {
            this.numWorkers = numWorkers;
            return this;
        }
        
        public StormLocalCluster build() {
            StormLocalCluster stormLocalCluster = new StormLocalCluster(this);
            validateObject(stormLocalCluster);
            return stormLocalCluster;
        }
        
        public void validateObject(StormLocalCluster stormLocalCluster) {
            if (stormLocalCluster.getZookeeperHost() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Host");
            }

            if (stormLocalCluster.getZookeeperPort() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Port");
            }

            if (stormLocalCluster.getEnableDebug() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Enable Debug");
            }
            
            if (stormLocalCluster.getNumWorkers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Num Workers");
            }
        }
    }

    public void configure() {
        conf.setDebug(enableDebug);
        conf.setNumWorkers(numWorkers);
    }

    public void start() {
        LOG.info("STORM: Instantiating LocalCluster");
        configure();
        localCluster = new LocalCluster(zookeeperHost, zookeeperPort);
    }

    public void stop() {
        localCluster.shutdown();
    }

    public void stop(String topologyName) {
        localCluster.killTopology(topologyName);
        stop();
    }
    
    public void stop(String topologyName, int waitSecs) {
        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(waitSecs);
        localCluster.killTopologyWithOpts(topologyName, killOptions);
        stop();
    }

    public void submitTopology(String topologyName, Config conf, StormTopology topology) {
        localCluster.submitTopology(topologyName, conf, topology);
    }

}
