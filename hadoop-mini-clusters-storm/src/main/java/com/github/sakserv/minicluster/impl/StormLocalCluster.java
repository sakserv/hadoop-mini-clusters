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

import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.MiniCluster;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StormLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(StormLocalCluster.class);
    
    private String zookeeperHost;
    private Long zookeeperPort;
    private Boolean enableDebug;
    private Integer numWorkers;
    private Config stormConf;
    private ILocalCluster localCluster;
    
    private StormLocalCluster(Builder builder) {
        this.zookeeperHost = builder.zookeeperHost;
        this.zookeeperPort = builder.zookeeperPort;
        this.enableDebug = builder.enableDebug;
        this.numWorkers = builder.numWorkers;
        this.stormConf = builder.stormConf;
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
    
    public Config getStormConf() { return stormConf; }
    
    public static class Builder {
        private String zookeeperHost;
        private Long zookeeperPort;
        private Boolean enableDebug;
        private Integer numWorkers;
        private Config stormConf;
        
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

        public Builder setStormConfig(Config stormConf) {
            this.stormConf = stormConf;
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

            if (stormLocalCluster.getStormConf() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Storm Config");
            }
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("STORM: Starting StormLocalCluster");
        configure();
        localCluster = Testing.getLocalCluster(stormConf);
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("STORM: Stopping StormLocalCluster");
        localCluster.shutdown();
        if(cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        stormConf.setDebug(enableDebug);
        stormConf.setNumWorkers(numWorkers);
        stormConf.put("nimbus-daemon", true);
        List<String> stormNimbusSeeds = new ArrayList<>();
        stormNimbusSeeds.add("localhost");
        stormConf.put(Config.NIMBUS_SEEDS, stormNimbusSeeds);
        stormConf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        stormConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "org.apache.storm.security.auth.SimpleTransportPlugin");
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);
        stormConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 5);
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
        stormConf.put(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 1048576);
        stormConf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(getZookeeperHost()));
        stormConf.put(Config.STORM_ZOOKEEPER_PORT, getZookeeperPort());
    }

    @Override
    public void cleanUp() throws Exception {
      FileUtils.deleteFolder("logs");
    }

    public void submitTopology(String topologyName, Config conf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException {
        localCluster.submitTopology(topologyName, conf, topology);
    }

    public void stop(String topologyName) throws Exception {
        try {
            localCluster.killTopology(topologyName);
        } catch (NotAliveException e) {
            LOG.debug("Topology not running: " + topologyName);
        }
        stop();
    }

}
