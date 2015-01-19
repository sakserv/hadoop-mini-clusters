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
import org.apache.log4j.Logger;

public class StormLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = Logger.getLogger(StormLocalCluster.class);

    LocalCluster cluster;
    private String zkHost;
    private Long zkPort;
    Config conf = new Config();

    public StormLocalCluster(String zkHost, Long zkPort) {
        configure();
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }

    public void configure() {
        conf.setDebug(false);
        conf.setNumWorkers(3);
    }

    public void start() {
        LOG.info("STORM: Instantiating LocalCluster");
        cluster = new LocalCluster(zkHost, zkPort);
    }

    public void stop() {
        cluster.shutdown();
    }

    public void stop(String topologyName) {
        cluster.killTopology(topologyName);
        stop();
    }
    
    public void stop(String topologyName, int waitSecs) {
        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(waitSecs);
        cluster.killTopologyWithOpts(topologyName, killOptions);
        stop();
    }

    public void dumpConfig() {
        LOG.info("STORM CONFIG: " + conf.toString());
    }

    public Config getConf() {
        return conf;
    }

    public void submitTopology(String topologyName, Config conf, StormTopology topology) {
        cluster.submitTopology(topologyName, conf, topology);
    }

}
