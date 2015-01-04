package com.github.skumpf.minicluster.impl;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import com.github.skumpf.minicluster.MiniCluster;

/**
 * Created by skumpf on 12/30/14.
 */
public class StormLocalCluster implements MiniCluster {

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
        System.out.println("STORM: Instantiating LocalCluster");
        cluster = new LocalCluster(zkHost, zkPort);
    }

    public void stop() {
        cluster.shutdown();
    }

    public void stop(String topologyName) {
        cluster.killTopology(topologyName);
        stop();
    }

    public void dumpConfig() {
        System.out.println("STORM CONFIG: " + conf.toString());
    }

    public Config getConf() {
        return conf;
    }

    public void submitTopology(String topologyName, Config conf, StormTopology topology) {
        cluster.submitTopology(topologyName, conf, topology);
    }

}
