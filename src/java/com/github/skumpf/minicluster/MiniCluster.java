package com.github.skumpf.minicluster;

/**
 * Created by skumpf on 12/30/14.
 */
public interface MiniCluster {

    public void start();

    public void stop();

    public void configure();

    public void dumpConfig();

}
