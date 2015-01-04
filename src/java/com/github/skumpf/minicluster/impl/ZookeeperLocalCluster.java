package com.github.skumpf.minicluster.impl;

import com.github.skumpf.minicluster.MiniCluster;
import com.github.skumpf.util.FileUtils;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    private static final String DEFAULT_ZK_TEMP_DIR = "embedded_zk";
    private static final int DEFAULT_ZK_PORT = 2181;

    private String zkTempDir;
    private TestingServer zkTestServer;
    private int zkPort;

    public ZookeeperLocalCluster() {
        zkPort = DEFAULT_ZK_PORT;
        zkTempDir = DEFAULT_ZK_TEMP_DIR;
        configure();
    }

    public ZookeeperLocalCluster(int zkPort) {
        this.zkPort = zkPort;
        zkTempDir = DEFAULT_ZK_TEMP_DIR;
        configure();
    }

    public ZookeeperLocalCluster(int zkPort, String zkTempDir) {
        this.zkPort = zkPort;
        this.zkTempDir = zkTempDir;
        configure();
    }

    // Curator does not leverage a configuration object
    public void configure() {}

    public void start() {
        System.out.println("ZOOKEEPER: Starting Zookeeper on port: " + zkPort);
        try {
            zkTestServer = new TestingServer(zkPort, new File(zkTempDir));
        } catch(Exception e) {
            System.out.println("ERROR: Failed to start Zookeeper");
            e.getStackTrace();
        }
    }

    public void stop()  {
        System.out.println("ZOOKEEPER: Stopping Zookeeper on port: " + zkPort);
        try {
            zkTestServer.stop();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void stop(boolean cleanUp) {
        stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(zkTempDir);
    }

    public String getZkConnectionString() {
        return zkTestServer.getConnectString();
    }

    public String getZkHostName() {
        return getZkConnectionString().split(":")[0];
    }

    public String getZkPort() {
        return getZkConnectionString().split(":")[1];
    }

    public void dumpConfig() {
        System.out.println("ZOOKEEPER CONFIG: " + zkTestServer.getTempDirectory());
    }

}
