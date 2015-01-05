package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;

/**
 * Created by skumpf on 12/6/14.
 */
public class HdfsLocalCluster implements MiniCluster {

    private static MiniDFSCluster.Builder clusterBuilder;
    private static MiniDFSCluster cluster;
    private static Configuration conf = new Configuration();
    private static final String DEFAULT_LOG_DIR = "embedded_hdfs";

    public HdfsLocalCluster() {
        configure();
        clusterBuilder = new MiniDFSCluster.Builder(getConf());
    }

    public void configure() {
        conf.setBoolean("dfs.permissions", false);
    }

    public Configuration getConf() {
        return conf;
    }

    public void dumpConfig() {
        System.out.println("HDFS CONF: " + String.valueOf(conf.toString()));
    }

    public void start() {
        start(1);
    }

    public void start(int numOfDataNodes) {
        System.out.println("HDFS: Starting MiniDfsCluster");
        try {

            cluster = clusterBuilder.numDataNodes(numOfDataNodes)
            .format(true)
            .racks(null)
            .build();

            System.out.println("HDFS: MiniDfsCluster started at " + cluster.getFileSystem().getCanonicalServiceName());
        } catch(IOException e) {
            System.out.println("ERROR: Failed to start MiniDfsCluster");
            e.printStackTrace();
        }
    }

    public void stop() {
        System.out.println("HDFS: Stopping MiniDfsCluster");
        cluster.shutdown();
        System.out.println("HDFS: MiniDfsCluster Stopped");

    }

    public String getHdfsUriString() {
        String hdfsUriString = "";
        try {
            hdfsUriString = "hdfs://" + cluster.getFileSystem().getCanonicalServiceName();
        } catch(IOException e) {
            System.out.println("ERROR: Failed to return MiniDFsCluster URI");
            e.printStackTrace();
        }
        return hdfsUriString;
    }

    public FileSystem getHdfsFileSystemHandle() {
        FileSystem hdfsFileSystemHandle = null;
        try {
            hdfsFileSystemHandle = cluster.getFileSystem();
        } catch(IOException e) {
            System.out.println("ERROR: Failed to return MiniDFsCluster URI");
            e.printStackTrace();
        }
        return hdfsFileSystemHandle;
    }
}
