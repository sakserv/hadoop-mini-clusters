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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;


public class HdfsLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = Logger.getLogger(HdfsLocalCluster.class);

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
        System.setProperty("test.build.data", DEFAULT_LOG_DIR);
    }

    public Configuration getConf() {
        return conf;
    }

    public void dumpConfig() {
        LOG.info("HDFS CONF:");
        Iterator it = conf.iterator();
        while(it.hasNext()) {
            LOG.info(it.next());
        }
    }

    public void start() {
        start(1);
    }

    public void start(int numOfDataNodes) {
        LOG.info("HDFS: Starting MiniDfsCluster");
        try {

            cluster = clusterBuilder.numDataNodes(numOfDataNodes)
            .format(true)
            .racks(null)
            .build();
            cluster.waitClusterUp();
            
        } catch(IOException e) {
            LOG.error("ERROR: Failed to start MiniDfsCluster");
            e.printStackTrace();
        }
    }

    public void stop() {
        LOG.info("HDFS: Stopping MiniDfsCluster");
        cluster.shutdown();
    }
    
    public void stop(boolean cleanUp) {
        stop();
        if(cleanUp) {
            cleanUp();
        }
        
    }
    
    public void cleanUp() {
        FileUtils.deleteFolder(DEFAULT_LOG_DIR);
    }

    public String getHdfsUriString() {
        String hdfsUriString = "";
        try {
            hdfsUriString = "hdfs://" + cluster.getFileSystem().getCanonicalServiceName();
        } catch(IOException e) {
            LOG.error("ERROR: Failed to return MiniDFsCluster URI");
            e.printStackTrace();
        }
        return hdfsUriString;
    }

    public FileSystem getHdfsFileSystemHandle() {
        FileSystem hdfsFileSystemHandle = null;
        try {
            hdfsFileSystemHandle = cluster.getFileSystem();
        } catch(IOException e) {
            LOG.error("ERROR: Failed to return MiniDFsCluster URI");
            e.printStackTrace();
        }
        return hdfsFileSystemHandle;
    }
}
