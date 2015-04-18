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

package com.github.sakserv.minicluster;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HbaseLocalClusterIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalClusterIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    private static HbaseLocalCluster hbaseLocalCluster;
    private static ZookeeperLocalCluster zookeeperLocalCluster;

@BeforeClass
    public static void setUp(){
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
            .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
            .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
            .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
            .build();
        zookeeperLocalCluster.start();
    }

    @AfterClass
    public static void tearDown(){
        zookeeperLocalCluster.stop();
    }

    @Test
    public void testHbaseLocalCluster() throws IOException {

        MiniHBaseCluster miniHBaseCluster;
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir", "embedded_hbase");
        configuration.set("hbase.replication", "false");
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperLocalCluster.getPort()));
        configuration.set("hbase.zookeeper.quorum", zookeeperLocalCluster.getZookeeperConnectionString());
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.set("hbase.master.info.port", "-1");
        try {
            miniHBaseCluster = new MiniHBaseCluster(configuration, 1);
            miniHBaseCluster.startMaster();
            miniHBaseCluster.startRegionServer();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }
}
