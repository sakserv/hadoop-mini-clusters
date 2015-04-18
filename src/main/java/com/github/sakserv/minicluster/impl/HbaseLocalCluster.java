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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseLocalCluster.class);

    MiniHBaseCluster miniHBaseCluster;

    private Integer hbaseMasterPort;
    private Integer numRegionServers;
    private String hbaseRootDir;
    private Integer zookeeperPort;
    private String zookeeperConnectionString;
    private String zookeeperZnodeParent;

    private HbaseLocalCluster(Builder builder) {

    }

    public static class Builder {

    }

    public void start() {}
    public void stop(){}
    public void configure() {}


}
