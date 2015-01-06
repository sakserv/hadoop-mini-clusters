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
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;

public class MongodbLocalServer implements MiniCluster {

    private static final String DEFAULT_DATABASE_NAME = "test_database";
    private static final String DEFAULT_COLLECTION_NAME = "test_collection";
    private static final int DEFAULT_PORT = 12345;

    private String dbName;
    private String collName;
    private int port;
    
    private MongodStarter starter;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private IMongodConfig conf;
    
    public MongodbLocalServer() {
        dbName = DEFAULT_DATABASE_NAME;
        collName = DEFAULT_COLLECTION_NAME;
        port = DEFAULT_PORT;
        configure();
    }
    
    public MongodbLocalServer(String dbName, String collName, int port) {
        this.dbName = dbName;
        this.collName = collName;
        this.port = port;
        configure();
    }
    
    public void start() {
        try {
            starter = MongodStarter.getDefaultInstance();
            mongodExe = starter.prepare(conf);
            mongod = mongodExe.start();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    public void stop() {
        mongod.stop();
        mongodExe.stop();
    }

    public void configure() {
        try {
            conf = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(port, Network.localhostIsIPv6()))
                    .build();
        } catch(IOException e) {
            e.printStackTrace();
        }
        
    }
    
    public void dumpConfig() {
        System.out.println("MONGODB: CONFIG: " + conf.toString());
        
    }
}
