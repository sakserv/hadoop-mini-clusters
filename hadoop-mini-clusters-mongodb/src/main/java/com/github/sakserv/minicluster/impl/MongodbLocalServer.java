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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbLocalServer implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongodbLocalServer.class);

    private String ip;
    private Integer port;
    
    private MongodStarter starter;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private IMongodConfig conf;
    
    private MongodbLocalServer(Builder builder) {
        this.ip = builder.ip;
        this.port = builder.port;
    }
    
    public String getIp() {
        return ip;
    }
    
    public Integer getPort() {
        return port;
    }
    
    public static class Builder {
        private String ip;
        private Integer port;
        
        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }
        
        public Builder setPort(int port){
            this.port = port;
            return this;
        }
        
        public MongodbLocalServer build() {
            MongodbLocalServer mongodbLocalServer = new MongodbLocalServer(this);
            validateObject(mongodbLocalServer);
            return  mongodbLocalServer;
        }

        private void validateObject(MongodbLocalServer mongodbLocalServer) {
            if(mongodbLocalServer.ip == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: MongoDB IP");
            }
            
            if(mongodbLocalServer.port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: MongoDB Port");
            }
        }
        
    }

    @Override
    public void start() throws Exception {
        LOG.info("MONGODB: Starting MongoDB on {}:{}", ip, port);
        starter = MongodStarter.getDefaultInstance();
        configure();
        mongodExe = starter.prepare(conf);
        mongod = mongodExe.start();
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("MONGODB: Stopping MongoDB on {}:{}", ip, port);
        mongod.stop();
        mongodExe.stop();
        if(cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        conf = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(ip, port, false))
                .build();
    }

    @Override
    public void cleanUp() throws Exception {

    }

}
