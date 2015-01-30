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
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;

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
        
        public MongodbLocalServer build() throws IOException {
            MongodbLocalServer mongodbLocalServer = new MongodbLocalServer(this);
            validateObject(mongodbLocalServer);
            return  mongodbLocalServer;
        }

        private void validateObject(MongodbLocalServer mongodbLocalServer) throws IOException {
            PropertyParser propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);

            if(mongodbLocalServer.ip == null) {
                this.ip = propertyParser.getProperty(ConfigVars.MONGO_IP_KEY);
            }
            
            if(mongodbLocalServer.port == null) {
                this.port = Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY));
            }
        }
        
    }
    
    public void start() {
        try {
            starter = MongodStarter.getDefaultInstance();
            configure();
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
                    .net(new Net(ip, port, false))
                    .build();
        } catch(IOException e) {
            e.printStackTrace();
        }
        
    }

}
