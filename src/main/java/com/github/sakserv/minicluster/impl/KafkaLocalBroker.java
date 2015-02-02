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
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * In memory Kafka Broker for testing
 */

public class KafkaLocalBroker implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocalBroker.class);
    
    private KafkaServer kafkaServer;
    private KafkaConfig kafkaConfig;
    
    private String kafkaHostname;
    private Integer kafkaPort;
    private Integer kafkaBrokerId;
    private Properties kafkaProperties;
    private String kafkaTempDir;
    private String zookeeperConnectionString;

    public String getKafkaHostname() {
        return kafkaHostname;
    }

    public Integer getKafkaPort() {
        return kafkaPort;
    }

    public Integer getKafkaBrokerId() {
        return kafkaBrokerId;
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    public String getKafkaTempDir() {
        return kafkaTempDir;
    }

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }
    
    private KafkaLocalBroker(Builder builder) {
        this.kafkaHostname = builder.kafkaHostname;
        this.kafkaPort = builder.kafkaPort;
        this.kafkaBrokerId = builder.kafkaBrokerId;
        this.kafkaProperties = builder.kafkaProperties;
        this.kafkaTempDir = builder.kafkaTempDir;
        this.zookeeperConnectionString = builder.zookeeperConnectionString;
        
    }

    
    public static class Builder {
        private String kafkaHostname;
        private Integer kafkaPort;
        private Integer kafkaBrokerId;
        private Properties kafkaProperties;
        private String kafkaTempDir;
        private String zookeeperConnectionString;
        
        public Builder setKafkaHostname(String kafkaHostname) {
            this.kafkaHostname = kafkaHostname;
            return this;
        }
        
        public Builder setKafkaPort(Integer kafkaPort) {
            this.kafkaPort = kafkaPort;
            return this;
        }
        
        public Builder setKafkaBrokerId(Integer kafkaBrokerId){
            this.kafkaBrokerId = kafkaBrokerId;
            return this;
        }
        
        public Builder setKafkaProperties(Properties kafkaProperties) {
            this.kafkaProperties = kafkaProperties;
            return this;
        }
        
        public Builder setKafkaTempDir(String kafkaTempDir) {
            this.kafkaTempDir = kafkaTempDir;
            return this;
        }
        
        public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
            this.zookeeperConnectionString = zookeeperConnectionString;
            return this;
        }
        
        public KafkaLocalBroker build() {
            KafkaLocalBroker kafkaLocalBroker = new KafkaLocalBroker(this);
            validateObject(kafkaLocalBroker);
            return kafkaLocalBroker;
        }
        
        public void validateObject(KafkaLocalBroker kafkaLocalBroker) {
            if(kafkaLocalBroker.kafkaHostname == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Hostname");
            }

            if(kafkaLocalBroker.kafkaPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Port");
            }

            if(kafkaLocalBroker.kafkaBrokerId == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Broker Id");
            }

            if(kafkaLocalBroker.kafkaProperties == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Properties");
            }

            if(kafkaLocalBroker.kafkaTempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Kafka Temp Dir");
            }

            if(kafkaLocalBroker.zookeeperConnectionString == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Zookeeper Connection String");
            }
        }
    }
    /**
     * default constructor
     */
    
    public void configure() {
        kafkaProperties.put("advertised.host.name", kafkaHostname);
        kafkaProperties.put("port", kafkaPort+"");
        kafkaProperties.put("broker.id", kafkaBrokerId+"");
        kafkaProperties.put("log.dir", kafkaTempDir);
        kafkaProperties.put("enable.zookeeper", "true");
        kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
        kafkaConfig = new KafkaConfig(kafkaProperties);
    }

    public void start() {
        configure();
        kafkaServer = new KafkaServer(kafkaConfig, new LocalSystemTime());
        LOG.info("KAFKA: Starting Kafka on port: " + kafkaPort);
        kafkaServer.startup();
    }

    public void stop() {
        stop(true);
    }

    public void stop(boolean cleanUp){
        LOG.info("KAFKA: Stopping Kafka on port: " + kafkaPort);
        kafkaServer.shutdown();

        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(kafkaTempDir);
    }
    
    public class LocalSystemTime implements Time {

        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                // no stress
            }
        }

    }
}