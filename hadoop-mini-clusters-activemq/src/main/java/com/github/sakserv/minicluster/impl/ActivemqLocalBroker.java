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
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Properties;

public class ActivemqLocalBroker implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ActivemqLocalBroker.class);

    private final String hostName;
    private final Integer port;
    private final String queueName;
    private final String storeDir;
    private final String uriPrefix;
    private final String uriPostfix;
    
    private BrokerService broker;
    private Destination dest;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    
    private ActivemqLocalBroker(Builder builder) {
        this.hostName = builder.hostName;
        this.port = builder.port;
        this.queueName = builder.queueName;
        this.storeDir = builder.storeDir;
        this.uriPrefix = builder.uriPrefix;
        this.uriPostfix = builder.uriPostfix;
    }
    
    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public String getStoreDir() {
        return storeDir;
    }
    
    public String getUriPrefix() {
        return uriPrefix;
    }
    
    public String getUriPostfix() {
        return uriPostfix;
    }

    
    public static class Builder
    {
        private String hostName;
        private Integer port;
        private String queueName;
        private String storeDir;
        private String uriPrefix;
        private String uriPostfix;

        public Builder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }
        
        public Builder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder setStoreDir(String storeDir) {
            this.storeDir = storeDir;
            return this;
        }

        public Builder setUriPrefix(String uriPrefix) {
            this.uriPrefix = uriPrefix;
            return this;
        }

        public Builder setUriPostfix(String uriPostfix) {
            this.uriPostfix = uriPostfix;
            return this;
        }
        
        public ActivemqLocalBroker build() {
            ActivemqLocalBroker activemqLocalBroker = new ActivemqLocalBroker(this);
            validateObject(activemqLocalBroker);
            return activemqLocalBroker;
        }
        
        private void validateObject(ActivemqLocalBroker activemqLocalBroker) {
            if(activemqLocalBroker.hostName == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ HostName");
            }

            if(activemqLocalBroker.port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ Port");
            }

            if(activemqLocalBroker.queueName == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ Queue Name");
            }

            if(activemqLocalBroker.storeDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ Store Dir");
            }

            if(activemqLocalBroker.uriPrefix == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ Uri Prefix");
            }

            if(activemqLocalBroker.uriPostfix == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: ActiveMQ Uri Postfix");
            }
        }
        
    }

    @Override
    public void start() throws Exception {
        String uri = uriPrefix + hostName + ":" + port;
        LOG.info("ACTIVEMQ: Starting ActiveMQ on {}", uri);
        configure();

        broker = new BrokerService();
        broker.addConnector(uri);
        broker.start();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri + uriPostfix);
        Connection conn = factory.createConnection();
        conn.start();

        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        dest = session.createQueue(queueName);
        consumer = session.createConsumer(dest);
        producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("ACTIVEMQ: Stopping ActiveMQ");
        consumer.close();
        session.close();
        broker.stop();

        if(cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY, storeDir);
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(storeDir);
    }

    public void sendTextMessage(String text) throws JMSException {
        TextMessage msg = session.createTextMessage(text);
        producer.send(dest,msg);
    }
    public String getTextMessage() throws JMSException {
        Message msg = consumer.receive(100);
        return ((TextMessage) msg).getText();
    }
}
