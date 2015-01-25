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
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
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
    
    private ActivemqLocalBroker(ActivemqLocalBrokerBuilder builder) {
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

    
    public static class ActivemqLocalBrokerBuilder
    {
        private String hostName;
        private Integer port;
        private String queueName;
        private String storeDir;
        private String uriPrefix;
        private String uriPostfix;

        public ActivemqLocalBrokerBuilder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public ActivemqLocalBrokerBuilder setPort(int port) {
            this.port = port;
            return this;
        }
        
        public ActivemqLocalBrokerBuilder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public ActivemqLocalBrokerBuilder setStoreDir(String storeDir) {
            this.storeDir = storeDir;
            return this;
        }

        public ActivemqLocalBrokerBuilder setUriPrefix(String uriPrefix) {
            this.uriPrefix = uriPrefix;
            return this;
        }

        public ActivemqLocalBrokerBuilder setUriPostfix(String uriPostfix) {
            this.uriPostfix = uriPostfix;
            return this;
        }
        
        public ActivemqLocalBroker build() throws IOException {
            ActivemqLocalBroker activemqLocalBroker = new ActivemqLocalBroker(this);
            validateObject(activemqLocalBroker);
            return activemqLocalBroker;
        }
        
        private void validateObject(ActivemqLocalBroker activemqLocalBroker) throws IOException {
            PropertyParser propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            
            if(activemqLocalBroker.hostName == null) {
                this.hostName = propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_VAR);
            }

            if(activemqLocalBroker.port == null) {
                this.port = Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_VAR));
            }

            if(activemqLocalBroker.queueName == null) {
                this.queueName = propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_VAR);
            }

            if(activemqLocalBroker.storeDir == null) {
                this.storeDir = propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_VAR);
            }

            if(activemqLocalBroker.uriPrefix == null) {
                this.uriPrefix = propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_VAR);
            }

            if(activemqLocalBroker.uriPostfix == null) {
                this.uriPostfix = propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_VAR);
            }
        }
        
    }

    @Override
    public void start() {
        String uri = uriPrefix + hostName + ":" + port;
        try {
            Properties props = System.getProperties();
            props.setProperty(ConfigVars.ACTIVEMQ_STORE_DIR_VAR, storeDir);
            
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

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            if (consumer != null ) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }
            broker.stop();
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop(boolean cleanUp) {
        stop();
        if(cleanUp) {
            cleanUp();
        }
    }
    public void cleanUp() {
        FileUtils.deleteFolder(storeDir);
    }
    
    @Override
    public void configure() {
    }

    @Override
    public void dumpConfig() {
        System.out.println(broker.getVmConnectorURI());
    }

    public void sendTextMessage(String text) throws JMSException {
        TextMessage msg = session.createTextMessage(text);
        producer.send(dest,msg);
    }
    public String getTextMessage() throws JMSException {
        Message msg = consumer.receive(100);
        if (msg instanceof TextMessage) {
            return ((TextMessage) msg).getText();
        }
        return "";
    }
}
