package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.util.Properties;

/**
 * Created by Vlad on 1/10/2015.
 */
public class ActivemqLocalBroker implements MiniCluster {
    private final int port = 61616;
    private String queueName;
    private BrokerService broker;
    private Destination dest;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    
    private String DEFAULT_STORAGE_PATH = "activemq-data";

    public ActivemqLocalBroker() {
        this("defaultQueue");
    }

    public ActivemqLocalBroker(String queueName) {
        this.queueName = queueName;

    }

    @Override
    public void start() {
        String uri = "vm://localhost:" + port;
        try {
            Properties props = System.getProperties();
            props.setProperty("activemq.store.dir", DEFAULT_STORAGE_PATH);
            
            broker = new BrokerService();
            broker.addConnector(uri);
            broker.start();

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri + "?create=false");
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
        FileUtils.deleteFolder(DEFAULT_STORAGE_PATH);
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
