package com.github.sakserv.minicluster;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.ActivemqLocalBroker;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ActivemqLocalBrokerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ActivemqLocalBrokerTest.class);

    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        } catch(IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    private ActivemqLocalBroker amq;

    @Before
    public void setUp() throws IOException {
        amq = new ActivemqLocalBroker.ActivemqLocalBrokerBuilder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_VAR))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_VAR)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_VAR))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_VAR))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_VAR))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_VAR))
                .build();
        
        amq.start();
    }

    @Test
    /*
    sends lots of short messages and one long one
     */
    public void testMessageProcessing() throws JMSException {
        int n = 10000;
        String msg;
        
        LOG.info("ACTIVEMQ: Sending " + n + " messages");

        //send a lot of messages
        for (int i = 0; i < n; i++) {
            msg = "hello from active mq. " + n;
            amq.sendTextMessage(msg);
            assertEquals(msg,amq.getTextMessage());
        }

        //send a really long message
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(n).append(" ");
        }
        msg = sb.toString();
        amq.sendTextMessage(msg);
        assertEquals(msg,amq.getTextMessage());

    }

    @After
    public void tearDown() {
        amq.stop(true );
    }

}
