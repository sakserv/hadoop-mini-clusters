package com.github.sakserv.minicluster;

import com.github.sakserv.minicluster.impl.ActivemqLocalBroker;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;

import static org.junit.Assert.assertEquals;

public class ActivemqLocalBrokerTest {

    private static final Logger LOG = Logger.getLogger(ActivemqLocalBrokerTest.class);
    
    private ActivemqLocalBroker amq;

    @Before
    public void setUp() {
        amq = new ActivemqLocalBroker();
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
