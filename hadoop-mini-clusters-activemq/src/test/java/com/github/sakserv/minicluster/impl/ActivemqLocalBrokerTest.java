package com.github.sakserv.minicluster.impl;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;

public class ActivemqLocalBrokerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ActivemqLocalBrokerTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();
    
    // Setup the activemq broker before running tests
    private static ActivemqLocalBroker amq;


    @Before
    public void setUp() throws Exception {
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }
    
    @Test
    public void testHostname() throws Exception {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY), amq.getHostName());
    }

    @Test
    public void testMissingHostname() throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }

    @Test
    public void testPort() throws Exception {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)), amq.getPort());
    }

    @Test
    public void testMissingPort() throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }

    @Test
    public void testQueueName() throws Exception {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY), amq.getQueueName());
    }

    @Test
    public void testMissingQueueName() throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }
    
    @Test
    public void testStoreDir() throws Exception {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY), amq.getStoreDir());
    }

    @Test
    public void testMissingStoreDir() throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }

    @Test
    public void testUriPrefix() throws Exception {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY), amq.getUriPrefix());
    }

    @Test
    public void testMissingUriPrefix() throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();
    }

    @Test
    public void testUriPostfix() throws Exception {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY), amq.getUriPostfix());
    }

    @Test
    public void testMissingUriPostfix()  throws Exception {
        exception.expect(IllegalArgumentException.class);
        amq = new ActivemqLocalBroker.Builder()
                .setHostName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .build();
    }

    @Test
    public void testStopThrowsExceptionIfNotStarted() throws Exception {
        amq = new ActivemqLocalBroker.Builder()
                .setHostName("fakehostname")
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)))
                .setQueueName(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY))
                .setStoreDir(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY))
                .setUriPrefix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY))
                .setUriPostfix(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY))
                .build();

        exception.expect(NullPointerException.class);
        amq.stop();
        amq.cleanUp();
    }
}
