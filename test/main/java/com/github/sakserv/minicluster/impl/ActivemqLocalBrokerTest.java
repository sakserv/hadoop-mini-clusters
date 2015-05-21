package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

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
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    // Setup the activemq broker before running tests
    private static ActivemqLocalBroker amq;
    @BeforeClass
    public static void setUp() {
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
    public void testHostname() {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_HOSTNAME_KEY), amq.getHostName());
    }

    @Test
    public void testPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.ACTIVEMQ_PORT_KEY)), amq.getPort());
    }

    @Test
    public void testQueueName() {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_QUEUE_NAME_KEY), amq.getQueueName());
    }
    
    @Test
    public void testStoreDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_STORE_DIR_KEY), amq.getStoreDir());
    }

    @Test
    public void testUriPrefix() {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_PREFIX_KEY), amq.getUriPrefix());
    }

    @Test
    public void testUriPostfix() {
        assertEquals(propertyParser.getProperty(ConfigVars.ACTIVEMQ_URI_POSTFIX_KEY), amq.getUriPostfix());
    }
}
