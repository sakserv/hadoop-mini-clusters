package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import org.apache.oozie.client.OozieClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * Created by vzlatkin on 6/28/15.
 */
public class OozieLocalServerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServerTest.class);

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

    private static OozieLocalServer localServer;

    @BeforeClass
    public static void setUp()  {
        localServer = new OozieLocalServer.Builder()
                .setHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .build();
    }

    @Test
    public void startOozie() throws Exception {
        localServer.start();

        OozieClient oozie = localServer.getOozie();
        Assert.notNull(oozie.getClientBuildVersion());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        localServer.stop(true);
    }
}
