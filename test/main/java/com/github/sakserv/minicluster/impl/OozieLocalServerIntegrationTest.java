package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class OozieLocalServerIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServerIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    private static OozieLocalServer localServer;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        localServer = new OozieLocalServer.Builder()
                .setHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .build();

        localServer.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        localServer.stop(true);
    }

    @Test
    public void submitWorkflow() throws Exception {
        OozieClient oozie = localServer.getOozie();
        FileSystem fs = localServer.getFileSystem();
        localServer.dumpConfig();

        Path appPath = new Path(fs.getHomeDirectory(), "testApp");
        fs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");

        //write workflow.xml
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" +
                "    <start to='end'/>" +
                "    <end name='end'/>" +
                "</workflow-app>";

        Writer writer = new OutputStreamWriter(fs.create(workflow));
        writer.write(wfApp);
        writer.close();

        //write job.properties
        Properties conf = oozie.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, workflow.toString());
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

        //submit and check
        final String jobId = oozie.submit(conf);
        WorkflowJob wf = oozie.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        LOG.info(wf.toString());
    }
}
