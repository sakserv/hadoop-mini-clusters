package com.github.sakserv.minicluster.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import com.github.sakserv.minicluster.oozie.sharelib.Framework;
import com.github.sakserv.minicluster.oozie.sharelib.util.OozieShareLibUtil;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;


public class OozieLocalServerIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServerIntegrationTest.class);

    // Test path
    private static final String TEST_INPUT_DIR = "/tmp/test_input_dir";
    private static final String TEST_INPUT_FILE = "test_input.txt";
    private static final String TEST_OUTPUT_DIR = "/tmp/test_output_dir";

    // Setup the property parser
    private static PropertyParser propertyParser;
    private static OozieLocalServer oozieLocalServer;
    private static HdfsLocalCluster hdfsLocalCluster;
    private static MRLocalCluster mrLocalCluster;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {

        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();

        mrLocalCluster = new MRLocalCluster.Builder()
                .setNumNodeManagers(Integer.parseInt(propertyParser.getProperty(ConfigVars.YARN_NUM_NODE_MANAGERS_KEY)))
                .setJobHistoryAddress(propertyParser.getProperty(ConfigVars.MR_JOB_HISTORY_ADDRESS_KEY))
                .setResourceManagerAddress(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setResourceManagerHostname(propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_HOSTNAME_KEY))
                .setResourceManagerSchedulerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY))
                .setResourceManagerResourceTrackerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY))
                .setResourceManagerWebappAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY))
                .setUseInJvmContainerExecutor(Boolean.parseBoolean(propertyParser.getProperty(
                        ConfigVars.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY)))
                .setHdfsDefaultFs(hdfsLocalCluster.getHdfsConfig().get("fs.defaultFS"))
                .setConfig(hdfsLocalCluster.getHdfsConfig())
                .build();

        mrLocalCluster.start();

        oozieLocalServer = new OozieLocalServer.Builder()
                .setOozieTestDir(propertyParser.getProperty(ConfigVars.OOZIE_TEST_DIR_KEY))
                .setOozieHomeDir(propertyParser.getProperty(ConfigVars.OOZIE_HOME_DIR_KEY))
                .setOozieUsername(System.getProperty("user.name"))
                .setOozieGroupname(propertyParser.getProperty(ConfigVars.OOZIE_GROUPNAME_KEY))
                .setOozieYarnResourceManagerAddress(propertyParser.getProperty(
                        ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY))
                .setOozieHdfsDefaultFs(hdfsLocalCluster.getHdfsConfig().get("fs.defaultFS"))
                .setOozieConf(mrLocalCluster.getConfig())
                .setOozieHdfsShareLibDir(propertyParser.getProperty(ConfigVars.OOZIE_HDFS_SHARE_LIB_DIR_KEY))
                .setOozieShareLibCreate(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.OOZIE_SHARE_LIB_CREATE_KEY)))
                .setOozieLocalShareLibCacheDir(propertyParser.getProperty(
                        ConfigVars.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY))
                .setOoziePurgeLocalShareLibCache(Boolean.parseBoolean(propertyParser.getProperty(
                        ConfigVars.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY)))
                .setOozieShareLibFrameworks(Lists.newArrayList(Framework.MAPREDUCE_STREAMING, Framework.OOZIE))
                .build();

        OozieShareLibUtil oozieShareLibUtil = new OozieShareLibUtil(oozieLocalServer.getOozieHdfsShareLibDir(),
                oozieLocalServer.getOozieShareLibCreate(), oozieLocalServer.getOozieLocalShareLibCacheDir(),
                oozieLocalServer.getOoziePurgeLocalShareLibCache(), hdfsLocalCluster.getHdfsFileSystemHandle(),
                oozieLocalServer.getOozieShareLibFrameworks());
        oozieShareLibUtil.createShareLib();
        oozieLocalServer.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        oozieLocalServer.stop();
        mrLocalCluster.stop();
        hdfsLocalCluster.stop();
    }

    @Test
    public void testSubmitWorkflow() throws Exception {

        LOG.info("OOZIE: Test Submit Workflow Start");

        FileSystem hdfsFs = hdfsLocalCluster.getHdfsFileSystemHandle();
        OozieClient oozie = oozieLocalServer.getOozieClient();

        Path appPath = new Path(hdfsFs.getHomeDirectory(), "testApp");
        hdfsFs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");

        // Setup input directory and file
        hdfsFs.mkdirs(new Path(TEST_INPUT_DIR));
        hdfsFs.copyFromLocalFile(
                new Path(getClass().getClassLoader().getResource(TEST_INPUT_FILE).toURI()), new Path(TEST_INPUT_DIR));

        //write workflow.xml
        String wfApp = "<workflow-app name=\"sugar-option-decision\" xmlns=\"uri:oozie:workflow:0.5\">\n" +
                "  <global>\n" +
                "    <job-tracker>${jobTracker}</job-tracker>\n" +
                "    <name-node>${nameNode}</name-node>\n" +
                "    <configuration>\n" +
                "      <property>\n" +
                "        <name>mapreduce.output.fileoutputformat.outputdir</name>\n" +
                "        <value>" + TEST_OUTPUT_DIR + "</value>\n" +
                "      </property>\n" +
                "      <property>\n" +
                "        <name>mapreduce.input.fileinputformat.inputdir</name>\n" +
                "        <value>" + TEST_INPUT_DIR + "</value>\n" +
                "      </property>\n" +
                "    </configuration>\n" +
                "  </global>\n" +
                "  <start to=\"first\"/>\n" +
                "  <action name=\"first\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + TEST_OUTPUT_DIR + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"decision-second-option\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <decision name=\"decision-second-option\">\n" +
                "    <switch>\n" +
                "      <case to=\"option\">${doOption}</case>\n" +
                "      <default to=\"second\"/>\n" +
                "    </switch>\n" +
                "  </decision>\n" +
                "  <action name=\"option\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + TEST_OUTPUT_DIR + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"second\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <action name=\"second\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + TEST_OUTPUT_DIR + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"end\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <kill name=\"kill\">\n" +
                "    <message>\n" +
                "      Failed to workflow, error message[${wf: errorMessage (wf: lastErrorNode ())}]\n" +
                "    </message>\n" +
                "  </kill>\n" +
                "  <end name=\"end\"/>\n" +
                "</workflow-app>";

        Writer writer = new OutputStreamWriter(hdfsFs.create(workflow));
        writer.write(wfApp);
        writer.close();

        //write job.properties
        Properties conf = oozie.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, workflow.toString());
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        conf.setProperty("nameNode", "hdfs://localhost:" + hdfsLocalCluster.getHdfsNamenodePort());
        conf.setProperty("jobTracker", mrLocalCluster.getResourceManagerAddress());
        conf.setProperty("doOption", "true");

        //submit and check
        final String jobId = oozie.run(conf);
        WorkflowJob wf = oozie.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.RUNNING, wf.getStatus());


        while(true){
            Thread.sleep(1000);
            wf = oozie.getJobInfo(jobId);
            if(wf.getStatus() == WorkflowJob.Status.FAILED || wf.getStatus() == WorkflowJob.Status.KILLED || wf.getStatus() == WorkflowJob.Status.PREP || wf.getStatus() == WorkflowJob.Status.SUCCEEDED){
                break;
            }
        }

        wf = oozie.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());

        LOG.info("OOZIE: Workflow: {}", wf.toString());
        hdfsFs.close();

    }

    @Test
    public void testSubmitCoordinator() throws Exception {

        LOG.info("OOZIE: Test Submit Coordinator Start");

        FileSystem hdfsFs = hdfsLocalCluster.getHdfsFileSystemHandle();
        OozieClient oozie = oozieLocalServer.getOozieCoordClient();

        Path appPath = new Path(hdfsFs.getHomeDirectory(), "testApp");
        hdfsFs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");
        Path coordinator = new Path(appPath, "coordinator.xml");

        //write workflow.xml
        String wfApp =
                "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" +
                        "    <start to='end'/>" +
                        "    <end name='end'/>" +
                        "</workflow-app>";

        String coordApp =
                "<coordinator-app timezone='UTC' end='2016-07-26T02:26Z' start='2016-07-26T01:26Z' frequency='${coord:hours(1)}' name='test-coordinator' xmlns='uri:oozie:coordinator:0.4'>" +
                        "    <action>" +
                        "        <workflow>" +
                        "            <app-path>" + workflow.toString() + "</app-path>" +
                        "        </workflow>" +
                        "    </action>" +
                        "</coordinator-app>";

        Writer writer = new OutputStreamWriter(hdfsFs.create(workflow));
        writer.write(wfApp);
        writer.close();

        Writer coordWriter = new OutputStreamWriter(hdfsFs.create(coordinator));
        coordWriter.write(coordApp);
        coordWriter.close();

        //write job.properties
        Properties conf = oozie.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, coordinator.toString());
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

        //submit and check
        final String jobId = oozie.submit(conf);
        CoordinatorJob coord  = oozie.getCoordJobInfo(jobId);
        assertNotNull(coord);
        assertEquals(Job.Status.PREP, coord.getStatus());

        LOG.info("OOZIE: Coordinator: {}", coord.toString());
        hdfsFs.close();
    }

    @Test
    public void testOozieShareLib() throws Exception {
        LOG.info("OOZIE: Test Oozie Share Lib Start");

        FileSystem hdfsFs = hdfsLocalCluster.getHdfsFileSystemHandle();
        OozieShareLibUtil oozieShareLibUtil = new OozieShareLibUtil(oozieLocalServer.getOozieHdfsShareLibDir(),
                oozieLocalServer.getOozieShareLibCreate(), oozieLocalServer.getOozieLocalShareLibCacheDir(),
                oozieLocalServer.getOoziePurgeLocalShareLibCache(), hdfsFs,
                oozieLocalServer.getOozieShareLibFrameworks());
        oozieShareLibUtil.createShareLib();

        // Validate the share lib dir was created and contains a single directory
        FileStatus[] fileStatuses = hdfsFs.listStatus(new Path(oozieLocalServer.getOozieHdfsShareLibDir()));
        assertEquals(1, fileStatuses.length);
        hdfsFs.close();
    }
}
