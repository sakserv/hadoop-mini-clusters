package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * code to create an in-memory Oozie server
 *
 * work is largely based on org.apache.oozie.test.XTestCase, but does not require a checkout of Oozie source code to function
 */
public class OozieLocalServer implements MiniCluster {
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServer.class);

    private File oozieHome = new File("target/oozie");
    private HdfsLocalCluster hdfs;
    private MRLocalCluster mr;
    private OozieClient oozie;
    private String oozieUser = System.getProperty("user.name");
    private String oozieGroup = "testg";

    public OozieLocalServer(Builder builder) {
        this.oozieHome = new File(builder.dir);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Oozie: Starting local server");

        File oozieConfDir, hadoopConfDir, target;
        Configuration oozieSiteConf, hadoopConf, jobConf;

        configure();

        //create local test directory
        oozieConfDir = new File(oozieHome, "/conf");
        oozieConfDir.mkdirs();
        hadoopConfDir = new File(oozieConfDir, "hadoop-conf");
        hadoopConfDir.mkdirs();
        new File(oozieConfDir, "action-conf").mkdirs();

        //create oozie-site.xml in memory
        oozieSiteConf = new Configuration(false);
        oozieSiteConf.set("oozie.service.JPAService.jdbc.driver", "org.hsqldb.jdbcDriver");
        oozieSiteConf.set("oozie.service.JPAService.jdbc.url", "jdbc:hsqldb:mem:oozie-db;create=true");
        oozieSiteConf.set(JPAService.CONF_CREATE_DB_SCHEMA, "true");

        target = new File(oozieConfDir, "oozie-site.xml");
        oozieSiteConf.writeXml(new FileOutputStream(target));

        //write hadoop-site.xml
        hadoopConf = new Configuration(false);
        hadoopConf.set("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@LOCALREAL");
        hadoopConf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@LOCALREALM");
        hadoopConf.set("mapreduce.framework.name", "yarn");
        target = new File(hadoopConfDir, "hadoop-site.xml");
        hadoopConf.writeXml(new FileOutputStream(target));

        //setup users
        UserGroupInformation.createUserForTesting(oozieUser, new String[]{oozieGroup});
        JobConf conf = createDFSConfig();

        //setup MR and HDFS
        PropertyParser propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
        propertyParser.parsePropsFile();
        hdfs = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsConfig(conf)
                .build();
        hdfs.start();

        String defaultFs = hdfs.getHdfsConfig().get("fs.defaultFS");
        mr = new MRLocalCluster.Builder()
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
                .setHdfsDefaultFs(defaultFs)
                .setConfig(conf)
                .build();
        mr.start();

        jobConf = new JobConf(mr.getConfig());
        target = new File(hadoopConfDir, "core-site.xml");
        jobConf.writeXml(new FileOutputStream(target));

        //set system properties
        System.setProperty(Services.OOZIE_HOME_DIR, oozieHome.getAbsolutePath());
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, oozieConfDir.getAbsolutePath());
        System.setProperty("oozielocal.log", oozieHome + "/oozielocal.log");
        System.setProperty(XTestCase.OOZIE_TEST_JOB_TRACKER, propertyParser.getProperty(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY));
        System.setProperty(XTestCase.OOZIE_TEST_NAME_NODE, defaultFs);
        System.setProperty("oozie.test.db.host", "localhost");
        System.setProperty(ConfigurationService.OOZIE_DATA_DIR, oozieHome.getAbsolutePath());
        System.setProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "*");

        LocalOozie.start();
        oozie = LocalOozie.getClient();

    }

    @Override
    public void stop() throws Exception {
        stop(false);

    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("Oozie: Stopping local server");

        mr.stop(cleanUp);
        hdfs.stop(cleanUp);
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        /*
        let hadoop find winutils.exe
          the binaries come from hdp windows distribution, from hadoop-2.6.0.2.2.6.0-2800.winpkg.zip
          also need to set hadoop-windows\lib as a dependency of the project in intelliJ
         */
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", new File("hadoop-windows").getAbsolutePath());
        }
        oozieHome.mkdirs();
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(oozieHome.getAbsolutePath());
    }

    public OozieClient getOozie() {
        return oozie;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(hdfs.getHdfsConfig());
    }

    private JobConf createDFSConfig() throws UnknownHostException {
        JobConf conf = new JobConf();
        conf.set("dfs.block.access.token.enable", "false");
        conf.set("dfs.permissions", "true");
        conf.set("hadoop.security.authentication", "simple");

        //Doing this because Hadoop 1.x does not support '*' and
        //Hadoop 0.23.x does not process wildcard if the value is
        // '*,127.0.0.1'
        StringBuilder sb = new StringBuilder();
        sb.append("127.0.0.1,localhost");
        for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
            sb.append(",").append(i.getCanonicalHostName());
        }
        conf.set("hadoop.proxyuser." + oozieUser + ".hosts", sb.toString());

        conf.set("hadoop.proxyuser." + oozieUser + ".groups", oozieGroup);
        conf.set("mapred.tasktracker.map.tasks.maximum", "4");
        conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

        conf.set("hadoop.tmp.dir", "target/test-data" + "/minicluster");

        // Scheduler properties required for YARN CapacityScheduler to work
        conf.set("yarn.scheduler.capacity.root.queues", "default");
        conf.set("yarn.scheduler.capacity.root.default.capacity", "100");
        // Required to prevent deadlocks with YARN CapacityScheduler
        conf.set("yarn.scheduler.capacity.maximum-am-resource-percent", "0.5");
        return conf;
    }

    public void dumpConfig() throws IOException {
        OutputStream os = new ByteArrayOutputStream();
        mr.getConfig().writeXml(os);
        LOG.info("mr config\n" + os.toString());
        hdfs.getHdfsConfig().writeXml(os);
        LOG.info("hdfs config\n" + os.toString());
    }

    public static class Builder {
        private String dir;

        public OozieLocalServer build() {
            validateObject();
            return new OozieLocalServer(this);
        }

        public Builder setHomeDir(String dir) {
            this.dir = dir;
            return this;
        }

        private void validateObject() {
            if (StringUtils.isBlank(dir)) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie home directory");
            }
        }
    }
}
