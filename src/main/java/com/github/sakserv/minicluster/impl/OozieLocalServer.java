package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.oozie.util.OozieConfigHelpers;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * Create an in-memory Oozie server
 *
 * work is largely based on org.apache.oozie.test.XTestCase,
 * but does not require a checkout of Oozie source code to function
 */
public class OozieLocalServer implements MiniCluster {
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieLocalServer.class);

    // Need to build out the proper directory structure
    // for Oozie. These variables are needed.
    private String fullOozieHomeDir;
    private String fullOozieConfDir;
    private String fullOozieHadoopConfDir;
    private String fullOozieActionDir;

    private String oozieTestDir;
    private String oozieHomeDir;
    private String oozieUsername;
    private String oozieGroupname;
    private String oozieYarnResourceManagerAddress;
    private String oozieHdfsDefaultFs;
    private Configuration oozieConf;

    private OozieClient oozieClient;

    private OozieLocalServer(Builder builder) {
        this.oozieTestDir = builder.oozieTestDir;
        this.oozieHomeDir = builder.oozieHomeDir;
        this.oozieUsername = builder.oozieUsername;
        this.oozieGroupname = builder.oozieGroupname;
        this.oozieYarnResourceManagerAddress = builder.oozieYarnResourceManagerAddress;
        this.oozieHdfsDefaultFs = builder.oozieHdfsDefaultFs;
        this.oozieConf = builder.oozieConf;
    }

    public String getOozieTestDir() {
        return oozieTestDir;
    }

    public String getOozieHomeDir() {
        return oozieHomeDir;
    }

        public String getOozieUsername() {
        return oozieUsername;
    }

    public String getOozieGroupname() {
        return oozieGroupname;
    }

    public String getOozieYarnResourceManagerAddress() {
        return oozieYarnResourceManagerAddress;
    }

    public String getOozieHdfsDefaultFs() {
        return oozieHdfsDefaultFs;
    }

    public Configuration getOozieConf() {
        return oozieConf;
    }

    public static class Builder {
        private String oozieTestDir;
        private String oozieHomeDir;
        private String oozieUsername;
        private String oozieGroupname;
        private String oozieYarnResourceManagerAddress;
        private String oozieHdfsDefaultFs;
        private Configuration oozieConf;

        public Builder setOozieTestDir(String oozieTestDir) {
            this.oozieTestDir = oozieTestDir;
            return this;
        }

        public Builder setOozieHomeDir(String oozieHomeDir) {
            this.oozieHomeDir = oozieHomeDir;
            return this;
        }

        public Builder setOozieUsername(String oozieUsername) {
            this.oozieUsername = oozieUsername;
            return this;
        }

        public Builder setOozieGroupname(String oozieGroupname) {
            this.oozieGroupname = oozieGroupname;
            return this;
        }

        public Builder setOozieYarnResourceManagerAddress(String oozieYarnResourceManagerAddress) {
            this.oozieYarnResourceManagerAddress = oozieYarnResourceManagerAddress;
            return this;
        }

        public Builder setOozieHdfsDefaultFs(String oozieHdfsDefaultFs) {
            this.oozieHdfsDefaultFs = oozieHdfsDefaultFs;
            return this;
        }

        public Builder setOozieConf(Configuration oozieConf) {
            this.oozieConf = oozieConf;
            return this;
        }

        public OozieLocalServer build() {
            OozieLocalServer oozieLocalServer = new OozieLocalServer(this);
            validateObject(oozieLocalServer);
            return oozieLocalServer;
        }

        private void validateObject(OozieLocalServer oozieLocalServer) {

            if (oozieLocalServer.getOozieTestDir() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie test directory");
            }

            if (oozieLocalServer.getOozieHomeDir() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie home directory");
            }

            if (oozieLocalServer.getOozieUsername() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie user name");
            }

            if (oozieLocalServer.getOozieGroupname() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie group name");
            }

            if (oozieLocalServer.getOozieYarnResourceManagerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie YARN Resource Manager address");
            }

            if (oozieLocalServer.getOozieHdfsDefaultFs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie HDFS Default FS");
            }

            if (oozieLocalServer.getOozieConf() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Oozie Configuration");
            }
        }
    }

    @Override
    public void start() throws Exception {

        configure();

        // Create the directories
        new File(fullOozieHomeDir).mkdirs();
        new File(fullOozieHadoopConfDir).mkdirs();
        new File(fullOozieActionDir).mkdirs();

        // Create the configs
        OozieConfigHelpers.writeXml(OozieConfigHelpers.getOozieSite(new Configuration()),
                fullOozieConfDir + "/oozie-site.xml");

        // Note: Oozie requires the Hadoop config be stored in a directory "hadoop-conf", handle that here.
        OozieConfigHelpers.writeXml(new Configuration(), fullOozieHadoopConfDir + "/core-site.xml");

        //setup users
        UserGroupInformation.createUserForTesting(oozieUsername, new String[]{oozieGroupname});

        LocalOozie.start();
        oozieClient = LocalOozie.getClient();

    }

    @Override
    public void stop() throws Exception {
        stop(true);

    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("OOZIE: Stopping local server");

        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(oozieTestDir);
        FileUtils.deleteFolder(new File("derby.log").getAbsolutePath());
    }

    @Override
    public void configure() throws Exception {

        // Oozie has very particular naming conventions for these directories, don't change
        fullOozieHomeDir = oozieTestDir + "/" + oozieHomeDir;
        fullOozieConfDir = fullOozieHomeDir + "/conf";
        fullOozieHadoopConfDir = fullOozieConfDir + "/hadoop-conf";
        fullOozieActionDir = fullOozieConfDir + "/action-conf";

        //set system properties
        System.setProperty(Services.OOZIE_HOME_DIR, new File(fullOozieHomeDir).getAbsolutePath());
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, fullOozieConfDir);
        System.setProperty("oozielocal.log", fullOozieHomeDir + "/oozielocal.log");
        System.setProperty(XTestCase.OOZIE_TEST_JOB_TRACKER, oozieYarnResourceManagerAddress);
        System.setProperty(XTestCase.OOZIE_TEST_NAME_NODE, oozieHdfsDefaultFs);
        System.setProperty("oozie.test.db.host", "localhost");
        System.setProperty(ConfigurationService.OOZIE_DATA_DIR, fullOozieHomeDir);
        System.setProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "*");

        // Handle defining hadoop home dir to include the Windows libs
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", new File("hadoop-windows").getAbsolutePath());
        }
    }

    public OozieClient getOozieClient() {
        return oozieClient;
    }
}
