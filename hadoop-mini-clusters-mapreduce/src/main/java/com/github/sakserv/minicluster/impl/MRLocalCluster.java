/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.minicluster.impl;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;

public class MRLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MRLocalCluster.class);

    private String testName = getClass().getName();
    private String inJvmContainerExecutorClass = "com.github.sakserv.minicluster.yarn.InJvmContainerExecutor";
    private Integer numNodeManagers;
    private String jobHistoryAddress;
    private String resourceManagerAddress;
    private String resourceManagerHostname;
    private String resourceManagerSchedulerAddress;
    private String resourceManagerResourceTrackerAddress;
    private String resourceManagerWebappAddress;
    private Boolean useInJvmContainerExecutor;
    private String hdfsDefaultFs;
    private Configuration configuration;
    
    private MiniMRYarnCluster miniMRYarnCluster;

    public String getTestName() {
        return testName;
    }

    public Integer getNumNodeManagers() {
        return numNodeManagers;
    }

    public String getJobHistoryAddress() {
        return jobHistoryAddress;
    }

    public String getResourceManagerAddress() {
        return resourceManagerAddress;
    }

    public String getResourceManagerHostname() {
        return resourceManagerHostname;
    }

    public String getResourceManagerSchedulerAddress() {
        return resourceManagerSchedulerAddress;
    }

    public String getResourceManagerResourceTrackerAddress() {
        return resourceManagerResourceTrackerAddress;
    }

    public String getResourceManagerWebappAddress() {
        return resourceManagerWebappAddress;
    }

    public Boolean getUseInJvmContainerExecutor() {
        return useInJvmContainerExecutor;
    }

    public String getHdfsDefaultFs() {
        return hdfsDefaultFs;
    }

    public Configuration getConfig() {
        return configuration;
    }

    private MRLocalCluster(Builder builder) {
        this.numNodeManagers = builder.numNodeManagers;
        this.jobHistoryAddress = builder.jobHistoryAddress;
        this.resourceManagerAddress = builder.resourceManagerAddress;
        this.resourceManagerHostname = builder.resourceManagerHostname;
        this.resourceManagerSchedulerAddress = builder.resourceManagerSchedulerAddress;
        this.resourceManagerResourceTrackerAddress = builder.resourceManagerResourceTrackerAddress;
        this.resourceManagerWebappAddress = builder.resourceManagerWebappAddress;
        this.useInJvmContainerExecutor = builder.useInJvmContainerExecutor;
        this.hdfsDefaultFs = builder.hdfsDefaultFs;
        this.configuration = builder.configuration;
    }
    
    public static class Builder {
        private Integer numNodeManagers;
        private String jobHistoryAddress;
        private String resourceManagerAddress;
        private String resourceManagerHostname;
        private String resourceManagerSchedulerAddress;
        private String resourceManagerResourceTrackerAddress;
        private String resourceManagerWebappAddress;
        private Boolean useInJvmContainerExecutor;
        private String hdfsDefaultFs;
        private Configuration configuration;
        
        public Builder setNumNodeManagers(Integer numNodeManagers) {
            this.numNodeManagers = numNodeManagers;
            return this;
        }

        public Builder setJobHistoryAddress(String jobHistoryAddress) {
            this.jobHistoryAddress = jobHistoryAddress;
            return this;
        }
        
        public Builder setResourceManagerAddress(String resourceManagerAddress) {
            this.resourceManagerAddress = resourceManagerAddress;
            return this;
        }

        public Builder setResourceManagerHostname(String resourceManagerHostname) {
            this.resourceManagerHostname = resourceManagerHostname;
            return this;
        }

        public Builder setResourceManagerSchedulerAddress(String resourceManagerSchedulerAddress) {
            this.resourceManagerSchedulerAddress = resourceManagerSchedulerAddress;
            return this;
        }

        public Builder setResourceManagerResourceTrackerAddress(String resourceManagerResourceTrackerAddress) {
            this.resourceManagerResourceTrackerAddress = resourceManagerResourceTrackerAddress;
            return this;
        }

        public Builder setResourceManagerWebappAddress(String resourceManagerWebappAddress) {
            this.resourceManagerWebappAddress = resourceManagerWebappAddress;
            return this;
        }

        public Builder setUseInJvmContainerExecutor(Boolean useInJvmContainerExecutor) {
            this.useInJvmContainerExecutor = useInJvmContainerExecutor;
            return this;
        }

        public Builder setHdfsDefaultFs(String hdfsDefaultFs) {
            this.hdfsDefaultFs = hdfsDefaultFs;
            return this;
        }

        public Builder setConfig(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }
        
        public MRLocalCluster build() {
            MRLocalCluster mrLocalCluster = new MRLocalCluster(this);
            validateObject(mrLocalCluster);
            return mrLocalCluster;
        }

        public void validateObject(MRLocalCluster mrLocalCluster) {
            if (mrLocalCluster.getNumNodeManagers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Node Managers");
            }

            if(mrLocalCluster.getJobHistoryAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: MR Job History Address");
            }

            if(mrLocalCluster.getResourceManagerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Yarn Resource Manager Address");
            }
            
            if(mrLocalCluster.getResourceManagerHostname() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Yarn Resource Manager Hostname");
            }
            
            if(mrLocalCluster.getResourceManagerSchedulerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Scheduler Address");
            }
            
            if(mrLocalCluster.getResourceManagerResourceTrackerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Resource Tracker Address");
            }

            if(mrLocalCluster.getResourceManagerWebappAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Webapp Address");
            }

            if(mrLocalCluster.getUseInJvmContainerExecutor() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Use In JVM Container Executor");
            }

            if (mrLocalCluster.getConfig() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Configuration");
            }
        }
        
    }


    @Override
    public void start() throws Exception {
        LOG.info("MR: Starting MiniMRYarnCluster");
        configure();
        miniMRYarnCluster = new MiniMRYarnCluster(testName, numNodeManagers);
        miniMRYarnCluster.serviceInit(configuration);
        miniMRYarnCluster.init(configuration);
        miniMRYarnCluster.start();
    }

    @Override
    public void stop() throws Exception {stop(true);}

    @Override
    public void stop(boolean cleanUp) throws Exception {

        LOG.info("MR: Stopping MiniMRYarnCluster");
        miniMRYarnCluster.stop();

        if(cleanUp) {
            cleanUp();
        }

    }

    @Override
    public void configure() throws Exception {

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        configuration.set(YarnConfiguration.RM_ADDRESS, resourceManagerAddress);
        configuration.set(YarnConfiguration.RM_HOSTNAME, resourceManagerHostname);
        configuration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, resourceManagerSchedulerAddress);
        configuration.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, resourceManagerResourceTrackerAddress);
        configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, resourceManagerWebappAddress);
        configuration.set(JHAdminConfig.MR_HISTORY_ADDRESS, jobHistoryAddress);
        configuration.set(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, "true");
        if (getUseInJvmContainerExecutor()) {
            configuration.set(YarnConfiguration.NM_CONTAINER_EXECUTOR, inJvmContainerExecutorClass);
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }

        if (null != hdfsDefaultFs) {
            configuration.set("fs.defaultFS", hdfsDefaultFs);
        }
    }

    @Override
    public void cleanUp() throws Exception {
        // Depending on if we are running in the module or the parent
        // project, the target folder will be in a different location.
        // We don't want to nuke the entire target directory, unless only
        // the mini cluster is using it.
        // A reasonable check to keep things clean is to check for the existence
        // of ./target/classes and only delete the mini cluster temporary dir if true.
        // Delete the entire ./target if false
        if (new File("./target/classes").exists()) {
            FileUtils.deleteFolder("./target/" + testName);
        } else {
            FileUtils.deleteFolder("./target");
        }
    }
}
