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

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class YarnLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(YarnLocalCluster.class);

    private String testName = getClass().getName();
    private Integer numResourceManagers = 1;
    private String inJvmContainerExecutorClass = "com.github.sakserv.minicluster.yarn.InJvmContainerExecutor";
    private Boolean enableHa = false;
    private Integer numNodeManagers;
    private Integer numLocalDirs;
    private Integer numLogDirs;
    private String resourceManagerAddress;
    private String resourceManagerHostname;
    private String resourceManagerSchedulerAddress;
    private String resourceManagerResourceTrackerAddress;
    private String resourceManagerWebappAddress;
    private Boolean useInJvmContainerExecutor;
    private Configuration configuration;
    
    private MiniYARNCluster miniYARNCluster;
    

    public String getTestName() {
        return testName;
    }

    public Integer getNumNodeManagers() {
        return numNodeManagers;
    }

    public Integer getNumLocalDirs() {
        return numLocalDirs;
    }

    public Integer getNumLogDirs() {
        return numLogDirs;
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

    public Configuration getConfig() {
        return configuration;
    }

    private YarnLocalCluster(Builder builder) {
        this.numNodeManagers = builder.numNodeManagers;
        this.numLocalDirs = builder.numLocalDirs;
        this.numLogDirs = builder.numLogDirs;
        this.resourceManagerAddress = builder.resourceManagerAddress;
        this.resourceManagerHostname = builder.resourceManagerHostname;
        this.resourceManagerSchedulerAddress = builder.resourceManagerSchedulerAddress;
        this.resourceManagerResourceTrackerAddress = builder.resourceManagerResourceTrackerAddress;
        this.resourceManagerWebappAddress = builder.resourceManagerWebappAddress;
        this.useInJvmContainerExecutor = builder.useInJvmContainerExecutor;
        this.configuration = builder.configuration;
    }
    
    public static class Builder {
        private Integer numNodeManagers;
        private Integer numLocalDirs;
        private Integer numLogDirs;
        private String resourceManagerAddress;
        private String resourceManagerHostname;
        private String resourceManagerSchedulerAddress;
        private String resourceManagerResourceTrackerAddress;
        private String resourceManagerWebappAddress;
        private Boolean useInJvmContainerExecutor;
        private Configuration configuration;
        
        public Builder setNumNodeManagers(Integer numNodeManagers) {
            this.numNodeManagers = numNodeManagers;
            return this;
        }
        
        public Builder setNumLocalDirs(Integer numLocalDirs) {
            this.numLocalDirs = numLocalDirs;
            return this;
        }
        
        public Builder setNumLogDirs(Integer numLogDirs) {
            this.numLogDirs = numLogDirs;
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
        
        public Builder setConfig(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }
        
        public YarnLocalCluster build() {
            YarnLocalCluster yarnLocalCluster = new YarnLocalCluster(this);
            validateObject(yarnLocalCluster);
            return yarnLocalCluster;
        }
        
        public void validateObject(YarnLocalCluster yarnLocalCluster) {
            if (yarnLocalCluster.getNumNodeManagers() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Node Managers");
            }
            
            if (yarnLocalCluster.getNumLocalDirs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Local Dirs");
            }
            
            if (yarnLocalCluster.getNumLogDirs() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Number of Log Dirs");
            }
            
            if(yarnLocalCluster.getResourceManagerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Yarn Resource Manager Address");
            }
            
            if(yarnLocalCluster.getResourceManagerHostname() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Yarn Resource Manager Hostname");
            }
            
            if(yarnLocalCluster.getResourceManagerSchedulerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Scheduler Address");
            }
            
            if(yarnLocalCluster.getResourceManagerResourceTrackerAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Resource Tracker Address");
            }

            if(yarnLocalCluster.getResourceManagerWebappAddress() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: " +
                        "Yarn Resource Manager Webapp Address");
            }


            if(yarnLocalCluster.getUseInJvmContainerExecutor() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Use In JVM Container Executor");
            }
            
            if (yarnLocalCluster.getConfig() == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: Configuration");
            }
            
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("YARN: Starting MiniYarnCluster");
        configure();
        miniYARNCluster = new MiniYARNCluster(testName, numResourceManagers, numNodeManagers,
                numLocalDirs, numLogDirs, enableHa);

        miniYARNCluster.serviceInit(configuration);
        miniYARNCluster.init(configuration);
        miniYARNCluster.start();
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("YARN: Stopping MiniYarnCluster");
        miniYARNCluster.stop();
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
        configuration.set(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, "true");
        if (getUseInJvmContainerExecutor()) {
            configuration.set(YarnConfiguration.NM_CONTAINER_EXECUTOR, inJvmContainerExecutorClass);
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
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
