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
package com.github.sakserv.minicluster.config;

public class ConfigVars {
    
    // Props file
    public static final String DEFAULT_PROPS_FILE = "default.properties";
    
    //HDFS
    public static final String HDFS_NAMENODE_PORT_KEY = "hdfs.namenode.port";
    public static final String HDFS_TEMP_DIR_KEY = "hdfs.temp.dir";
    public static final String HDFS_NUM_DATANODES_KEY = "hdfs.num.datanodes";
    public static final String HDFS_ENABLE_PERMISSIONS_KEY = "hdfs.enable.permissions";
    public static final String HDFS_FORMAT_KEY = "hdfs.format";
    public static final String HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER = "hdfs.enable.running.user.as.proxy.user";
    
    // YARN
    public static final String YARN_NUM_NODE_MANAGERS_KEY = "yarn.num.node.managers";
    public static final String YARN_NUM_LOCAL_DIRS_KEY = "yarn.num.local.dirs";
    public static final String YARN_NUM_LOG_DIRS_KEY = "yarn.num.log.dirs";
    public static final String YARN_RESOURCE_MANAGER_ADDRESS_KEY = "yarn.resource.manager.address";
    public static final String YARN_RESOURCE_MANAGER_HOSTNAME_KEY = "yarn.resource.manager.hostname";
    public static final String YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY = "yarn.resource.manager.scheduler.address";
    public static final String YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY = "yarn.resource.manager.webapp.address";
    public static final String YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY = 
            "yarn.resource.manager.resource.tracker.address";
    public static final String YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY = "yarn.use.in.jvm.container.executor";
    
    // MR
    public static final String MR_JOB_HISTORY_ADDRESS_KEY = "mr.job.history.address";

    //Oozie
    public static final String OOZIE_TEST_DIR_KEY = "oozie.test.dir";
    public static final String OOZIE_HOME_DIR_KEY = "oozie.home.dir";
    public static final String OOZIE_USERNAME_KEY = "oozie.username";
    public static final String OOZIE_GROUPNAME_KEY = "oozie.groupname";

    public String toString() {
        return "ConfigVars";
    }
}
