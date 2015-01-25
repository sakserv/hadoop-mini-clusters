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
    
    // ActiveMQ
    public static final String ACTIVEMQ_HOSTNAME_VAR = "activemq.hostname";
    public static final String ACTIVEMQ_PORT_VAR = "activemq.port";
    public static final String ACTIVEMQ_QUEUE_NAME_VAR = "activemq.queue";
    public static final String ACTIVEMQ_STORE_DIR_VAR = "activemq.store.dir";
    public static final String ACTIVEMQ_URI_PREFIX_VAR = "activemq.uri.prefix";
    public static final String ACTIVEMQ_URI_POSTFIX_VAR = "activemq.uri.postfix";
}
