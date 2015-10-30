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
    
    // Zookeeper
    public static final String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    public static final String ZOOKEEPER_HOST_KEY = "zookeeper.host";
    public static final String ZOOKEEPER_TEMP_DIR_KEY = "zookeeper.temp.dir";
    public static final String ZOOKEEPER_CONNECTION_STRING_KEY = "zookeeper.connection.string";
    
    // Kafka
    public static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    public static final String KAFKA_PORT_KEY = "kafka.port";
    
    // Kafka Test
    public static final String KAFKA_TEST_TOPIC_KEY = "kafka.test.topic";
    public static final String KAFKA_TEST_MESSAGE_COUNT_KEY = "kafka.test.message.count";
    public static final String KAFKA_TEST_BROKER_ID_KEY = "kafka.test.broker.id";
    public static final String KAFKA_TEST_TEMP_DIR_KEY = "kafka.test.temp.dir";

    public String toString() {
        return "ConfigVars";
    }
}
