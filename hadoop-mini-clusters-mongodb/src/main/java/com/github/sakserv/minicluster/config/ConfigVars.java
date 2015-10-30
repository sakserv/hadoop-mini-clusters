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

    // MongoDB
    public static final String MONGO_IP_KEY = "mongo.ip";
    public static final String MONGO_PORT_KEY = "mongo.port";
    public static final String MONGO_DATABASE_NAME_KEY = "mongo.database.name";
    public static final String MONGO_COLLECTION_NAME_KEY = "mongo.collection.name";

    public String toString() {
        return "ConfigVars";
    }
}
