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
    
    // HSQLDB
    public static final String HSQLDB_HOSTNAME_KEY = "hsqldb.hostname";
    public static final String HSQLDB_PORT_KEY = "hsqldb.port";
    public static final String HSQLDB_DATABASE_NAME_KEY = "hsqldb.database.name";
    public static final String HSQLDB_TEMP_DIR_KEY = "hsqldb.temp.dir";
    public static final String HSQLDB_COMPATIBILITY_MODE_KEY = "hsqldb.compatibility.mode";
    public static final String HSQLDB_JDBC_DRIVER_KEY = "hsqldb.jdbc.driver";
    public static final String HSQLDB_JDBC_CONNECTION_STRING_PREFIX_KEY = "hsqldb.jdbc.connection.string.prefix";

    public String toString() {
        return "ConfigVars";
    }
}
