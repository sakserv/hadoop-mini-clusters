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

    // Hive
    public static final String HIVE_SCRATCH_DIR_KEY = "hive.scratch.dir";
    public static final String HIVE_WAREHOUSE_DIR_KEY = "hive.warehouse.dir";
    
    // Hive Metastore
    public static final String HIVE_METASTORE_HOSTNAME_KEY = "hive.metastore.hostname";
    public static final String HIVE_METASTORE_PORT_KEY = "hive.metastore.port";
    public static final String HIVE_METASTORE_DERBY_DB_DIR_KEY = "hive.metastore.derby.db.dir";
    
    // Hive Test
    public static final String HIVE_TEST_DATABASE_NAME_KEY = "hive.test.database.name";
    public static final String HIVE_TEST_TABLE_NAME_KEY = "hive.test.table.name";

    public String toString() {
        return "ConfigVars";
    }
}
