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
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HsqldbLocalServer implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HsqldbLocalServer.class);
    
    private HsqlProperties hsqlProperties = new HsqlProperties();
    private Server server;
    
    private String hsqldbHostName;
    private String hsqldbPort;
    private String hsqldbTempDir;
    private String hsqldbDatabaseName;
    private String hsqldbCompatibilityMode;
    private String hsqldbJdbcDriver;
    private String hsqldbJdbcConnectionStringPrefix;

    public String getHsqldbHostName() {
        return hsqldbHostName;
    }

    public String getHsqldbPort() {
        return hsqldbPort;
    }

    public String getHsqldbTempDir() {
        return hsqldbTempDir;
    }

    public String getHsqldbDatabaseName() {
        return hsqldbDatabaseName;
    }

    public String getHsqldbCompatibilityMode() {
        return hsqldbCompatibilityMode;
    }

    public String getHsqldbJdbcDriver() {
        return hsqldbJdbcDriver;
    }

    public String getHsqldbJdbcConnectionStringPrefix() {
        return hsqldbJdbcConnectionStringPrefix;
    }

    private HsqldbLocalServer(Builder builder) {
        this.hsqldbHostName = builder.hsqldbHostName;
        this.hsqldbPort = builder.hsqldbPort;
        this.hsqldbTempDir = builder.hsqldbTempDir;
        this.hsqldbDatabaseName = builder.hsqldbDatabaseName;
        this.hsqldbCompatibilityMode = builder.hsqldbCompatibilityMode;
        this.hsqldbJdbcDriver = builder.hsqldbJdbcDriver;
        this.hsqldbJdbcConnectionStringPrefix = builder.hsqldbJdbcConnectionStringPrefix;
    }
    
    public static class Builder {
        private String hsqldbHostName;
        private String hsqldbPort;
        private String hsqldbTempDir;
        private String hsqldbDatabaseName;
        private String hsqldbCompatibilityMode;
        private String hsqldbJdbcDriver;
        private String hsqldbJdbcConnectionStringPrefix;
        
        public Builder setHsqldbHostName(String hsqldbHostName) {
            this.hsqldbHostName = hsqldbHostName;
            return this;
        }
        
        public Builder setHsqldbPort(String hsqldbPort) {
            this.hsqldbPort = hsqldbPort;
            return this;
        }
        
        public Builder setHsqldbTempDir(String hsqldbTempDir) {
            this.hsqldbTempDir = hsqldbTempDir;
            return this;
        }
        
        public Builder setHsqldbDatabaseName(String hsqldbDatabaseName) {
            this.hsqldbDatabaseName = hsqldbDatabaseName;
            return this;
        }
        
        public Builder setHsqldbCompatibilityMode(String hsqldbCompatibilityMode) {
            this.hsqldbCompatibilityMode = hsqldbCompatibilityMode;
            return this;
        }
        
        public Builder setHsqldbJdbcDriver(String hsqldbJdbcDriver) {
            this.hsqldbJdbcDriver = hsqldbJdbcDriver;
            return this;
        }

        public Builder setHsqldbJdbcConnectionStringPrefix(String hsqldbJdbcConnectionStringPrefix) {
            this.hsqldbJdbcConnectionStringPrefix = hsqldbJdbcConnectionStringPrefix;
            return this;
        }

        public HsqldbLocalServer build() {
            HsqldbLocalServer hsqldbLocalServer = new HsqldbLocalServer(this);
            validateObject(hsqldbLocalServer);
            return hsqldbLocalServer;
        }
        
        public void validateObject(HsqldbLocalServer hsqldbLocalServer) {
            if(hsqldbLocalServer.hsqldbHostName == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB Host Name");
            }
            if(hsqldbLocalServer.hsqldbPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB Port");
            }
            if(hsqldbLocalServer.hsqldbTempDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB Temp Dir");
            }
            if(hsqldbLocalServer.hsqldbDatabaseName == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB Database Name");
            }
            if(hsqldbLocalServer.hsqldbCompatibilityMode == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB Compatibility Mode");
            }
            if(hsqldbLocalServer.hsqldbJdbcDriver == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HSQLDB JDBC Driver");
            }
            if(hsqldbLocalServer.hsqldbJdbcConnectionStringPrefix == null) {
                throw new IllegalArgumentException(
                        "ERROR: Missing required config: HSQLDB JDBC Connection String Prefix");
            }
        }
        
    }

    @Override
    public void start() throws Exception {
        LOG.info("HSQLDB: Starting HSQLDB");
        configure();
        server = new Server();
        server.setProperties(hsqlProperties);
        server.start();
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("HSQLDB: Stopping HSQLDB");
        server.stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        hsqlProperties.setProperty("server.address", getHsqldbHostName());
        hsqlProperties.setProperty("server.port", getHsqldbPort());
        hsqlProperties.setProperty("server.database.0", "file:" + new File(getHsqldbTempDir()).getAbsolutePath());
        hsqlProperties.setProperty("server.dbname.0", getHsqldbDatabaseName());
        hsqlProperties.setProperty("server.remote_open", "true");
        hsqlProperties.setProperty("server.max_allowed_packet", "32M");
    }

    @Override
    public void cleanUp() throws Exception {
        
        FileUtils.deleteFolder(getHsqldbTempDir() + ".tmp");
        FileUtils.deleteFolder(getHsqldbTempDir() + ".log");
        FileUtils.deleteFolder(getHsqldbTempDir() + ".properties");
        FileUtils.deleteFolder(getHsqldbTempDir() + ".script");
        FileUtils.deleteFolder(getHsqldbTempDir() + ".lck");
    }

    public String getHsqldbCompatibilityModeStatement() {
        String dbTypeString = "MYS"; // default to mysql if called

        if(getHsqldbCompatibilityMode().equals("postresql")) {
            dbTypeString = "PGS";
        } else if(getHsqldbCompatibilityMode().equals("mysql")) {
            dbTypeString = "MYS";
        } else if(getHsqldbCompatibilityMode().equals("oracle")) {
            dbTypeString = "ORA";
        } else if(getHsqldbCompatibilityMode().equals("db2")) {
            dbTypeString = "DB2";
        } else if(getHsqldbCompatibilityMode().equals("mssql")) {
            dbTypeString = "MSS";
        }

        return "SET DATABASE SQL SYNTAX " + dbTypeString + " TRUE";

    }
    
}
