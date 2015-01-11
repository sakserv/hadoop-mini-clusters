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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.log4j.Logger;

import java.io.File;
import java.security.Permission;

public class HiveLocalMetaStore implements MiniCluster {

    // Logger
    private static final Logger LOG = Logger.getLogger(HiveLocalMetaStore.class);

    private static final int DEFAULT_METASTORE_PORT = 20102;
    private static final String DEFAULT_DERBY_DB_PATH = "metastore_db";
    private static final String DEFAULT_HIVE_SCRATCH_DIR = "hive_scratch_dir";

    private static int msPort;
    private static String derbyDbPath;
    private static HiveConf hiveConf;
    private static SecurityManager securityManager;
    private Thread t;

    public class NoExitSecurityManager extends SecurityManager {

        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {

            super.checkExit(status);
        }
    }

    private static class StartHiveLocalMetaStore implements Runnable {

        @Override
        public void run() {
            try {
                HiveMetaStore.startMetaStore(msPort, new HadoopThriftAuthBridge(), HiveLocalMetaStore.getConf());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public HiveLocalMetaStore() {
        this(DEFAULT_METASTORE_PORT, DEFAULT_DERBY_DB_PATH);
    }

    public HiveLocalMetaStore(int msPort, String derbyDbPath) {
        this.msPort = msPort;
        this.derbyDbPath = derbyDbPath;
        configure(msPort, derbyDbPath);
    }

    public void configure() {
        hiveConf = new HiveConf();
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.set(HiveConf.ConfVars.SCRATCHDIR.varname, DEFAULT_HIVE_SCRATCH_DIR);
        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=" + DEFAULT_DERBY_DB_PATH + ";create=true");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new File("warehouse_dir").getAbsolutePath());
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
                "false");
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
    }

    public void configure(int msPort, String derbyDbPath) {
        configure();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
                + msPort);
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=" + derbyDbPath + ";create=true");
    }

    public void stop() {
        cleanDb();
        t.interrupt();
    }

    public void stop(boolean cleanUp) {
        stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(derbyDbPath);
        FileUtils.deleteFolder(new File("derby.log").getAbsolutePath());
    }

    public void start() {
        t = new Thread(new StartHiveLocalMetaStore());
        t.start();
        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        prepDb();
    }

    public void prepDb() {
        try {
            LOG.info("HIVE METASTORE: Prepping the database");
            TxnDbUtil.setConfValues(getConf());
            TxnDbUtil.prepDb();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanDb() {
        try {
            LOG.info("HIVE METASTORE: Cleaning up the database");
            TxnDbUtil.setConfValues(getConf());
            TxnDbUtil.cleanDb();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getMetaStoreUri() {
        return hiveConf.get("hive.metastore.uris");
    }

    public static HiveConf getConf() {
        return hiveConf;
    }

    public void dumpConfig() {
        LOG.info("HIVE METASTORE CONFIG: " + String.valueOf(hiveConf.getAllProperties()));
    }

}
