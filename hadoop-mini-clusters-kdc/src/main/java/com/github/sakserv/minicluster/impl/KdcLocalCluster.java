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
import com.google.common.base.Throwables;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class KdcLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalCluster.class);

    public static final String HDFS_USER_NAME = "hdfs";
    public static final String HBASE_USER_NAME = "hbase";
    public static final String YARN_USER_NAME = "yarn";
    public static final String MRV2_USER_NAME = "mapreduce";
    public static final String ZOOKEEPER_USER_NAME = "zookeeper";
    public static final String STORM_USER_NAME = "storm";
    public static final String OOZIE_USER_NAME = "oozie";
    public static final String OOZIE_PROXIED_USER_NAME = "oozie_user";
    public static final String SPNEGO_USER_NAME = "HTTP";

    public static List<String> DEFAULT_PRINCIPALS = Collections.unmodifiableList(Arrays.asList(
            HDFS_USER_NAME, HBASE_USER_NAME, YARN_USER_NAME, MRV2_USER_NAME, ZOOKEEPER_USER_NAME, STORM_USER_NAME, OOZIE_USER_NAME, OOZIE_PROXIED_USER_NAME, SPNEGO_USER_NAME
    ));

    private MiniKdc miniKdc;

    private final String orgName;
    private final String orgDomain;
    private final Integer port;
    private final String host;
    private final String baseDir;
    private final String krbInstance;
    private List<String> principals;
    private final String instance;
    private final String transport;
    private final Integer maxTicketLifetime;
    private final Integer maxRenewableLifetime;
    private final Boolean debug;

    private Configuration baseConf;

    private Properties conf;

    public String getOrgName() {
        return orgName;
    }

    public Integer getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public String getKrbInstance() {
        return krbInstance;
    }

    public String getOrgDomain() {
        return orgDomain;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public File getKrb5conf() {
        return miniKdc.getKrb5conf();
    }

    public List<String> getPrincipals() {
        return principals;
    }

    public String getInstance() {
        return instance;
    }

    public String getTransport() {
        return transport;
    }

    public Integer getMaxTicketLifetime() {
        return maxTicketLifetime;
    }

    public Integer getMaxRenewableLifetime() {
        return maxRenewableLifetime;
    }

    public Boolean getDebug() {
        return debug;
    }

    public Configuration getBaseConf() {
        return baseConf;
    }

    private KdcLocalCluster(Builder builder) {
        this.orgName = builder.orgName;
        this.orgDomain = builder.orgDomain;
        this.port = builder.port;
        this.host = builder.host;
        this.baseDir = builder.baseDir;
        this.krbInstance = builder.krbInstance;
        this.principals = builder.principals;
        this.instance = builder.instance;
        this.transport = builder.transport;
        this.maxTicketLifetime = builder.maxTicketLifetime;
        this.maxRenewableLifetime = builder.maxRenewableLifetime;
        this.debug = builder.debug;
    }

    public static class Builder {
        private String orgName = "acme";
        private String orgDomain = "org";
        private Integer port;
        private String host;
        private String baseDir;
        private String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
        private List<String> principals = DEFAULT_PRINCIPALS;
        private String instance = "DefaultKrbServer";
        private String transport = "TCP";
        private Integer maxTicketLifetime = 86400000;
        private Integer maxRenewableLifetime = 604800000;
        private Boolean debug = false;

        public Builder setOrgName(String orgName) {
            this.orgName = orgName;
            return this;
        }

        public Builder setOrgDomain(String orgDomain) {
            this.orgDomain = orgDomain;
            return this;
        }

        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setBaseDir(String baseDir) {
            this.baseDir = baseDir;
            return this;
        }

        public Builder setKrbInstance(String krbInstance) {
            this.krbInstance = krbInstance;
            return this;
        }

        public Builder setPrincipals(List<String> principals) {
            this.principals = principals;
            return this;
        }

        public Builder setPrincipals(String[] principals) {
            this.principals = Arrays.asList(principals);
            return this;
        }

        public Builder setInstance(String instance) {
            this.instance = instance;
            return this;
        }

        public Builder setTransport(String transport) {
            this.transport = transport;
            return this;
        }

        public Builder setMaxTicketLifetime(Integer maxTicketLifetime) {
            this.maxTicketLifetime = maxTicketLifetime;
            return this;
        }

        public Builder setMaxRenewableLifetime(Integer maxRenewableLifetime) {
            this.maxRenewableLifetime = maxRenewableLifetime;
            return this;
        }

        public Builder setDebug(Boolean debug) {
            this.debug = debug;
            return this;
        }

        public KdcLocalCluster build() {
            KdcLocalCluster kdcLocalCluster = new KdcLocalCluster(this);
            validateObject(kdcLocalCluster);
            return kdcLocalCluster;
        }

        public void validateObject(KdcLocalCluster kdcLocalCluster) {
            if (kdcLocalCluster.orgName == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Name");
            }
            if (kdcLocalCluster.orgDomain == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Domain");
            }
            if (kdcLocalCluster.host == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Host");
            }
            if (kdcLocalCluster.baseDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC BaseDir");
            }
            if (kdcLocalCluster.krbInstance == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC KrbInstance");
            }
            if (CollectionUtils.isEmpty(kdcLocalCluster.principals)) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Principals");
            }
            if (kdcLocalCluster.instance == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Instance");
            }
            if (kdcLocalCluster.instance == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Instance");
            }
            if (kdcLocalCluster.transport == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Tranport");
            }
            if (kdcLocalCluster.maxTicketLifetime == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC MaxTicketLifetime");
            }
            if (kdcLocalCluster.maxRenewableLifetime == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC MaxRenewableLifetime");
            }
            if (kdcLocalCluster.debug == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: KDC Debug");
            }
        }
    }

    public String getKrbPrincipalWithRealm(String principal) {
        return principal + "/" + krbInstance + "@" + miniKdc
                .getRealm();
    }

    public String getRealm() {
        return miniKdc.getRealm();
    }

    public String getKrbPrincipal(String principal) {
        return principal + "/" + krbInstance;
    }

    public String getKeytabForPrincipal(String principal) {
        return new File(baseDir, principal + ".keytab").getAbsolutePath();
    }

    @Override
    public void start() throws Exception {

        LOG.info("KDC: Starting MiniKdc");
        configure();
        miniKdc = new MiniKdc(conf, new File(baseDir));
        miniKdc.start();

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("guest");
        UserGroupInformation.setLoginUser(ugi);
        String username = UserGroupInformation.getLoginUser().getShortUserName();

        List<String> temp = new ArrayList<>(principals);
        temp.add(username);
        this.principals = Collections.unmodifiableList(temp);

        principals.forEach(p -> {
            try {
                File keytab = new File(baseDir, p + ".keytab");
                LOG.info("KDC: Creating keytab for {} in {}", p, keytab);
                miniKdc.createPrincipal(keytab, p, getKrbPrincipal(p), getKrbPrincipalWithRealm(p));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
        refreshDefaultRealm();
        prepareSecureConfiguration(username);
    }

    protected void refreshDefaultRealm() throws Exception {
        // Config is statically initialized at this point. But the above configuration results in a different
        // initialization which causes the tests to fail. So the following two changes are required.

        // (1) Refresh Kerberos config.
        // refresh the config
        Class<?> configClass;
        if (System.getProperty("java.vendor").contains("IBM")) {
            configClass = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            configClass = Class.forName("sun.security.krb5.Config");
        }
        Method refreshMethod = configClass.getMethod("refresh", new Class[0]);
        refreshMethod.invoke(configClass, new Object[0]);
        // (2) Reset the default realm.
        try {
            Class<?> hadoopAuthClass = Class.forName("org.apache.hadoop.security.authentication.util.KerberosName");
            Field defaultRealm = hadoopAuthClass.getDeclaredField("defaultRealm");
            defaultRealm.setAccessible(true);
            defaultRealm.set(null, KerberosUtil.getDefaultRealm());
            LOG.info("HADOOP: Using default realm " + KerberosUtil.getDefaultRealm());
        } catch (ClassNotFoundException e) {
            // Don't care
            LOG.info("Class org.apache.hadoop.security.authentication.util.KerberosName not found, Kerberos default realm not updated");
        }

        try {
            Class<?> zookeeperAuthClass = Class.forName("org.apache.zookeeper.server.auth.KerberosName");
            Field defaultRealm = zookeeperAuthClass.getDeclaredField("defaultRealm");
            defaultRealm.setAccessible(true);
            defaultRealm.set(null, KerberosUtil.getDefaultRealm());
            LOG.info("ZOOKEEPER: Using default realm " + KerberosUtil.getDefaultRealm());
        } catch (ClassNotFoundException e) {
            // Don't care
            LOG.info("Class org.apache.zookeeper.server.auth.KerberosName not found, Kerberos default realm not updated");
        }
    }

    protected void prepareSecureConfiguration(String username) throws Exception {
        baseConf = new Configuration(false);
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, baseConf);
        baseConf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
        //baseConf.set(CommonConfigurationKeys.HADOOP_RPC_PROTECTION, "authentication");

        String sslConfigDir = KeyStoreTestUtil.getClasspathDir(this.getClass());
        KeyStoreTestUtil.setupSSLConfig(baseDir, sslConfigDir, baseConf, false);

        // User
        baseConf.set("hadoop.proxyuser." + username + ".hosts", "*");
        baseConf.set("hadoop.proxyuser." + username + ".groups", "*");

        // HTTP
        String spnegoPrincipal = getKrbPrincipalWithRealm(SPNEGO_USER_NAME);
        baseConf.set("hadoop.proxyuser." + SPNEGO_USER_NAME + ".groups", "*");
        baseConf.set("hadoop.proxyuser." + SPNEGO_USER_NAME + ".hosts", "*");

        // Oozie
        String ooziePrincipal = getKrbPrincipalWithRealm(OOZIE_USER_NAME);
        baseConf.set("hadoop.proxyuser." + OOZIE_USER_NAME + ".hosts", "*");
        baseConf.set("hadoop.proxyuser." + OOZIE_USER_NAME + ".groups", "*");
        baseConf.set("hadoop.user.group.static.mapping.overrides", OOZIE_PROXIED_USER_NAME + "=oozie");
        baseConf.set("oozie.service.HadoopAccessorService.keytab.file", getKeytabForPrincipal(OOZIE_USER_NAME));
        baseConf.set("oozie.service.HadoopAccessorService.kerberos.principal", ooziePrincipal);
        baseConf.setBoolean("oozie.service.HadoopAccessorService.kerberos.enabled", true);

        // HDFS
        String hdfsPrincipal = getKrbPrincipalWithRealm(HDFS_USER_NAME);
        baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
        baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, getKeytabForPrincipal(HDFS_USER_NAME));
        baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
        baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, getKeytabForPrincipal(HDFS_USER_NAME));
        baseConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
        baseConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, getKeytabForPrincipal(SPNEGO_USER_NAME));
        baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
        baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
        baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
        baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
        baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
        baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
        baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

        // HBase
        String hbasePrincipal = getKrbPrincipalWithRealm(HBASE_USER_NAME);
        baseConf.set("hbase.security.authentication", "kerberos");
        baseConf.setBoolean("hbase.security.authorization", true);
        baseConf.set("hbase.regionserver.kerberos.principal", hbasePrincipal);
        baseConf.set("hbase.regionserver.keytab.file", getKeytabForPrincipal(HBASE_USER_NAME));
        baseConf.set("hbase.master.kerberos.principal", hbasePrincipal);
        baseConf.set("hbase.master.keytab.file", getKeytabForPrincipal(HBASE_USER_NAME));
        baseConf.set("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.token.TokenProvider");
        baseConf.set("hbase.rest.authentication.kerberos.keytab", getKeytabForPrincipal(SPNEGO_USER_NAME));
        baseConf.set("hbase.rest.authentication.kerberos.principal", spnegoPrincipal);
        baseConf.set("hbase.rest.kerberos.principal", hbasePrincipal);
        baseConf.set("hadoop.proxyuser." + HBASE_USER_NAME + ".groups", "*");
        baseConf.set("hadoop.proxyuser." + HBASE_USER_NAME + ".hosts", "*");

        //hbase.coprocessor.master.classes -> org.apache.hadoop.hbase.security.access.AccessController
        //hbase.coprocessor.region.classes -> org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.security.access.AccessController

        // Storm
        //String stormPrincipal = getKrbPrincipalWithRealm(STORM_USER_NAME);

        // Yarn
        String yarnPrincipal = getKrbPrincipalWithRealm(YARN_USER_NAME);
        baseConf.set("yarn.resourcemanager.keytab", getKeytabForPrincipal(YARN_USER_NAME));
        baseConf.set("yarn.resourcemanager.principal", yarnPrincipal);
        baseConf.set("yarn.nodemanager.keytab", getKeytabForPrincipal(YARN_USER_NAME));
        baseConf.set("yarn.nodemanager.principal", yarnPrincipal);

        // Mapreduce
        String mrv2Principal = getKrbPrincipalWithRealm(MRV2_USER_NAME);
        baseConf.set("mapreduce.jobhistory.keytab", getKeytabForPrincipal(MRV2_USER_NAME));
        baseConf.set("mapreduce.jobhistory.principal", mrv2Principal);
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("KDC: Stopping MiniKdc");
        miniKdc.stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
        conf = new Properties();
        conf.setProperty("kdc.port", Integer.toString(getPort()));
        conf.setProperty("kdc.bind.address", getHost());
        conf.setProperty("org.name", getOrgName());
        conf.setProperty("org.domain", getOrgDomain());
        conf.setProperty("instance", getInstance());
        conf.setProperty("transport", getTransport());
        conf.setProperty("max.ticket.lifetime", Integer.toString(getMaxTicketLifetime()));
        conf.setProperty("max.renewable.lifetime", Integer.toString(getMaxRenewableLifetime()));
        conf.setProperty("debug", Boolean.toString(getDebug()));

        // Handle Windows
        WindowsLibsUtils.setHadoopHome();
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(baseDir, true);
    }

    public MiniKdc getMiniKdc() throws Exception {
        return this.miniKdc;
    }
}
