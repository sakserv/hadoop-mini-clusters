package com.github.sakserv.minicluster.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.gateway.GatewayMessages;
import org.apache.hadoop.gateway.config.GatewayConfig;
import org.apache.hadoop.gateway.i18n.messages.MessagesFactory;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.gateway.config.impl.GatewayConfigImpl.*;

/**
 * @author Vincent Devillers
 */
public class LocalGatewayConfig extends Configuration implements GatewayConfig {
    private static final String GATEWAY_DEFAULT_TOPOLOGY_NAME_PARAM = "default.app.topology.name";
    private static final String GATEWAY_DEFAULT_TOPOLOGY_NAME = null;
    private static GatewayMessages log = (GatewayMessages) MessagesFactory.get(GatewayMessages.class);
    private static final String GATEWAY_CONFIG_DIR_PREFIX = "conf";
    private static final String GATEWAY_CONFIG_FILE_PREFIX = "gateway";
    private static final String DEFAULT_STACKS_SERVICES_DIR = "services";
    private static final String DEFAULT_APPLICATIONS_DIR = "applications";
    public static final String[] GATEWAY_CONFIG_FILENAMES = new String[]{"conf/gateway-default.xml", "conf/gateway-site.xml"};
    public static final String HTTP_HOST = "gateway.host";
    public static final String HTTP_PORT = "gateway.port";
    public static final String HTTP_PATH = "gateway.path";
    public static final String DEPLOYMENT_DIR = "gateway.deployment.dir";
    public static final String SECURITY_DIR = "gateway.security.dir";
    public static final String DATA_DIR = "gateway.data.dir";
    public static final String STACKS_SERVICES_DIR = "gateway.services.dir";
    public static final String GLOBAL_RULES_SERVICES = "gateway.global.rules.services";
    public static final String APPLICATIONS_DIR = "gateway.applications.dir";
    public static final String HADOOP_CONF_DIR = "gateway.hadoop.conf.dir";
    public static final String FRONTEND_URL = "gateway.frontend.url";
    private static final String TRUST_ALL_CERTS = "gateway.trust.all.certs";
    private static final String CLIENT_AUTH_NEEDED = "gateway.client.auth.needed";
    private static final String TRUSTSTORE_PATH = "gateway.truststore.path";
    private static final String TRUSTSTORE_TYPE = "gateway.truststore.type";
    private static final String KEYSTORE_TYPE = "gateway.keystore.type";
    private static final String XFORWARDED_ENABLED = "gateway.xforwarded.enabled";
    private static final String EPHEMERAL_DH_KEY_SIZE = "gateway.jdk.tls.ephemeralDHKeySize";
    private static final String HTTP_CLIENT_MAX_CONNECTION = "gateway.httpclient.maxConnections";
    private static final String HTTP_CLIENT_CONNECTION_TIMEOUT = "gateway.httpclient.connectionTimeout";
    private static final String HTTP_CLIENT_SOCKET_TIMEOUT = "gateway.httpclient.socketTimeout";
    private static final String THREAD_POOL_MAX = "gateway.threadpool.max";
    public static final String HTTP_SERVER_REQUEST_BUFFER = "gateway.httpserver.requestBuffer";
    public static final String HTTP_SERVER_REQUEST_HEADER_BUFFER = "gateway.httpserver.requestHeaderBuffer";
    public static final String HTTP_SERVER_RESPONSE_BUFFER = "gateway.httpserver.responseBuffer";
    public static final String HTTP_SERVER_RESPONSE_HEADER_BUFFER = "gateway.httpserver.responseHeaderBuffer";
    public static final String DEPLOYMENTS_BACKUP_VERSION_LIMIT = "gateway.deployment.backup.versionLimit";
    public static final String DEPLOYMENTS_BACKUP_AGE_LIMIT = "gateway.deployment.backup.ageLimit";
    private static final String SSL_ENABLED = "ssl.enabled";
    private static final String SSL_EXCLUDE_PROTOCOLS = "ssl.exclude.protocols";
    private static final String SSL_INCLUDE_CIPHERS = "ssl.include.ciphers";
    private static final String SSL_EXCLUDE_CIPHERS = "ssl.exclude.ciphers";
    public static final String DEFAULT_HTTP_PORT = "8888";
    public static final String DEFAULT_HTTP_PATH = "gateway";
    public static final String DEFAULT_DEPLOYMENT_DIR = "deployments";
    public static final String DEFAULT_SECURITY_DIR = "security";
    public static final String DEFAULT_DATA_DIR = "data";
    private static List<String> DEFAULT_GLOBAL_RULES_SERVICES;

    public LocalGatewayConfig() {
        super(false);
        this.init();
    }

    private String getVar(String variableName, String defaultValue) {
        String value = this.get(variableName);
        if (value == null) {
            value = System.getProperty(variableName);
        }

        if (value == null) {
            value = System.getenv(variableName);
        }

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    private String getGatewayHomeDir() {
        String home = this.get("GATEWAY_HOME", System.getProperty("GATEWAY_HOME", System.getenv("GATEWAY_HOME")));
        return home;
    }

    private void setGatewayHomeDir(String dir) {
        this.set("GATEWAY_HOME", dir);
    }

    public String getGatewayConfDir() {
        String value = this.getVar("GATEWAY_CONF_HOME", this.getGatewayHomeDir() + File.separator + "conf");
        return value;
    }

    public String getGatewayDataDir() {
        String systemValue = System.getProperty("GATEWAY_DATA_HOME", System.getenv("GATEWAY_DATA_HOME"));
        String dataDir = null;
        if (systemValue != null) {
            dataDir = systemValue;
        } else {
            dataDir = this.get("gateway.data.dir", this.getGatewayHomeDir() + File.separator + "data");
        }

        return dataDir;
    }

    public String getGatewayServicesDir() {
        return this.get("gateway.services.dir", this.getGatewayDataDir() + File.separator + "services");
    }

    public String getGatewayApplicationsDir() {
        return this.get("gateway.applications.dir", this.getGatewayDataDir() + File.separator + "applications");
    }

    public String getHadoopConfDir() {
        return this.get("gateway.hadoop.conf.dir");
    }

    private void init() {
        this.setDefaultGlobalRulesServices();
    }

    private void setDefaultGlobalRulesServices() {
        DEFAULT_GLOBAL_RULES_SERVICES = new ArrayList();
        DEFAULT_GLOBAL_RULES_SERVICES.add("NAMENODE");
        DEFAULT_GLOBAL_RULES_SERVICES.add("JOBTRACKER");
        DEFAULT_GLOBAL_RULES_SERVICES.add("WEBHDFS");
        DEFAULT_GLOBAL_RULES_SERVICES.add("WEBHCAT");
        DEFAULT_GLOBAL_RULES_SERVICES.add("OOZIE");
        DEFAULT_GLOBAL_RULES_SERVICES.add("WEBHBASE");
        DEFAULT_GLOBAL_RULES_SERVICES.add("HIVE");
        DEFAULT_GLOBAL_RULES_SERVICES.add("RESOURCEMANAGER");
    }

    public String getGatewayHost() {
        String host = this.get("gateway.host", "0.0.0.0");
        return host;
    }

    public int getGatewayPort() {
        return Integer.parseInt(this.get("gateway.port", "8888"));
    }

    public String getGatewayPath() {
        return this.get("gateway.path", "gateway");
    }

    public String getGatewayTopologyDir() {
        return this.getGatewayConfDir() + File.separator + "topologies";
    }

    public String getGatewayDeploymentDir() {
        return this.get("gateway.deployment.dir", this.getGatewayDataDir() + File.separator + "deployments");
    }

    public String getGatewaySecurityDir() {
        return this.get("gateway.security.dir", this.getGatewayDataDir() + File.separator + "security");
    }

    public InetSocketAddress getGatewayAddress() throws UnknownHostException {
        String host = this.getGatewayHost();
        int port = this.getGatewayPort();
        InetSocketAddress address = new InetSocketAddress(host, port);
        return address;
    }

    public boolean isSSLEnabled() {
        String enabled = this.get("ssl.enabled", "true");
        return "true".equals(enabled);
    }

    public boolean isHadoopKerberosSecured() {
        String hadoopKerberosSecured = this.get("gateway.hadoop.kerberos.secured", "false");
        return "true".equals(hadoopKerberosSecured);
    }

    public String getKerberosConfig() {
        return this.get("java.security.krb5.conf");
    }

    public boolean isKerberosDebugEnabled() {
        String kerberosDebugEnabled = this.get("sun.security.krb5.debug", "false");
        return "true".equals(kerberosDebugEnabled);
    }

    public String getKerberosLoginConfig() {
        return this.get("java.security.auth.login.config");
    }

    public String getDefaultTopologyName() {
        String name = this.get("default.app.topology.name");
        return name != null ? name : GATEWAY_DEFAULT_TOPOLOGY_NAME;
    }

    public String getDefaultAppRedirectPath() {
        String defTopo = this.getDefaultTopologyName();
        return defTopo == null ? null : "/" + this.getGatewayPath() + "/" + defTopo;
    }

    public String getFrontendUrl() {
        String url = this.get("gateway.frontend.url", (String) null);
        return url;
    }

    public List<String> getExcludedSSLProtocols() {
        List protocols = null;
        String value = this.get("ssl.exclude.protocols");
        if (!"none".equals(value)) {
            protocols = Arrays.asList(value.split("\\s*,\\s*"));
        }

        return protocols;
    }

    public List<String> getIncludedSSLCiphers() {
        List list = null;
        String value = this.get("ssl.include.ciphers");
        if (value != null && !value.isEmpty() && !"none".equalsIgnoreCase(value.trim())) {
            list = Arrays.asList(value.trim().split("\\s*,\\s*"));
        }

        return list;
    }

    public List<String> getExcludedSSLCiphers() {
        List list = null;
        String value = this.get("ssl.exclude.ciphers");
        if (value != null && !value.isEmpty() && !"none".equalsIgnoreCase(value.trim())) {
            list = Arrays.asList(value.trim().split("\\s*,\\s*"));
        }

        return list;
    }

    public boolean isClientAuthNeeded() {
        String clientAuthNeeded = this.get("gateway.client.auth.needed", "false");
        return "true".equals(clientAuthNeeded);
    }

    public String getTruststorePath() {
        return this.get("gateway.truststore.path", (String) null);
    }

    public boolean getTrustAllCerts() {
        String trustAllCerts = this.get("gateway.trust.all.certs", "false");
        return "true".equals(trustAllCerts);
    }

    public String getTruststoreType() {
        return this.get("gateway.truststore.type", "JKS");
    }

    public String getKeystoreType() {
        return this.get("gateway.keystore.type", "JKS");
    }

    public boolean isXForwardedEnabled() {
        String xForwardedEnabled = this.get("gateway.xforwarded.enabled", "true");
        return "true".equals(xForwardedEnabled);
    }

    public String getEphemeralDHKeySize() {
        return this.get("gateway.jdk.tls.ephemeralDHKeySize", "2048");
    }

    public int getHttpClientMaxConnections() {
        return this.getInt("gateway.httpclient.maxConnections", 32);
    }

    public int getHttpClientConnectionTimeout() {
        int t = -1;
        String s = this.get("gateway.httpclient.connectionTimeout", (String) null);
        if (s != null) {
            try {
                t = (int) parseNetworkTimeout(s);
            } catch (Exception var4) {
                ;
            }
        }

        return t;
    }

    public int getHttpClientSocketTimeout() {
        int t = -1;
        String s = this.get("gateway.httpclient.socketTimeout", (String) null);
        if (s != null) {
            try {
                t = (int) parseNetworkTimeout(s);
            } catch (Exception var4) {
                ;
            }
        }

        return t;
    }

    public int getThreadPoolMax() {
        int i = this.getInt("gateway.threadpool.max", 254);
        if (i < 5) {
            i = 5;
        }

        return i;
    }

    public int getHttpServerRequestBuffer() {
        int i = this.getInt("gateway.httpserver.requestBuffer", 16384);
        return i;
    }

    public int getHttpServerRequestHeaderBuffer() {
        int i = this.getInt("gateway.httpserver.requestHeaderBuffer", 8192);
        return i;
    }

    public int getHttpServerResponseBuffer() {
        int i = this.getInt("gateway.httpserver.responseBuffer", '耀');
        return i;
    }

    public int getHttpServerResponseHeaderBuffer() {
        int i = this.getInt("gateway.httpserver.responseHeaderBuffer", 8192);
        return i;
    }

    public int getGatewayDeploymentsBackupVersionLimit() {
        int i = this.getInt("gateway.deployment.backup.versionLimit", 5);
        if (i < 0) {
            i = -1;
        }

        return i;
    }

    public long getGatewayDeploymentsBackupAgeLimit() {
        PeriodFormatter f = (new PeriodFormatterBuilder()).appendDays().toFormatter();
        String s = this.get("gateway.deployment.backup.ageLimit", "-1");

        long d;
        try {
            Period e = Period.parse(s, f);
            d = e.toStandardDuration().getMillis();
            if (d < 0L) {
                d = -1L;
            }
        } catch (Exception var6) {
            d = -1L;
        }

        return d;
    }

    public String getSigningKeystoreName() {
        return this.get("gateway.signing.keystore.name");
    }

    public String getSigningKeyAlias() {
        return this.get("gateway.signing.key.alias");
    }

    public List<String> getGlobalRulesServices() {
        String value = this.get("gateway.global.rules.services");
        return value != null && !value.isEmpty() && !"none".equalsIgnoreCase(value.trim()) ? Arrays.asList(value.trim().split("\\s*,\\s*")) : DEFAULT_GLOBAL_RULES_SERVICES;
    }

    private static long parseNetworkTimeout(String s) {
        PeriodFormatter f = (new PeriodFormatterBuilder()).appendMinutes().appendSuffix("m", " min").appendSeconds().appendSuffix("s", " sec").appendMillis().toFormatter();
        Period p = Period.parse(s, f);
        return p.toStandardDuration().getMillis();
    }

    @Override
    public List<String> getMimeTypesToCompress() {
        List<String> mimeTypes = null;
        String value = get(MIME_TYPES_TO_COMPRESS, DEFAULT_MIME_TYPES_TO_COMPRESS);
        if (value != null && !value.isEmpty()) {
            mimeTypes = Arrays.asList(value.trim().split("\\s*,\\s*"));
        }
        return mimeTypes;
    }

  @Override
  public boolean isCookieScopingToPathEnabled() {
    return false;
  }

  @Override
    public boolean isWebsocketEnabled() {
        final String result = get( WEBSOCKET_FEATURE_ENABLED, Boolean.toString(DEFAULT_WEBSOCKET_FEATURE_ENABLED));
        return Boolean.parseBoolean(result);
    }

    @Override
    public int getWebsocketMaxTextMessageSize() {
        return getInt( WEBSOCKET_MAX_TEXT_MESSAGE_SIZE, DEFAULT_WEBSOCKET_MAX_TEXT_MESSAGE_SIZE);
    }

    @Override
    public int getWebsocketMaxBinaryMessageSize() {
        return getInt( WEBSOCKET_MAX_BINARY_MESSAGE_SIZE, DEFAULT_WEBSOCKET_MAX_BINARY_MESSAGE_SIZE);
    }

    @Override
    public int getWebsocketMaxTextMessageBufferSize() {
        return getInt( WEBSOCKET_MAX_TEXT_MESSAGE_BUFFER_SIZE, DEFAULT_WEBSOCKET_MAX_TEXT_MESSAGE_BUFFER_SIZE);
    }

    @Override
    public int getWebsocketMaxBinaryMessageBufferSize() {
        return getInt( WEBSOCKET_MAX_BINARY_MESSAGE_BUFFER_SIZE, DEFAULT_WEBSOCKET_MAX_BINARY_MESSAGE_BUFFER_SIZE);
    }

    @Override
    public int getWebsocketInputBufferSize() {
        return getInt( WEBSOCKET_INPUT_BUFFER_SIZE, DEFAULT_WEBSOCKET_INPUT_BUFFER_SIZE);
    }

    @Override
    public int getWebsocketAsyncWriteTimeout() {
        return getInt( WEBSOCKET_ASYNC_WRITE_TIMEOUT, DEFAULT_WEBSOCKET_ASYNC_WRITE_TIMEOUT);
    }

    @Override
    public int getWebsocketIdleTimeout() {
        return getInt( WEBSOCKET_IDLE_TIMEOUT, DEFAULT_WEBSOCKET_IDLE_TIMEOUT);
    }

    @Override
    public boolean isMetricsEnabled() {
        String metricsEnabled = get( METRICS_ENABLED, "true" );
        return "true".equals(metricsEnabled);
    }

    @Override
    public boolean isJmxMetricsReportingEnabled() {
        String enabled = get( JMX_METRICS_REPORTING_ENABLED, "true" );
        return "true".equals(enabled);
    }

    @Override
    public boolean isGraphiteMetricsReportingEnabled() {
        String enabled = get( GRAPHITE_METRICS_REPORTING_ENABLED, "false" );
        return "true".equals(enabled);
    }

    @Override
    public String getGraphiteHost() {
        String host = get( GRAPHITE_METRICS_REPORTING_HOST, "localhost" );
        return host;
    }

    @Override
    public int getGraphitePort() {
        int i = getInt( GRAPHITE_METRICS_REPORTING_PORT, 32772 );
        return i;
    }

    @Override
    public int getGraphiteReportingFrequency() {
        int i = getInt( GRAPHITE_METRICS_REPORTING_FREQUENCY, 1 );
        return i;
    }

    @Override
    public long getGatewayIdleTimeout() {
        return getLong(GATEWAY_IDLE_TIMEOUT, 300000l);
    }
}
