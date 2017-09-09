package com.github.sakserv.minicluster.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.gateway.GatewayMessages;
import org.apache.hadoop.gateway.config.GatewayConfig;
import org.apache.hadoop.gateway.config.impl.GatewayConfigImpl;
import org.apache.hadoop.gateway.i18n.messages.MessagesFactory;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.gateway.config.impl.GatewayConfigImpl.*;

public class LocalGatewayConfig extends Configuration implements GatewayConfig {
    private static final String GATEWAY_DEFAULT_TOPOLOGY_NAME = null;
    private static final String GATEWAY_CONFIG_FILE_PREFIX = "gateway";
    public static final String HTTP_HOST = "gateway.host";
    public static final String HTTP_PORT = "gateway.port";
    public static final String HTTP_PATH = "gateway.path";
    private static List<String> DEFAULT_GLOBAL_RULES_SERVICES;
    private static final String CRYPTO_ALGORITHM = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.algorithm";
    private static final String CRYPTO_PBE_ALGORITHM = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.pbe.algorithm";
    private static final String CRYPTO_TRANSFORMATION = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.transformation";
    private static final String CRYPTO_SALTSIZE = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.salt.size";
    private static final String CRYPTO_ITERATION_COUNT = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.iteration.count";
    private static final String CRYPTO_KEY_LENGTH = GATEWAY_CONFIG_FILE_PREFIX + ".crypto.key.length";
    public static final String SERVER_HEADER_ENABLED = GATEWAY_CONFIG_FILE_PREFIX + ".server.header.enabled";

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

    @Override
    public boolean isGatewayPortMappingEnabled() {
        String enabled = get( GATEWAY_PORT_MAPPING_ENABLED, "false" );
        return "true".equals(enabled);
    }

    @Override
    public String getAlgorithm() {
        return getVar(CRYPTO_ALGORITHM, null);
    }

    @Override
    public String getPBEAlgorithm() {
        return getVar(CRYPTO_PBE_ALGORITHM, null);
    }

    @Override
    public String getTransformation() {
        return getVar(CRYPTO_TRANSFORMATION, null);
    }

    @Override
    public String getSaltSize() {
        return getVar(CRYPTO_SALTSIZE, null);
    }

    @Override
    public String getIterationCount() {
        return getVar(CRYPTO_ITERATION_COUNT, null);
    }

    @Override
    public String getKeyLength() {
        return getVar(CRYPTO_KEY_LENGTH, null);
    }

    @Override
    public boolean isGatewayServerHeaderEnabled() {
        return Boolean.parseBoolean(getVar(SERVER_HEADER_ENABLED, "true"));
    }

    /**
     * Map of Topology names and their ports.
     *
     * @return
     */
    @Override
    public Map<String, Integer> getGatewayPortMappings() {

        final Map<String, Integer> result = new ConcurrentHashMap<String, Integer>();
        final Map<String, String> properties = getValByRegex(GATEWAY_PORT_MAPPING_REGEX);

        // Convert port no. from string to int
        for(final Map.Entry<String, String> e : properties.entrySet()) {
            // ignore the GATEWAY_PORT_MAPPING_ENABLED property
            if(!e.getKey().equalsIgnoreCase(GATEWAY_PORT_MAPPING_ENABLED)) {
                // extract the topology name and use it as a key
                result.put(StringUtils.substringAfter(e.getKey(), GATEWAY_PORT_MAPPING_PREFIX), Integer.parseInt(e.getValue()) );
            }

        }

        return Collections.unmodifiableMap(result);
    }

}
