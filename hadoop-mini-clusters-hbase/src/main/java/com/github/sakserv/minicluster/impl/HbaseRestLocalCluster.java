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
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.rest.RESTServer;
import org.apache.hadoop.hbase.rest.RESTServlet;
import org.apache.hadoop.hbase.rest.ResourceConfig;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.HttpServerUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class HbaseRestLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HbaseRestLocalCluster.class);

    private Server server;

    Integer hbaseRestPort;
    private Integer hbaseRestInfoPort;
    private String hbaseRestHost;
    private Boolean hbaseRestReadOnly;
    private Integer hbaseRestThreadMin;
    private Integer hbaseRestThreadMax;
    private HbaseLocalCluster.Builder builder;

    public Integer getHbaseRestPort() {
        return hbaseRestPort;
    }

    public Integer getHbaseRestInfoPort() {
        return hbaseRestInfoPort;
    }

    public String getHbaseRestHost() {
        return hbaseRestHost;
    }

    public Boolean getHbaseRestReadOnly() {
        return hbaseRestReadOnly;
    }

    public Integer getHbaseRestThreadMin() {
        return hbaseRestThreadMin;
    }

    public Integer getHbaseRestThreadMax() {
        return hbaseRestThreadMax;
    }

    public HbaseLocalCluster.Builder getBuilder() {
        return builder;
    }

    private HbaseRestLocalCluster(RestBuilder builder) {
        this.hbaseRestPort = builder.hbaseRestPort;
        this.hbaseRestInfoPort = builder.hbaseRestInfoPort;
        this.hbaseRestHost = builder.hbaseRestHost;
        this.hbaseRestReadOnly = builder.hbaseRestReadOnly;
        this.hbaseRestThreadMin = builder.hbaseRestThreadMin;
        this.hbaseRestThreadMax = builder.hbaseRestThreadMax;
        this.builder = builder.builder;
    }

    static class RestBuilder {
        private Integer hbaseRestPort;
        private Integer hbaseRestInfoPort;
        private String hbaseRestHost;
        private Boolean hbaseRestReadOnly;
        private Integer hbaseRestThreadMin;
        private Integer hbaseRestThreadMax;
        private HbaseLocalCluster.Builder builder;

        public RestBuilder(HbaseLocalCluster.Builder builder) {
            this.builder = builder;
        }

        public RestBuilder setHbaseRestPort(Integer hbaseRestPort) {
            this.hbaseRestPort = hbaseRestPort;
            return this;
        }

        public RestBuilder setHbaseRestInfoPort(Integer hbaseRestInfoPort) {
            this.hbaseRestInfoPort = hbaseRestInfoPort;
            return this;
        }

        public RestBuilder setHbaseRestHost(String hbaseRestHost) {
            this.hbaseRestHost = hbaseRestHost;
            return this;
        }

        public RestBuilder setHbaseRestReadOnly(Boolean hbaseRestReadOnly) {
            this.hbaseRestReadOnly = hbaseRestReadOnly;
            return this;
        }

        public RestBuilder setHbaseRestThreadMin(Integer hbaseRestThreadMin) {
            this.hbaseRestThreadMin = hbaseRestThreadMin;
            return this;
        }

        public RestBuilder setHbaseRestThreadMax(Integer hbaseRestThreadMax) {
            this.hbaseRestThreadMax = hbaseRestThreadMax;
            return this;
        }

        public HbaseLocalCluster.Builder build() {
            HbaseRestLocalCluster hbaseRestLocalCluster = new HbaseRestLocalCluster(this);
            validateObject(hbaseRestLocalCluster);
            builder.setHbaseRestLocalCluster(hbaseRestLocalCluster);
            return builder;
        }

        public void validateObject(HbaseRestLocalCluster hbaseRestLocalCluster) {
            if (hbaseRestLocalCluster.hbaseRestPort == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: HBase Rest Port");
            }
        }
    }

    @Override
    public void start() throws Exception {
        VersionInfo.logVersion();
        Configuration conf = builder.getHbaseConfiguration();

        conf.set("hbase.rest.port", hbaseRestPort.toString());
        conf.set("hbase.rest.readonly", (hbaseRestReadOnly == null) ? "true" : hbaseRestReadOnly.toString());
        conf.set("hbase.rest.info.port", (hbaseRestInfoPort == null) ? "8085" : hbaseRestInfoPort.toString());
        String hbaseRestHost = (this.hbaseRestHost == null) ? "0.0.0.0" : this.hbaseRestHost;

        Integer hbaseRestThreadMax = (this.hbaseRestThreadMax == null) ? 100 : this.hbaseRestThreadMax;
        Integer hbaseRestThreadMin = (this.hbaseRestThreadMin == null) ? 2 : this.hbaseRestThreadMin;

        UserProvider userProvider = UserProvider.instantiate(conf);
        Pair<FilterHolder, Class<? extends ServletContainer>> pair = loginServerPrincipal(userProvider, conf);
        FilterHolder authFilter = pair.getFirst();
        Class<? extends ServletContainer> containerClass = pair.getSecond();
        RESTServlet.getInstance(conf, userProvider);

        // set up the Jersey servlet container for Jetty
        ServletHolder sh = new ServletHolder(containerClass);
        sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", ResourceConfig.class.getCanonicalName());
        sh.setInitParameter("com.sun.jersey.config.property.packages", "jetty");
        ServletHolder shPojoMap = new ServletHolder(containerClass);
        Map<String, String> shInitMap = sh.getInitParameters();
        for (Map.Entry<String, String> e : shInitMap.entrySet()) {
            shPojoMap.setInitParameter(e.getKey(), e.getValue());
        }
        shPojoMap.setInitParameter(JSONConfiguration.FEATURE_POJO_MAPPING, "true");

        // set up Jetty and run the embedded server

        server = new Server();

        Connector connector = new SelectChannelConnector();
        if (conf.getBoolean(RESTServer.REST_SSL_ENABLED, false)) {
            SslSelectChannelConnector sslConnector = new SslSelectChannelConnector();
            String keystore = conf.get(RESTServer.REST_SSL_KEYSTORE_STORE);
            String password = HBaseConfiguration.getPassword(conf, RESTServer.REST_SSL_KEYSTORE_PASSWORD, null);
            String keyPassword = HBaseConfiguration.getPassword(conf, RESTServer.REST_SSL_KEYSTORE_KEYPASSWORD, password);
            sslConnector.setKeystore(keystore);
            sslConnector.setPassword(password);
            sslConnector.setKeyPassword(keyPassword);
            connector = sslConnector;
        }
        connector.setPort(hbaseRestPort);
        connector.setHost(hbaseRestHost);
        connector.setHeaderBufferSize(8192);


        server.addConnector(connector);

        QueuedThreadPool threadPool = new QueuedThreadPool(hbaseRestThreadMax);
        threadPool.setMinThreads(hbaseRestThreadMin);
        server.setThreadPool(threadPool);

        server.setSendServerVersion(false);
        server.setSendDateHeader(false);
        server.setStopAtShutdown(true);
        // set up context
        Context context = new Context(server, "/", Context.SESSIONS);
        context.addServlet(shPojoMap, "/status/cluster");
        context.addServlet(sh, "/*");
        if (authFilter != null) {
            context.addFilter(authFilter, "/*", 1);
        }

        HttpServerUtil.constrainHttpMethods(context);

        // Put up info server.
        int port = (hbaseRestInfoPort == null) ? 8085 : hbaseRestInfoPort;
        if (port >= 0) {
            conf.setLong("startcode", System.currentTimeMillis());
            String a = hbaseRestHost;
            InfoServer infoServer = new InfoServer("rest", a, port, false, conf);
            infoServer.setAttribute("hbase.conf", conf);
            infoServer.start();
        }
        // start server
        server.start();
    }

    @Override
    public void stop() throws Exception {
        server.stop();
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        stop();
    }

    @Override
    public void configure() throws Exception {
        //NOTHING TO DO
    }

    @Override
    public void cleanUp() throws Exception {
        //NOTHING TO DO
    }

    Pair<FilterHolder, Class<? extends ServletContainer>> loginServerPrincipal(UserProvider userProvider, Configuration conf) throws Exception {
        Class<? extends ServletContainer> containerClass = ServletContainer.class;
        return new Pair<>(null, containerClass);
    }
}
