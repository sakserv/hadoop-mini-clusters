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
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.gateway.GatewayServer;
import org.apache.hadoop.gateway.services.DefaultGatewayServices;
import org.apache.hadoop.gateway.services.ServiceLifecycleException;
import org.apache.hadoop.gateway.services.topology.impl.DefaultTopologyService;
import org.apache.hadoop.gateway.topology.Service;
import org.apache.hadoop.gateway.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * @author Vincent Devillers
 */
public class KnoxLocalCluster implements MiniCluster {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KnoxLocalCluster.class);

    private GatewayServer gatewayServer;
    private File gatewayDir;

    private String host;
    private Integer port;
    private String homeDir;
    private String path;
    private String topology;
    private String cluster;

    public Integer getPort() {
        return port;
    }

    public String getHomeDir() {
        return homeDir;
    }

    public String getTopology() {
        return topology;
    }

    public String getPath() {
        return path;
    }

    public String getHost() {
        return host;
    }

    public String getCluster() {
        return cluster;
    }

    private KnoxLocalCluster(Builder builder) {
        this.port = builder.port;
        this.homeDir = builder.homeDir;
        this.path = builder.path;
        this.topology = builder.topology;
        this.host = builder.host;
        this.cluster = builder.cluster;
    }

    public static class Builder {
        private String host = "localhost";
        private Integer port;
        private String homeDir;
        private String path;
        private String topology;
        private String cluster;

        public Builder setHost(String Host) {
            this.host = Host;
            return this;
        }

        public Builder setPort(Integer Port) {
            this.port = Port;
            return this;
        }

        public Builder setHomeDir(String homeDir) {
            this.homeDir = homeDir;
            return this;
        }

        public Builder setCluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder setPath(String Path) {
            this.path = Path;
            return this;
        }

        public Builder setTopology(String topology) {
            this.topology = topology;
            return this;
        }

        public KnoxLocalCluster build() {
            KnoxLocalCluster knoxLocalCluster = new KnoxLocalCluster(this);
            validateObject(knoxLocalCluster);
            return knoxLocalCluster;
        }

        public void validateObject(KnoxLocalCluster knoxLocalCluster) {
            if (knoxLocalCluster.port == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: port");
            }

            if (knoxLocalCluster.homeDir == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: homeDir");
            }

            if (knoxLocalCluster.path == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: path");
            }

            if (knoxLocalCluster.topology == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: topology");
            }

            if (knoxLocalCluster.cluster == null) {
                throw new IllegalArgumentException("ERROR: Missing required config: cluster");
            }
        }
    }

    @Override
    public void start() throws Exception {

        LOG.info("KNOX: Starting GatewayServer");
        configure();

        gatewayDir = new File(homeDir, "gateway-home-" + UUID.randomUUID());
        gatewayDir.mkdirs();

        LocalGatewayConfig config = new LocalGatewayConfig();
        config.set("GATEWAY_HOME", gatewayDir.getAbsolutePath());
        config.set(LocalGatewayConfig.HTTP_HOST, host);
        config.setInt(LocalGatewayConfig.HTTP_PORT, port);
        config.set(LocalGatewayConfig.HTTP_PATH, path);
        config.set("default.app.topology.name", cluster);
        config.set("ssl.exclude.protocols", "none");

        // {GATEWAY_HOME}/data
        File dataDir = new File(config.getGatewayDataDir());
        dataDir.mkdirs();

        // {GATEWAY_HOME}/data/deployments
        File deploymentDir = new File(config.getGatewayDeploymentDir());
        deploymentDir.mkdirs();

        // {GATEWAY_HOME}/data/security
        File securityDir = new File(config.getGatewaySecurityDir());
        securityDir.mkdirs();

        // {GATEWAY_HOME}/data/services
        File stacksDir = new File(config.getGatewayServicesDir());
        stacksDir.mkdirs();

        // {GATEWAY_HOME}/conf
        //config.set(LocalGatewayConfig.HADOOP_CONF_DIR, new File(gatewayDir, "conf").getAbsolutePath());
        File confDir = new File(config.getGatewayConfDir());
        confDir.mkdirs();

        // {GATEWAY_HOME}/conf/topologies
        File topologiesDirectory = new File(config.getGatewayTopologyDir());
        topologiesDirectory.mkdirs();

        LOG.info("Using topology: {}", topology);
        File descriptor = new File(topologiesDirectory, cluster + ".xml");
        try (FileOutputStream stream = new FileOutputStream(descriptor)) {
            stream.write(topology.getBytes());
        }

        DefaultGatewayServices services = new DefaultGatewayServices();

        Map<String, String> options = new HashMap<>();
        options.put("persist-master", "false");
        options.put("master", "password");

        try {
            services.init(config, options);
        } catch (ServiceLifecycleException e) {
            LOG.error("Unable to init the services", e);
            throw Throwables.propagate(e);
        }

        URL resource = getClass().getClassLoader().getResource("services");
        // Copy the services definitions from the JAR
        copyResourcesRecursively(resource, stacksDir);

        StringWriter writer = new StringWriter();
        config.writeXml(writer);
        LOG.info("Using gateway-site.xml:{}", writer.toString());

        gatewayServer = GatewayServer.startGateway(config, services);
        LOG.info("Gateway address = " + gatewayServer.getURI());

        DefaultTopologyService topologyService = services.getService(DefaultGatewayServices.TOPOLOGY_SERVICE);
        Topology topology = topologyService.getTopologies().iterator().next();
        for (Service service : topology.getServices()) {
            LOG.info("Deployed: {} -> {}", service.getRole(), StringUtils.join(service.getUrls(), ","));
        }
    }

    @Override
    public void stop() throws Exception {
        stop(true);
    }

    @Override
    public void stop(boolean cleanUp) throws Exception {
        LOG.info("KNOX: Stopping GatewayServer");
        gatewayServer.stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    @Override
    public void configure() throws Exception {
    }

    @Override
    public void cleanUp() throws Exception {
        FileUtils.deleteFolder(gatewayDir.getAbsolutePath());
    }


    public static boolean copyFile(final File toCopy, final File destFile) {
        try {
            return copyStream(new FileInputStream(toCopy),
                    new FileOutputStream(destFile));
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean copyFilesRecusively(final File toCopy,
                                               final File destDir) {
        assert destDir.isDirectory();

        if (!toCopy.isDirectory()) {
            return copyFile(toCopy, new File(destDir, toCopy.getName()));
        } else {
            final File newDestDir = new File(destDir, toCopy.getName());
            if (!newDestDir.exists() && !newDestDir.mkdir()) {
                return false;
            }
            for (final File child : toCopy.listFiles()) {
                if (!copyFilesRecusively(child, newDestDir)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean copyJarResourcesRecursively(final File destDir,
                                                      final JarURLConnection jarConnection) throws IOException {

        final JarFile jarFile = jarConnection.getJarFile();

        for (final Enumeration<JarEntry> e = jarFile.entries(); e.hasMoreElements(); ) {
            final JarEntry entry = e.nextElement();
            if (entry.getName().startsWith(jarConnection.getEntryName())) {
                final String filename = StringUtils.removeStart(entry.getName(), //
                        jarConnection.getEntryName());

                final File f = new File(destDir, filename);
                if (!entry.isDirectory()) {
                    final InputStream entryInputStream = jarFile.getInputStream(entry);
                    if (!copyStream(entryInputStream, f)) {
                        return false;
                    }
                    entryInputStream.close();
                } else {
                    if (!ensureDirectoryExists(f)) {
                        throw new IOException("Could not create directory: "
                                + f.getAbsolutePath());
                    }
                }
            }
        }
        return true;
    }

    public static boolean copyResourcesRecursively( //
                                                    final URL originUrl, final File destination) {
        try {
            final URLConnection urlConnection = originUrl.openConnection();
            if (urlConnection instanceof JarURLConnection) {
                return copyJarResourcesRecursively(destination,
                        (JarURLConnection) urlConnection);
            } else {
                return copyFilesRecusively(new File(originUrl.getPath()),
                        destination);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean copyStream(final InputStream is, final File f) {
        try {
            return copyStream(is, new FileOutputStream(f));
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean copyStream(final InputStream is, final OutputStream os) {
        try {
            final byte[] buf = new byte[1024];

            int len = 0;
            while ((len = is.read(buf)) > 0) {
                os.write(buf, 0, len);
            }
            is.close();
            os.close();
            return true;
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean ensureDirectoryExists(final File f) {
        return f.exists() || f.mkdir();
    }
}
