/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.gateway;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.gateway.audit.api.*;
import org.apache.hadoop.gateway.audit.log4j.audit.AuditConstants;
import org.apache.hadoop.gateway.config.GatewayConfig;
import org.apache.hadoop.gateway.config.impl.GatewayConfigImpl;
import org.apache.hadoop.gateway.deploy.DeploymentException;
import org.apache.hadoop.gateway.deploy.DeploymentFactory;
import org.apache.hadoop.gateway.filter.CorrelationHandler;
import org.apache.hadoop.gateway.filter.DefaultTopologyHandler;
import org.apache.hadoop.gateway.i18n.messages.MessagesFactory;
import org.apache.hadoop.gateway.i18n.resources.ResourcesFactory;
import org.apache.hadoop.gateway.services.GatewayServices;
import org.apache.hadoop.gateway.services.registry.ServiceRegistry;
import org.apache.hadoop.gateway.services.security.SSLService;
import org.apache.hadoop.gateway.services.topology.TopologyService;
import org.apache.hadoop.gateway.topology.Application;
import org.apache.hadoop.gateway.topology.Topology;
import org.apache.hadoop.gateway.topology.TopologyEvent;
import org.apache.hadoop.gateway.topology.TopologyListener;
import org.apache.hadoop.gateway.trace.AccessHandler;
import org.apache.hadoop.gateway.trace.ErrorHandler;
import org.apache.hadoop.gateway.trace.TraceHandler;
import org.apache.hadoop.gateway.util.Urls;
import org.apache.hadoop.gateway.util.XmlUtils;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jboss.shrinkwrap.api.exporter.ExplodedExporter;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;


/**
 * Big Hack to avoid these kind of errors:
 * java.lang.IncompatibleClassChangeError: class org.eclipse.jetty.annotations.AnnotationParser$MyClassVisitor has interface org.objectweb.asm.ClassVisitor as super class
 *
 * https://issues.apache.org/jira/browse/KNOX-943
 *
 * The feature : Loading jsp has been disabled
 */
public class GatewayServer {
    private static GatewayResources res = ResourcesFactory.get(GatewayResources.class);
    private static GatewayMessages log = MessagesFactory.get(GatewayMessages.class);
    private static Auditor auditor = AuditServiceFactory.getAuditService().getAuditor(AuditConstants.DEFAULT_AUDITOR_NAME,
            AuditConstants.KNOX_SERVICE_NAME, AuditConstants.KNOX_COMPONENT_NAME);
    private static GatewayServer server;
    private static GatewayServices services;

    private static Properties buildProperties;

    private Server jetty;
    private GatewayConfig config;
    private ContextHandlerCollection contexts;
    private TopologyService monitor;
    private TopologyListener listener;
    private Map<String, WebAppContext> deployments;

    public static void main(String[] args) {
        try {
            configureLogging();
            logSysProps();
            CommandLine cmd = GatewayCommandLine.parse(args);
            if (cmd.hasOption(GatewayCommandLine.HELP_LONG)) {
                GatewayCommandLine.printHelp();
            } else if (cmd.hasOption(GatewayCommandLine.VERSION_LONG)) {
                printVersion();
            } else if (cmd.hasOption(GatewayCommandLine.REDEPLOY_LONG)) {
                redeployTopologies(cmd.getOptionValue(GatewayCommandLine.REDEPLOY_LONG));
            } else {
                buildProperties = loadBuildProperties();
                services = instantiateGatewayServices();
                if (services == null) {
                    log.failedToInstantiateGatewayServices();
                }
                GatewayConfig config = new GatewayConfigImpl();
                if (config.isHadoopKerberosSecured()) {
                    configureKerberosSecurity(config);
                }
                Map<String, String> options = new HashMap<String, String>();
                options.put(GatewayCommandLine.PERSIST_LONG, Boolean.toString(cmd.hasOption(GatewayCommandLine.PERSIST_LONG)));
                services.init(config, options);
                if (!cmd.hasOption(GatewayCommandLine.NOSTART_LONG)) {
                    startGateway(config, services);
                }
            }
        } catch (ParseException e) {
            log.failedToParseCommandLine(e);
            GatewayCommandLine.printHelp();
        } catch (Exception e) {
            log.failedToStartGateway(e);
            // Make sure the process exits.
            System.exit(1);
        }
    }

    private static void printVersion() {
        System.out.println(res.gatewayVersionMessage( // I18N not required.
                getBuildVersion(),
                getBuildHash()));
    }

    public static String getBuildHash() {
        String hash = "unknown";
        if (buildProperties != null) {
            hash = buildProperties.getProperty("build.hash", hash);
        }
        return hash;
    }

    public static String getBuildVersion() {
        String version = "unknown";
        if (buildProperties != null) {
            version = buildProperties.getProperty("build.version", version);
        }
        return version;
    }

    private static GatewayServices instantiateGatewayServices() {
        ServiceLoader<GatewayServices> loader = ServiceLoader.load(GatewayServices.class);
        Iterator<GatewayServices> services = loader.iterator();
        if (services.hasNext()) {
            return services.next();
        }
        return null;
    }

    public static synchronized GatewayServices getGatewayServices() {
        return services;
    }

    private static void logSysProp(String name) {
        log.logSysProp(name, System.getProperty(name));
    }

    private static void logSysProps() {
        logSysProp("user.name");
        logSysProp("user.dir");
        logSysProp("java.runtime.name");
        logSysProp("java.runtime.version");
        logSysProp("java.home");
    }

    private static void configureLogging() {
        PropertyConfigurator.configure(System.getProperty("log4j.configuration"));
//    String fileName = config.getGatewayConfDir() + File.separator + "log4j.properties";
//    File file = new File( fileName );
//    if( file.isFile() && file.canRead() ) {
//      FileInputStream stream;
//      try {
//        stream = new FileInputStream( file );
//        Properties properties = new Properties();
//        properties.load( stream );
//        PropertyConfigurator.configure( properties );
//        log.loadedLoggingConfig( fileName );
//      } catch( IOException e ) {
//        log.failedToLoadLoggingConfig( fileName );
//      }
//    }
    }

    private static void configureKerberosSecurity(GatewayConfig config) {
        System.setProperty(GatewayConfig.HADOOP_KERBEROS_SECURED, "true");
        System.setProperty(GatewayConfig.KRB5_CONFIG, config.getKerberosConfig());
        System.setProperty(GatewayConfig.KRB5_DEBUG,
                Boolean.toString(config.isKerberosDebugEnabled()));
        System.setProperty(GatewayConfig.KRB5_LOGIN_CONFIG, config.getKerberosLoginConfig());
        System.setProperty(GatewayConfig.KRB5_USE_SUBJECT_CREDS_ONLY, "false");
    }

    private static Properties loadBuildProperties() {
        Properties properties = new Properties();
        InputStream inputStream = GatewayServer.class.getClassLoader().getResourceAsStream("build.properties");
        if (inputStream != null) {
            try {
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
        return properties;
    }

    public static void redeployTopologies(String topologyName) {
        TopologyService ts = getGatewayServices().getService(GatewayServices.TOPOLOGY_SERVICE);
        ts.reloadTopologies();
        ts.redeployTopologies(topologyName);
    }

    private void cleanupTopologyDeployments() {
        File deployDir = new File(config.getGatewayDeploymentDir());
        TopologyService ts = getGatewayServices().getService(GatewayServices.TOPOLOGY_SERVICE);
        for (Topology topology : ts.getTopologies()) {
            cleanupTopologyDeployments(deployDir, topology);
        }
    }

    private void cleanupTopologyDeployments(File deployDir, Topology topology) {
        log.cleanupDeployments(topology.getName());
        File[] files = deployDir.listFiles(new RegexDirFilter(topology.getName() + "\\.(war|topo)\\.[0-9A-Fa-f]+"));
        if (files != null) {
            Arrays.sort(files, new FileModificationTimeDescendingComparator());
            int verLimit = config.getGatewayDeploymentsBackupVersionLimit();
            long ageLimit = config.getGatewayDeploymentsBackupAgeLimit();
            long keepTime = System.currentTimeMillis() - ageLimit;
            for (int i = 1; i < files.length; i++) {
                File file = files[i];
                if (((verLimit >= 0) && (i > verLimit)) ||
                        ((ageLimit >= 0) && (file.lastModified() < keepTime))) {
                    log.cleanupDeployment(file.getAbsolutePath());
                    FileUtils.deleteQuietly(file);
                }
            }
        }
    }

    public static GatewayServer startGateway(GatewayConfig config, GatewayServices svcs) throws Exception {
        log.startingGateway();
        server = new GatewayServer(config);
        synchronized (server) {
            //KM[ Commented this out because is causes problems with
            // multiple services instance used in a single test process.
            // I'm not sure what drive including this check though.
            //if (services == null) {
            services = svcs;
            //}
            //KM]
            services.start();
            DeploymentFactory.setGatewayServices(services);
            server.start();
            log.startedGateway(server.jetty.getURI().getPort());
            return server;
        }
    }

    public GatewayServer(GatewayConfig config) {
        this(config, null);
    }

    public GatewayServer(GatewayConfig config, Properties options) {
        this.config = config;
        this.listener = new InternalTopologyListener();
    }

    private static Connector createConnector(final Server server, final GatewayConfig config) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        ServerConnector connector;

        // Determine the socket address and check availability.
        InetSocketAddress address = config.getGatewayAddress();
        checkAddressAvailability(address);

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setRequestHeaderSize(config.getHttpServerRequestHeaderBuffer());
        //httpConfig.setRequestBufferSize( config.getHttpServerRequestBuffer() );
        httpConfig.setResponseHeaderSize(config.getHttpServerResponseHeaderBuffer());
        httpConfig.setOutputBufferSize(config.getHttpServerResponseBuffer());

        if (config.isSSLEnabled()) {
            HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
            httpsConfig.setSecureScheme("https");
            httpsConfig.setSecurePort(address.getPort());
            httpsConfig.addCustomizer(new SecureRequestCustomizer());
            SSLService ssl = services.getService("SSLService");
            String keystoreFileName = config.getGatewaySecurityDir() + File.separatorChar + "keystores" + File.separatorChar + "gateway.jks";
            SslContextFactory sslContextFactory = (SslContextFactory) ssl.buildSslContextFactory(keystoreFileName);
            connector = new ServerConnector(server, sslContextFactory, new HttpConnectionFactory(httpsConfig));
        } else {
            connector = new ServerConnector(server);
        }
        connector.setHost(address.getHostName());
        connector.setPort(address.getPort());

        return connector;
    }

    private static HandlerCollection createHandlers(
            final GatewayConfig config,
            final GatewayServices services,
            final ContextHandlerCollection contexts) {
        HandlerCollection handlers = new HandlerCollection();
        RequestLogHandler logHandler = new RequestLogHandler();
        logHandler.setRequestLog(new AccessHandler());

        TraceHandler traceHandler = new TraceHandler();
        traceHandler.setHandler(contexts);
        traceHandler.setTracedBodyFilter(System.getProperty("org.apache.knox.gateway.trace.body.status.filter"));

        CorrelationHandler correlationHandler = new CorrelationHandler();
        correlationHandler.setHandler(traceHandler);

        DefaultTopologyHandler defaultTopoHandler = new DefaultTopologyHandler(config, services, contexts);

        handlers.setHandlers(new Handler[]{correlationHandler, defaultTopoHandler, logHandler});
        return handlers;
    }

    private synchronized void start() throws Exception {
        // Create the global context handler.
        contexts = new ContextHandlerCollection();
        // A map to keep track of current deployments by cluster name.
        deployments = new ConcurrentHashMap<String, WebAppContext>();

        // Start Jetty.
        jetty = new Server(new QueuedThreadPool(config.getThreadPoolMax()));
        jetty.addConnector(createConnector(jetty, config));
        jetty.setHandler(createHandlers(config, services, contexts));

        // Add Annotations processing into the Jetty server to support JSPs
//        Configuration.ClassList classlist = Configuration.ClassList.setServerDefault( jetty );
//        classlist.addBefore(
//                "org.eclipse.jetty.webapp.JettyWebXmlConfiguration",
//                "org.eclipse.jetty.annotations.AnnotationConfiguration" );

        // Load the current topologies.
        File topologiesDir = calculateAbsoluteTopologiesDir();
        log.loadingTopologiesFromDirectory(topologiesDir.getAbsolutePath());
        monitor = services.getService(GatewayServices.TOPOLOGY_SERVICE);
        monitor.addTopologyChangeListener(listener);
        monitor.reloadTopologies();

        try {
            jetty.start();
        } catch (IOException e) {
            log.failedToStartGateway(e);
            throw e;
        }

        cleanupTopologyDeployments();

        // Start the topology monitor.
        log.monitoringTopologyChangesInDirectory(topologiesDir.getAbsolutePath());
        monitor.startMonitor();
    }

    public synchronized void stop() throws Exception {
        log.stoppingGateway();
        services.stop();
        monitor.stopMonitor();
        jetty.stop();
        jetty.join();
        log.stoppedGateway();
    }

    public URI getURI() {
        return jetty.getURI();
    }

    public InetSocketAddress[] getAddresses() {
        InetSocketAddress[] addresses = new InetSocketAddress[jetty.getConnectors().length];
        for (int i = 0, n = addresses.length; i < n; i++) {
            NetworkConnector connector = (NetworkConnector) jetty.getConnectors()[i];
            String host = connector.getHost();
            if (host == null) {
                addresses[i] = new InetSocketAddress(connector.getLocalPort());
            } else {
                addresses[i] = new InetSocketAddress(host, connector.getLocalPort());
            }
        }
        return addresses;
    }

    private ErrorHandler createErrorHandler() {
        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks(false);
        errorHandler.setTracedBodyFilter(System.getProperty("org.apache.knox.gateway.trace.body.status.filter"));
        return errorHandler;
    }

    private WebAppContext createWebAppContext(Topology topology, File warFile, String warPath) throws IOException, ZipException, TransformerException, SAXException, ParserConfigurationException {
        String topoName = topology.getName();
        WebAppContext context = new WebAppContext();
        String contextPath;
        contextPath = "/" + Urls.trimLeadingAndTrailingSlashJoin(config.getGatewayPath(), topoName, warPath);
        context.setContextPath(contextPath);
        context.setWar(warFile.getAbsolutePath());
        context.setAttribute(GatewayServices.GATEWAY_CLUSTER_ATTRIBUTE, topoName);
        context.setAttribute("org.apache.knox.gateway.frontend.uri", getFrontendUri(context, config));
        context.setAttribute(GatewayConfig.GATEWAY_CONFIG_ATTRIBUTE, config);
//        // Add support for JSPs.
//        context.setAttribute(
//                "org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
//                ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$|.*/[^/]*taglibs.*\\.jar$" );
        context.setTempDirectory(FileUtils.getFile(warFile, "META-INF", "temp"));
        context.setErrorHandler(createErrorHandler());
        return context;
    }

    private static void explodeWar(File source, File target) throws IOException, ZipException {
        if (source.isDirectory()) {
            FileUtils.copyDirectory(source, target);
        } else {
            ZipFile zip = new ZipFile(source);
            zip.extractAll(target.getAbsolutePath());
        }
    }

//    private void mergeWebXmlOverrides(File webInfDir) throws IOException, SAXException, ParserConfigurationException, TransformerException {
//        File webXmlFile = new File(webInfDir, "web.xml");
//        Document webXmlDoc;
//        if (webXmlFile.exists()) {
//            // Backup original web.xml file.
//            File originalWebXmlFile = new File(webInfDir, "original-web.xml");
//            FileUtils.copyFile(webXmlFile, originalWebXmlFile);
//            webXmlDoc = XmlUtils.readXml(webXmlFile);
//        } else {
//            webXmlDoc = XmlUtils.createDocument();
//            webXmlDoc.appendChild(webXmlDoc.createElement("web-app"));
//        }
//        File overrideWebXmlFile = new File(webInfDir, "override-web.xml");
//        if (overrideWebXmlFile.exists()) {
//            Document overrideWebXmlDoc = XmlUtils.readXml(overrideWebXmlFile);
//            Element originalRoot = webXmlDoc.getDocumentElement();
//            Element overrideRoot = overrideWebXmlDoc.getDocumentElement();
//            NodeList overrideNodes = overrideRoot.getChildNodes();
//            for (int i = 0, n = overrideNodes.getLength(); i < n; i++) {
//                Node overrideNode = overrideNodes.item(i);
//                if (overrideNode.getNodeType() == Node.ELEMENT_NODE) {
//                    Node importedNode = webXmlDoc.importNode(overrideNode, true);
//                    originalRoot.appendChild(importedNode);
//                }
//            }
//            XmlUtils.writeXml(webXmlDoc, webXmlFile);
//        }
//    }

//    private synchronized void internalDeployApplications(Topology topology, File topoDir) throws IOException, ZipException, ParserConfigurationException, TransformerException, SAXException {
//        if (topology != null) {
//            Collection<Application> applications = topology.getApplications();
//            if (applications != null) {
//                for (Application application : applications) {
//                    List<String> urls = application.getUrls();
//                    if (urls == null || urls.isEmpty()) {
//                        internalDeployApplication(topology, topoDir, application, application.getName());
//                    } else {
//                        for (String url : urls) {
//                            internalDeployApplication(topology, topoDir, application, url);
//                        }
//                    }
//                }
//            }
//        }
//    }

//    private synchronized void internalDeployApplication(Topology topology, File topoDir, Application application, String url) throws IOException, ZipException, TransformerException, SAXException, ParserConfigurationException {
//        File appsDir = new File(config.getGatewayApplicationsDir());
//        File appDir = new File(appsDir, application.getName());
//        if (!appDir.exists()) {
//            appDir = new File(appsDir, application.getName() + ".war");
//        }
//        if (!appDir.exists()) {
//            throw new DeploymentException("Application archive does not exist: " + appDir.getAbsolutePath());
//        }
//        File warDir = new File(topoDir, Urls.encode("/" + Urls.trimLeadingAndTrailingSlash(url)));
//        File webInfDir = new File(warDir, "WEB-INF");
//        explodeWar(appDir, warDir);
//        mergeWebXmlOverrides(webInfDir);
//        createArchiveTempDir(warDir);
//    }

    private synchronized void internalActivateTopology(Topology topology, File topoDir) throws IOException, ZipException, ParserConfigurationException, TransformerException, SAXException {
        log.activatingTopology(topology.getName());
        File[] files = topoDir.listFiles(new RegexDirFilter("%.*"));
        if (files != null) {
            for (File file : files) {
                internalActivateArchive(topology, file);
            }
        }
    }

    private synchronized void internalActivateArchive(Topology topology, File warDir) throws IOException, ZipException, ParserConfigurationException, TransformerException, SAXException {
        log.activatingTopologyArchive(topology.getName(), warDir.getName());
        try {
            WebAppContext newContext = createWebAppContext(topology, warDir, Urls.decode(warDir.getName()));
            WebAppContext oldContext = deployments.get(newContext.getContextPath());
            deployments.put(newContext.getContextPath(), newContext);
            if (oldContext != null) {
                contexts.removeHandler(oldContext);
            }
            contexts.addHandler(newContext);
            if (contexts.isRunning() && !newContext.isRunning()) {
                newContext.start();
            }
        } catch (Exception e) {
            auditor.audit(Action.DEPLOY, topology.getName(), ResourceType.TOPOLOGY, ActionOutcome.FAILURE);
            log.failedToDeployTopology(topology.getName(), e);
        }
    }

    private synchronized void internalDeactivateTopology(Topology topology) {

        log.deactivatingTopology(topology.getName());

        String topoName = topology.getName();
        String topoPath = "/" + Urls.trimLeadingAndTrailingSlashJoin(config.getGatewayPath(), topoName);
        String topoPathSlash = topoPath + "/";

        ServiceRegistry sr = getGatewayServices().getService(GatewayServices.SERVICE_REGISTRY_SERVICE);
        if (sr != null) {
            sr.removeClusterServices(topoName);
        }

        // Find all the deployed contexts we need to deactivate.
        List<WebAppContext> deactivate = new ArrayList<WebAppContext>();
        if (deployments != null) {
            for (WebAppContext app : deployments.values()) {
                String appPath = app.getContextPath();
                if (appPath.equals(topoPath) || appPath.startsWith(topoPathSlash)) {
                    deactivate.add(app);
                }
            }
        }
        // Deactivate the required deployed contexts.
        for (WebAppContext context : deactivate) {
            String contextPath = context.getContextPath();
            deployments.remove(contextPath);
            contexts.removeHandler(context);
            try {
                context.stop();
            } catch (Exception e) {
                auditor.audit(Action.UNDEPLOY, topology.getName(), ResourceType.TOPOLOGY, ActionOutcome.FAILURE);
                log.failedToUndeployTopology(topology.getName(), e);
            }
        }
        deactivate.clear();

    }

    // Using an inner class to hide the handleTopologyEvent method from consumers of GatewayServer.
    private class InternalTopologyListener implements TopologyListener {

        @Override
        public void handleTopologyEvent(List<TopologyEvent> events) {
            synchronized (GatewayServer.this) {
                for (TopologyEvent event : events) {
                    Topology topology = event.getTopology();
                    File deployDir = calculateAbsoluteDeploymentsDir();
                    if (event.getType().equals(TopologyEvent.Type.DELETED)) {
                        handleDeleteDeployment(topology, deployDir);
                    } else {
                        handleCreateDeployment(topology, deployDir);
                    }
                }
            }
        }

        private void handleDeleteDeployment(Topology topology, File deployDir) {
            log.deletingTopology(topology.getName());
            File[] files = deployDir.listFiles(new RegexDirFilter(topology.getName() + "\\.(war|topo)\\.[0-9A-Fa-f]+"));
            if (files != null) {
                auditor.audit(Action.UNDEPLOY, topology.getName(), ResourceType.TOPOLOGY,
                        ActionOutcome.UNAVAILABLE);
                internalDeactivateTopology(topology);
                for (File file : files) {
                    log.deletingDeployment(file.getAbsolutePath());
                    FileUtils.deleteQuietly(file);
                }
            }
        }

        private void handleCreateDeployment(Topology topology, File deployDir) {
            try {
                File topoDir = calculateDeploymentDir(topology);
                if (!topoDir.exists()) {
                    auditor.audit(Action.DEPLOY, topology.getName(), ResourceType.TOPOLOGY, ActionOutcome.UNAVAILABLE);

//          KNOX-564 - Topology should fail to deploy with no providers configured.
//TODO:APPS:This should only fail if there are services in the topology.
                    if (topology.getProviders().isEmpty()) {
                        throw new DeploymentException("No providers found inside topology.");
                    }

                    log.deployingTopology(topology.getName(), topoDir.getAbsolutePath());
                    internalDeactivateTopology(topology); // KNOX-152

                    EnterpriseArchive ear = DeploymentFactory.createDeployment(config, topology);
                    if (!deployDir.exists()) {
                        deployDir.mkdirs();
                        if (!deployDir.exists()) {
                            throw new DeploymentException("Failed to create topology deployment temporary directory: " + deployDir.getAbsolutePath());
                        }
                    }
                    File tmp = ear.as(ExplodedExporter.class).exportExploded(deployDir, topoDir.getName() + ".tmp");
                    if (!tmp.renameTo(topoDir)) {
                        FileUtils.deleteQuietly(tmp);
                        throw new DeploymentException("Failed to create topology deployment directory: " + topoDir.getAbsolutePath());
                    }
//                    internalDeployApplications(topology, topoDir);
                    internalActivateTopology(topology, topoDir);
                    log.deployedTopology(topology.getName());
                } else {
                    auditor.audit(Action.REDEPLOY, topology.getName(), ResourceType.TOPOLOGY, ActionOutcome.UNAVAILABLE);
                    log.redeployingTopology(topology.getName(), topoDir.getAbsolutePath());
                    internalActivateTopology(topology, topoDir);
                    log.redeployedTopology(topology.getName());
                }
                cleanupTopologyDeployments(deployDir, topology);
            } catch (Throwable e) {
                auditor.audit(Action.DEPLOY, topology.getName(), ResourceType.TOPOLOGY, ActionOutcome.FAILURE);
                log.failedToDeployTopology(topology.getName(), e);
            }
        }

    }

    private File createArchiveTempDir(File warDir) {
        File tempDir = FileUtils.getFile(warDir, "META-INF", "temp");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
            if (!tempDir.exists()) {
                throw new DeploymentException("Failed to create archive temporary directory: " + tempDir.getAbsolutePath());
            }
        }
        return tempDir;
    }

    private static File calculateAbsoluteTopologiesDir(GatewayConfig config) {
        File topoDir = new File(config.getGatewayTopologyDir());
        topoDir = topoDir.getAbsoluteFile();
        return topoDir;
    }

    private static File calculateAbsoluteDeploymentsDir(GatewayConfig config) {
        File deployDir = new File(config.getGatewayDeploymentDir());
        deployDir = deployDir.getAbsoluteFile();
        return deployDir;
    }

    private File calculateAbsoluteTopologiesDir() {
        return calculateAbsoluteTopologiesDir(config);
    }

    private File calculateAbsoluteDeploymentsDir() {
        return calculateAbsoluteDeploymentsDir(config);
    }

    private File calculateDeploymentDir(Topology topology) {
        File dir = new File(calculateAbsoluteDeploymentsDir(), calculateDeploymentName(topology));
        return dir;
    }

    private String calculateDeploymentExtension(Topology topology) {
        return ".topo.";
    }

    private String calculateDeploymentName(Topology topology) {
        String name = topology.getName() + calculateDeploymentExtension(topology) + Long.toHexString(topology.getTimestamp());
        return name;
    }

    private static void checkAddressAvailability(InetSocketAddress address) throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(address);
        socket.close();
    }

    private class RegexDirFilter implements FilenameFilter {

        Pattern pattern;

        RegexDirFilter(String regex) {
            pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(File dir, String name) {
            return pattern.matcher(name).matches();
        }
    }

    public URI getFrontendUri(WebAppContext context, GatewayConfig config) {
        URI frontendUri = null;
        String frontendStr = config.getFrontendUrl();
        if (frontendStr != null && !frontendStr.trim().isEmpty()) {
            String topoName = (String) context.getAttribute(GatewayServices.GATEWAY_CLUSTER_ATTRIBUTE);
            try {
                frontendStr = frontendStr.trim();
                if (frontendStr.endsWith("/")) {
                    frontendUri = new URI(frontendStr + topoName);
                } else {
                    frontendUri = new URI(frontendStr + "/" + topoName);
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return frontendUri;
    }

    private static class FileModificationTimeDescendingComparator implements Comparator<File> {
        @Override
        public int compare(File left, File right) {
            long leftTime = (left == null ? Long.MIN_VALUE : left.lastModified());
            long rightTime = (right == null ? Long.MIN_VALUE : right.lastModified());
            if (leftTime > rightTime) {
                return -1;
            } else if (leftTime < rightTime) {
                return 1;
            } else {
                return 0;
            }
        }
    }

}