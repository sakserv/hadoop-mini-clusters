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
package com.github.sakserv.minicluster.yarn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;

import com.github.sakserv.minicluster.yarn.util.EnvironmentUtils;
import com.github.sakserv.minicluster.yarn.util.ExecJavaCliParser;
import com.github.sakserv.minicluster.yarn.util.ExecShellCliParser;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;

/**
 * !!!!! FOR TESTING WITH MINI CLUSTER ONLY !!!!!
 *
 * Container executor which will launch Java containers in the same JVM.
 *
 * In order to use it you must override
 * 'yarn.nodemanager.container-executor.class' property in the server
 * configuration (e.g., mini cluster) and set it to the fully qualified name of
 * this class
 *
 * @author Oleg Zhurakousky
 *
 */
public class InJvmContainerExecutor extends DefaultContainerExecutor {

    private static final Log logger = LogFactory.getLog(InJvmContainerExecutor.class);
    

    /**
     * Will construct the instance of this {@link ContainerExecutor} and will
     * install a {@link SystemExitDisallowingSecurityManager} which will help with
     * managing the life-cycle of the containers that contain System.exit calls.
     */
    public InJvmContainerExecutor() {
        logger.info("Adding SystemExitDisallowingSecurityManager");
        System.setSecurityManager(new SystemExitDisallowingSecurityManager());
    }

  /**
   * Overrides the parent method while still invoking it. Since
   * {@link #isContainerActive(ContainerId)} method is also overridden here and
   * always returns 'false' the super.launchContainer(..) will only go through
   * the prep routine (e.g., creating temp dirs etc.) while never launching the
   * actual container via the launch script. This will ensure that all the
   * expectations of the container to be launched (e.g., symlinks etc.) are
   * satisfied. The actual launch will be performed by invoking
   * {@link #doLaunch(Container, Path)} method.
   */
  public int launchContainer(ContainerStartContext containerStartContext) throws IOException {
    Container container = containerStartContext.getContainer();
    Path containerWorkDir = containerStartContext.getContainerWorkDir();
    super.launchContainer(containerStartContext);
    int exitCode = 0;
    if (container.getLaunchContext().getCommands().toString().contains("bin/java")) {
      ExecJavaCliParser result = this.createExecCommandParser(containerWorkDir.toString());
      try {
        exitCode = this.doLaunch(container, containerWorkDir);
        if (logger.isInfoEnabled()) {
          logger.info(("Returned: " + exitCode));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      String cmd = container.getLaunchContext().getCommands().get(0);
      if (logger.isInfoEnabled()) {
        logger.info("Running Command: " + cmd);
      }
      ExecShellCliParser execShellCliParser = new ExecShellCliParser(cmd);
      try {
        exitCode = execShellCliParser.runCommand();
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (logger.isInfoEnabled()) {
        logger.info(("Returned: " + exitCode));
      }
    }
    return exitCode;
  }

    /**
     * Overrides the parent method while still invoking it. Since
     * {@link #isContainerActive(ContainerId)} method is also overridden here and
     * always returns 'false' the super.launchContainer(..) will only go through
     * the prep routine (e.g., creating temp dirs etc.) while never launching the
     * actual container via the launch script. This will ensure that all the
     * expectations of the container to be launched (e.g., symlinks etc.) are
     * satisfied. The actual launch will be performed by invoking
     * {@link #doLaunch(Container, Path)} method.
     */
    public int launchContainer(Container container,
                               Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
                               String userName, String appId, Path containerWorkDir,
                               List<String> localDirs, List<String> logDirs) throws IOException {
      ContainerStartContext containerStartContext = new ContainerStartContext
          .Builder().setContainer(container)
          .setLocalizedResources(container.getLocalizedResources())
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setUser(userName)
          .setAppId(appId)
          .setContainerWorkDir(containerWorkDir)
          .setLocalDirs(localDirs)
          .setLocalDirs(logDirs).build();

        super.launchContainer(containerStartContext);
        int exitCode = 0;
        if (container.getLaunchContext().getCommands().toString().contains("bin/java")) {
            ExecJavaCliParser result = this.createExecCommandParser(containerWorkDir.toString());
                try {
                    exitCode = this.doLaunch(container, containerWorkDir);
                    if (logger.isInfoEnabled()) {
                        logger.info(("Returned: " + exitCode));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
        } else {
            String cmd = container.getLaunchContext().getCommands().get(0);
            if (logger.isInfoEnabled()) {
                logger.info("Running Command: " + cmd);
            }
            ExecShellCliParser execShellCliParser = new ExecShellCliParser(cmd);
            try {
                exitCode = execShellCliParser.runCommand();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (logger.isInfoEnabled()) {
                logger.info(("Returned: " + exitCode));
            }
        }
        return exitCode;
    }

    /**
     * This is to ensure that call to super.launchContainer(..) doesn't actually
     * execute anything other then prep work (e.g., sets up directories etc.)
     */
    @Override
    protected boolean isContainerActive(ContainerId containerId) {
        return false;
    }

    /**
     * Will launch containers within the same JVM as this Container Executor. It
     * will do so by: - extracting Container's class name and program arguments
     * from the launch script (e.g., launch_container.sh) - Creating an isolated
     * ClassLoader for each container - Calling doLaunchContainer(..) method to
     * launch Container
     */
    private int doLaunch(Container container, Path containerWorkDir) throws Exception {
        Map<String, String> environment = container.getLaunchContext().getEnvironment();
        EnvironmentUtils.putAll(environment);

        Set<URL> additionalClassPathUrls = this.filterAndBuildUserClasspath(container);

        ExecJavaCliParser javaCliParser = this.createExecCommandParser(containerWorkDir.toString());

        UserGroupInformation.setLoginUser(null);
        try {
            // create Isolated Class Loader for each container and set it as context
            // class loader
            URLClassLoader containerCl =
                    new URLClassLoader(additionalClassPathUrls.toArray(additionalClassPathUrls.toArray(new URL[]{})), null);
            Thread.currentThread().setContextClassLoader(containerCl);
            String containerLauncher = javaCliParser.getMain();


            Class<?> containerClass = Class.forName(containerLauncher, true, containerCl);
            Method mainMethod = containerClass.getMethod("main", new Class[] { String[].class });
            mainMethod.setAccessible(true);
            String[] arguments = javaCliParser.getMainArguments();

            this.doLaunchContainer(containerClass, mainMethod, arguments);

        }
        catch (Exception e) {
            logger.error("Failed to launch container " + container, e);
            container.handle(new ContainerDiagnosticsUpdateEvent(container.getContainerId(), e.getMessage()));
            return -1;
        }
        finally {
            logger.info("Removing symlinks");
            this.cleanUp();
        }
        return 0;
    }

    /**
     * Will invoke Container's main method blocking if necessary. This method
     * contains a hack that I am not proud of it, but given the fact that some
     * containers rely on System.exit to manage its life-cycle instead of proper
     * exit this will ensure that together with the
     * SystemExitDisallowingSecurityManager (see constructor of this class) this
     * method will block until such container invokes System.exit
     *
     * ByteCodeUtils.hasSystemExit(..) will check if a container that was invoked
     * has calls to System.exit and if it does it will block this thread until
     * SystemExitException is thrown which will be caught allowing this method to
     * exit normally.
     *
     * Of course this doesn't guarantee anything since the underlying
     * implementation of the container may still be implemented in such way where
     * it exits gracefully while also has some shortcut method for some
     * exceptional conditions where System.exit is called and if that's the case
     * this process will block infinitely.
     *
     * The bottom line: DON'T USE System.exit when implementing application
     * containers!!!
     */
    private void doLaunchContainer(Class<?> containerClass, Method mainMethod, String[] arguments) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("Launching container for " + containerClass.getName()
                    + " with arguments: " + Arrays.asList(arguments));
        }

        try {
            mainMethod.invoke(null, (Object) arguments);
            logger.info("Keeping " + containerClass.getName() + " process alive");
            LockSupport.park();
        }
        catch (SystemExitException e) {
            logger.warn("Ignoring System.exit(..) call in " + containerClass.getName());
        }
        if (logger.isInfoEnabled()) {
            logger.warn("Container " + containerClass.getName() + " is finished");
        }
    }

    /**
     * YARN provides ability to pass resources (e.g., classpath) through
     * {@link LocalResource}s which allows user to provision all the resources
     * required to run the app. This method will extract those resources as a
     * {@link Set} of {@link URL}s so they are used when {@link ClassLoader} for a
     * container is created.
     *
     * This is done primarily as a convenience for applications that rely on
     * automatic classpath propagation (e.g., pull everything from my dev
     * classpath) instead of manual.
     *
     * @param container
     * @return
     */
    private Set<URL> filterAndBuildUserClasspath(Container container) {
        if (logger.isDebugEnabled()) {
            logger.debug("Building additional classpath for the container: " + container);
        }
        Set<URL> additionalClassPathUrls = new HashSet<URL>();
        Set<Path> userClassPath =
                this.extractUserProvidedClassPathEntries(container);

        for (Path resourcePath : userClassPath) {
            String resourceName = "file:///" + new File(resourcePath.getName()).getAbsolutePath();
            if (logger.isDebugEnabled()) {
                logger.debug("\t adding " + resourceName + " to the classpath");
            }
            try {
                additionalClassPathUrls.add(new URI(resourceName).toURL());
            }
            catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        return additionalClassPathUrls;
    }

    /**
     * Creates CLI parser which can be used to extract Container's class name and
     * its launch arguments.
     *
     * @param containerWorkDir
     * @return
     */
    private ExecJavaCliParser createExecCommandParser(String containerWorkDir) {
        String execLine =
                this.filterAndExecuteLaunchScriptAndReturnExecLine(containerWorkDir);
        String[] values = execLine.split("\"");
        String javaCli = values[1];
        String[] javaCliValues = javaCli.split(" ");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < javaCliValues.length; i++) {
            if (i > 0) {
                buffer.append(javaCliValues[i]);
                if (javaCliValues.length - i > 1) {
                    buffer.append(" ");
                }
            }
        }
        String extractedJavaCli = buffer.toString();
        ExecJavaCliParser execJavaCliParser = new ExecJavaCliParser(extractedJavaCli);
        return execJavaCliParser;
    }

    /**
     * This method does three things 1. It creates an updated version of the
     * initial launch script where it simply copies its contents less the 'exec'
     * line 2. It extract the 'exec' line and returns it so the Container's class
     * name and launch arguments could be retrieved. 3. It executes the
     * 'exec'-less launch script to ensure that all symlinks and other prepwork
     * expected by the underlying container is performed.
     *
     * @param containerWorkDir
     * @return
     */
    private String filterAndExecuteLaunchScriptAndReturnExecLine(String containerWorkDir) {
        BufferedReader reader = null;
        BufferedWriter writer = null;
        String execLine = null;
        File inJvmlaunchScript = null;
        try {
            File launchScript = new File(containerWorkDir, "launch_container.sh");
            inJvmlaunchScript =
                    new File(containerWorkDir.toString(), "injvm_launch_container.sh");
            inJvmlaunchScript.setExecutable(true);
            reader = new BufferedReader(new FileReader(launchScript));
            writer = new BufferedWriter(new FileWriter(inJvmlaunchScript));

            String line;
              while ((line = reader.readLine()) != null) {
                if (line.startsWith("exec")) {
                  execLine = line;
                } else if (line.startsWith("cp \"launch_container.sh\"")) {
                  String[] lineParts = line.split(" ");
                  writer.write(lineParts[0]);
                  writer.write(" \"" + containerWorkDir + "/launch_container.sh\" ");
                  writer.write(lineParts[2]);
                  writer.write("\n");
                } else {
                  writer.write(line);
                  writer.write("\n");
                }
            }
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to override default launch script", e);
        }
        finally {
            try {
                reader.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                writer.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (inJvmlaunchScript != null) {
            try {
                inJvmlaunchScript.setExecutable(true);
                Process process =
                        Runtime.getRuntime().exec(inJvmlaunchScript.getAbsolutePath());
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new IllegalStateException(
                            "Failed to execute launch script.  Exit code: " + exitCode);
                }
            }
            catch (Exception e) {
                throw new IllegalStateException("Failed to execute "
                        + inJvmlaunchScript.getAbsolutePath(), e);
            }
        }
        return execLine;
    }

    /**
     * Extracts {@link LocalResource}s from the {@link Container}.
     */
    @SuppressWarnings("unchecked")
    private Set<Path> extractUserProvidedClassPathEntries(Container container) {
        Map<Path, List<String>> localizedResources;
        try {
            Field lf = container.getClass().getDeclaredField("localizedResources");
            lf.setAccessible(true);
            localizedResources = (Map<Path, List<String>>) lf.get(container);
            Set<Path> paths = localizedResources.keySet();
            // Needed for Tez
            for (Path path : paths) {
                if (path.toString().endsWith("tez-conf.pb") || path.toString().endsWith("tez-dag.pb")){
                    File sourceFile = new File(path.toUri());

                    File targetFile = new File(System.getenv(Environment.PWD.name()) + "/" + sourceFile.getName());
                    FileUtils.copyFile(sourceFile, targetFile);

//			System.out.println("######## Copied file: " + targetFile);
//			FileInputStream fis = new FileInputStream(new File(System.getenv(Environment.PWD.name()), targetFile.getName()));
//			System.out.println(fis.available());
//			fis.close();
//			break;
                }
            }
            return paths;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will clean up symlinks that were created by a launch script
     */
    private void cleanUp() {
        try {
            File file = new File(System.getProperty("user.dir"));
            String[] links = file.list();
            for (String name : links) {
                File potentialSymLink = new File(file, name);
                if (FileUtils.isSymlink(potentialSymLink)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("DELETING: " + potentialSymLink);
                    }
                    potentialSymLink.delete();
                }
            }
        }
        catch (Exception e) {
            logger.warn("Failed to remove symlinks", e);
        }
    }

    /**
     * An implementation of the {@link SecurityManager} which can be used to
     * intercept System.exit. This implementation will simply throw an exception
     * when such call is made essentially making such call ineffective.
     *
     * It is used by this class to intercept System.exit calls made by some
     * implementations of YARN containers (e.g., Tez's DAGAppMaster). Since this
     * container executor will use the same JVM its running in to start those
     * containers any System.exit call will shut down the entire cluster. Using
     * such {@link SecurityManager} would allow such calls to be intercepted by
     * catching {@link SystemExitException}.
     */
    private static class SystemExitDisallowingSecurityManager extends
            SecurityManager {
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
            throw new SystemExitException();
        }
    }

    /**
     * An implementation of the {@link SecurityManager} which can be used to
     * intercept System.exit. This implementation will simply throw an exception
     * when such call is made essentially making such call ineffective.
     *
     * It is used by this class to intercept System.exit calls made by some
     * implementations of YARN containers (e.g., Tez's DAGAppMaster). Since this
     * container executor will use the same JVM its running in to start those
     * containers any System.exit call will shut down the entire cluster. Using
     * such {@link SecurityManager} would allow such calls to be intercepted by
     * catching {@link SystemExitException}.
     */
    public static class SystemExitAllowSecurityManager extends
            SecurityManager {
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
}