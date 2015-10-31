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
package com.github.sakserv.minicluster.oozie.util;

import com.github.sakserv.minicluster.http.HttpUtils;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OozieShareLibUtil {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieShareLibUtil.class);

    // Setup the property parser
    private static final String PROP_FILE = "sharelib.properties";
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(PROP_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", PROP_FILE);
        }
    }

    // Constants
    private static final String SHARE_LIB_PREFIX = "lib_";
    private static final String SHARE_LIB_LOCAL_TEMP_PREFIX = "oozie_share_lib_tmp";

    // Instance variables
    String oozieHdfsShareLibDir;
    Boolean oozieShareLibCreate;
    String shareLibCacheDir;
    Boolean purgeLocalShareLibCache;
    FileSystem hdfsFileSystem;

    // Constructor
    public OozieShareLibUtil(String oozieHdfsShareLibDir, Boolean oozieShareLibCreate, String shareLibCacheDir,
                             Boolean purgeLocalShareLibCache, FileSystem hdfsFileSystem) {
        this.oozieHdfsShareLibDir = oozieHdfsShareLibDir;
        this.oozieShareLibCreate = oozieShareLibCreate;
        this.shareLibCacheDir = shareLibCacheDir;
        this.purgeLocalShareLibCache = purgeLocalShareLibCache;
        this.hdfsFileSystem = hdfsFileSystem;
    }

    // Main driver that downloads, extracts, and deploys the oozie sharelib
    public void createShareLib() {

        if (!oozieShareLibCreate) {
            LOG.info("OOZIE: Share Lib Create Disabled... skipping");
        } else {

            final String fullOozieTarFilePath = shareLibCacheDir + Path.SEPARATOR + getOozieTarFileName();

            try {

                // Get and extract the oozie release
                getOozieTarFileFromRepo();
                String oozieExtractTempDir = extractOozieTarFileToTempDir(new File(fullOozieTarFilePath));

                // Extract the sharelib tarball to a temp dir
                String fullOozieShareLibTarFilePath = oozieExtractTempDir + Path.SEPARATOR +
                        "oozie-" + getOozieVersionFromOozieTarFileName() + Path.SEPARATOR +
                        "oozie-sharelib-" + getOozieVersionFromOozieTarFileName() + ".tar.gz";
                String oozieShareLibExtractTempDir = extractOozieShareLibTarFileToTempDir(
                        new File(fullOozieShareLibTarFilePath));

                // Copy the sharelib into HDFS
                Path destPath = new Path(oozieHdfsShareLibDir + Path.SEPARATOR +
                        SHARE_LIB_PREFIX + getTimestampDirectory());
                LOG.info("OOZIE: Writing share lib contents to: {}", destPath);
                hdfsFileSystem.copyFromLocalFile(false, new Path(new File(oozieShareLibExtractTempDir).toURI()), destPath);

                if (purgeLocalShareLibCache) {
                    FileUtils.deleteDirectory(new File(shareLibCacheDir));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getTimestampDirectory() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date).toString();
    }

    public String getOozieTarFileUrl() {
        String version = System.getProperty("hdp.release.version");
        return propertyParser.getProperty(version + ".url");
    }

    public String getOozieTarFileName() {
        String version = System.getProperty("hdp.release.version");
        String url = propertyParser.getProperty(version + ".url");
        return url.substring(url.lastIndexOf('/') + 1);

    }

    public String getOozieVersionFromOozieTarFileName() {
        return getOozieTarFileName().replace("-distro.tar.gz","").replace("oozie-","");
    }

    public void getOozieTarFileFromRepo() throws IOException {

        final String fullOozieTarFilePath = shareLibCacheDir + Path.SEPARATOR + getOozieTarFileName();

        if(purgeLocalShareLibCache) {
            FileUtils.deleteDirectory(new File(shareLibCacheDir));
        }

        if (new File(fullOozieTarFilePath).exists()) {
            LOG.info("OOZIE: Found Oozie tarball in cache, skipping download: {}", fullOozieTarFilePath);
        } else {
            downloadOozieTarFileToLocalCacheDir();
        }
    }

    public void downloadOozieTarFileToLocalCacheDir() throws IOException {
        final String fullOozieTarFilePath = shareLibCacheDir + Path.SEPARATOR + getOozieTarFileName();
        HttpUtils.downloadFileWithProgress(getOozieTarFileUrl(), fullOozieTarFilePath);
    }

    public String extractOozieTarFileToTempDir(File fullOozieTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieTarFilePath, tempDir);

        return tempDir.getAbsolutePath();
    }

    public String extractOozieShareLibTarFileToTempDir(File fullOozieShareLibTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieShareLibTarFilePath, tempDir);

        return tempDir.getAbsolutePath();
    }

}
