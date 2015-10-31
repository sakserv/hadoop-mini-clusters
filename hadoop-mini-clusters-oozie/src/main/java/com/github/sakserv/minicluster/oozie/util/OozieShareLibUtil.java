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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.tools.OozieSharelibCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OozieShareLibUtil {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieShareLibUtil.class);

    // Variables
    private File shareLibTarFile;

    // Constants
    private static final String SHARE_LIB_PREFIX = "lib_";
    private static final String SHARE_LIB_LOCAL_TEMP_PREFIX = "oozie_share_lib_tmp";


    public void createShareLib(String hdfsUri, FileSystem hdfsFileSystem) {

        shareLibTarFile = new File("./hadoop-mini-clusters-oozie/sharelib/2.3.2.0/oozie-sharelib.tar.gz");

        try {
            File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");

            LOG.info("OOZIE: Untar to {}", tempDir.getAbsoluteFile());
            tempDir.delete();
            tempDir.mkdir();
            tempDir.deleteOnExit();
            FileUtil.unTar(shareLibTarFile, tempDir);

            Path destPath = new Path(hdfsUri +  Path.SEPARATOR +  SHARE_LIB_PREFIX + getTimestampDirectory());
            LOG.info("OOZIE: Writing share lib contents to: {}", destPath);
            hdfsFileSystem.copyFromLocalFile(false, new Path(tempDir.toURI()), destPath);

            // Validate the share lib dir was created and contains a single directory
            FileStatus[] fileStatuses = hdfsFileSystem.listStatus(destPath);
            for (FileStatus fileStatus: fileStatuses) {
                LOG.info("OOZIE: Found in HDFS: {}", fileStatus);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getTimestampDirectory() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date).toString();
    }

}
