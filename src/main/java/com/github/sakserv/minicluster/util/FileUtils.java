package com.github.sakserv.minicluster.util;

import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by skumpf on 12/31/14.
 */
public class FileUtils {

    private static final Logger LOG = Logger.getLogger(FileUtils.class);

    public static void deleteFolder(String directory) {
        File directoryToClean = new File(directory);
        String directoryAbsPath = directoryToClean.getAbsolutePath();

        LOG.info("FILEUTILS: Deleting contents of directory: " + directoryAbsPath);

        File[] files = directoryToClean.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f.getAbsolutePath());
                } else {
                    LOG.info("FILEUTILS: Deleting file: " + f.getAbsolutePath());
                    f.delete();
                }
            }
        }
        directoryToClean.delete();
    }
}
