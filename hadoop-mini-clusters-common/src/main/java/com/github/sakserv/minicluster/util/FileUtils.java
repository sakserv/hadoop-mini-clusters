package com.github.sakserv.minicluster.util;

import org.slf4j.LoggerFactory;

import java.io.File;


public class FileUtils {

    // Logger
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void deleteFolder(String directory) {
        File directoryToClean = new File(directory);
        String directoryAbsPath = directoryToClean.getAbsolutePath();

        LOG.info("FILEUTILS: Deleting contents of directory: {}", directoryAbsPath);

        File[] files = directoryToClean.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    f.setWritable(true);
                    deleteFolder(f.getAbsolutePath());
                } else {
                    LOG.info("FILEUTILS: Deleting file: {}", f.getAbsolutePath());
                    f.setWritable(true);
                    f.delete();
                }
            }
        }
        LOG.info("FILEUTILS: Deleting file: {}", directoryToClean.getAbsolutePath());
        directoryToClean.setWritable(true);
        directoryToClean.delete();
    }

    @Override
    public String toString() {
        return "FileUtils";
    }
}
