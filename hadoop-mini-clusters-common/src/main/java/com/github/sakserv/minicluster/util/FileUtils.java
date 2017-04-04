package com.github.sakserv.minicluster.util;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;


public final class FileUtils {

    // Logger
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void deleteFolder(String directory) {
        File directoryToClean = null;
        try {
            directoryToClean = new File(new URI(directory).getPath());
        } catch (URISyntaxException e) {
            LOG.error("Directory is invalid for URI conversion: " + directory, e);
        }

        if (directoryToClean == null) {
          LOG.error("Directory to cleanup is null, skipping...");
        } else {
            LOG.info("FILEUTILS: Deleting contents of directory: {}", directoryToClean.getAbsolutePath());

            File[] files = directoryToClean.listFiles();
            if (files != null) { //some JVMs return null for empty dirs
                for (File f : files) {
                    if (f.isDirectory()) {
                        f.setWritable(true);
                        deleteFolder(f.getAbsolutePath());
                    } else {
                        LOG.debug("FILEUTILS: Deleting file: {}", f.getAbsolutePath());
                        f.setWritable(true);
                        f.delete();
                    }
                }
            }
            LOG.debug("FILEUTILS: Deleting file: {}", directoryToClean.getAbsolutePath());
            directoryToClean.setWritable(true);
            directoryToClean.delete();
        }
    }

    @Override
    public String toString() {
        return "FileUtils";
    }
}
