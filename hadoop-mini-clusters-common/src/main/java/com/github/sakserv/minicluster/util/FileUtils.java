package com.github.sakserv.minicluster.util;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public final class FileUtils {

    // Logger
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void deleteFolder(String directory, boolean quietly) {
        try {
            Path directoryPath = Paths.get(directory).toAbsolutePath();
            if (!quietly) {
                LOG.info("FILEUTILS: Deleting contents of directory: {}",
                        directoryPath.toAbsolutePath().toString());
            }
            Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.delete(file);
                    if (!quietly) {
                        LOG.info("Removing file: {}", file.toAbsolutePath().toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                    Files.delete(dir);
                    if (!quietly) {
                        LOG.info("Removing directory: {}", dir.toAbsolutePath().toString());
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOG.error("FILEUTILS: Unable to remove {}", directory);
        }
    }

    public static void deleteFolder(String directory) {
        deleteFolder(directory, false);
    }

    @Override
    public String toString() {
        return "FileUtils";
    }
}
