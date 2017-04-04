package com.github.sakserv.minicluster.util;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class FileUtils {

    // Logger
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void deleteFolder(String directory) {
      try {
        Path directoryPath = Paths.get(directory).toAbsolutePath();
        LOG.info("FILEUTILS: Deleting contents of directory: {}",
            directoryPath.toAbsolutePath().toString());
        Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            LOG.info("Removing file: {}", file.toAbsolutePath().toString());
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc)
              throws IOException {
            Files.delete(dir);
            LOG.info("Removing directory: {}", dir.toAbsolutePath().toString());
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException e) {
        LOG.error("FILEUTILS: Unable to remove {}", directory);
      }
    }

    @Override
    public String toString() {
        return "FileUtils";
    }
}
