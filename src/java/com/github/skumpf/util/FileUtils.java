package com.github.skumpf.util;

import java.io.File;

/**
 * Created by skumpf on 12/31/14.
 */
public class FileUtils {

    public static void deleteFolder(String directory) {
        File directoryToClean = new File(directory);
        String directoryAbsPath = directoryToClean.getAbsolutePath();

        System.out.println("FILEUTILS: Deleting contents of directory: " + directoryAbsPath);

        File[] files = directoryToClean.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f.getAbsolutePath());
                } else {
                    System.out.println("FILEUTILS: Deleting file: " + f.getAbsolutePath());
                    f.delete();
                }
            }
        }
        directoryToClean.delete();
    }
}
