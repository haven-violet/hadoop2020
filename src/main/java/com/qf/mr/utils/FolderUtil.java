package com.qf.mr.utils;

import java.io.File;
 
public class FolderUtil {
    public static boolean deleteFolder(File file) {
        if (!file.exists()) {
            return false;
        }
 
        if (file.isFile()) {
            return file.delete();
        } else {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteFolder(file1);
            }
        }
        return file.delete();
    }
}