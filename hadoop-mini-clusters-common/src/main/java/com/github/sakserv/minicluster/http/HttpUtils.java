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
package com.github.sakserv.minicluster.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static void downloadFileWithProgress(String fileUrl, String outputFilePath) throws IOException {
        String fileName =  fileUrl.substring(fileUrl.lastIndexOf('/') + 1);
        URL url = new URL(fileUrl);
        HttpURLConnection httpURLConnection = (HttpURLConnection) (url.openConnection());
        long fileSize = httpURLConnection.getContentLength();

        // Create the parent output directory if it doesn't exis
        if (!new File(outputFilePath).getParentFile().isDirectory()) {
            new File(outputFilePath).getParentFile().mkdirs();
        }

        BufferedInputStream bufferedInputStream = new BufferedInputStream(httpURLConnection.getInputStream());
        FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream, 1024);

        byte[] data = new byte[1024];
        long downloadedFileSize = 0;

        Integer previousProgress = 0;
        int x = 0;
        while((x = bufferedInputStream.read(data, 0, 1024)) >= 0) {
            downloadedFileSize  += x;

            final int currentProgress = (int) (((double)downloadedFileSize / (double)fileSize) * 100d);
            if (!previousProgress.equals(currentProgress)) {
                LOG.info("HTTP: Download Status: Filename {} - {}% ({}/{})", fileName, currentProgress,
                        downloadedFileSize, fileSize);
                previousProgress = currentProgress;
            }

            bufferedOutputStream.write(data, 0, x);
        }
        bufferedOutputStream.close();
        bufferedInputStream.close();
    }

}
