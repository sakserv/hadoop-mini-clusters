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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    // Proxy properties
    private static final String PROXY_PROPERTY_NAME = "HTTP_PROXY";
    private static final String ALL_PROXY_PROPERTY_NAME = "ALL_PROXY";

    public static void downloadFileWithProgress(String fileUrl, String outputFilePath) throws IOException {
        String fileName = fileUrl.substring(fileUrl.lastIndexOf('/') + 1);
        URL url = new URL(fileUrl);
        HttpURLConnection httpURLConnection;

        //Check if system proxy is set
        Proxy proxySettings = returnProxyIfEnabled();
        if (proxySettings != null) {
            httpURLConnection = (HttpURLConnection) (url.openConnection(proxySettings));
        } else {
            httpURLConnection = (HttpURLConnection) (url.openConnection());
        }
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
        while ((x = bufferedInputStream.read(data, 0, 1024)) >= 0) {
            downloadedFileSize += x;

            final int currentProgress = (int) (((double) downloadedFileSize / (double) fileSize) * 100d);
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

    public static Proxy returnProxyIfEnabled() {
        LOG.debug("returnProxyIfEnabled() start!!");
        String proxyStarturl = "http://";

        String proxyURLString = System.getProperty(PROXY_PROPERTY_NAME) != null ? System.getProperty(PROXY_PROPERTY_NAME)
                : System.getProperty(PROXY_PROPERTY_NAME.toLowerCase());
        String allproxyURLString = System.getProperty(ALL_PROXY_PROPERTY_NAME) != null
                ? System.getProperty(ALL_PROXY_PROPERTY_NAME) : System.getProperty(ALL_PROXY_PROPERTY_NAME.toLowerCase());
        //Pick PROXY URL from two widely used system properties
        String finalProxyString = proxyURLString != null ? proxyURLString : allproxyURLString;
        URL proxyURL = null;

        try {
            //If Proxy URL starts with HTTP then use HTTP PROXY settings
            if (finalProxyString != null && finalProxyString.toLowerCase().startsWith(proxyStarturl)) {
                // Basic method to validate proxy URL is correct or not.
                proxyURL = returnParsedURL(finalProxyString);
                LOG.debug("protocol of proxy used is: " + proxyURL.getProtocol());
                return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyURL.getHost(), proxyURL.getPort()));
            //If Proxy URL starts with no protocol then assume it is HTTP
            } else if (finalProxyString != null && !finalProxyString.contains("://")
                    && finalProxyString.split(":").length == 2) {

                LOG.debug("protocol of proxy used is: http default");
                proxyURL = returnParsedURL(proxyStarturl.concat(finalProxyString));
                return proxyURL != null ? new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyURL.getHost(), proxyURL.getPort())) : null;
            //If Proxy URL starts with SOCKS4 or SOCKS5 protocol then go for SOCKS settings
            } else if (finalProxyString != null && finalProxyString.toLowerCase().startsWith("sock")
                    && finalProxyString.split("://").length == 2) {
                LOG.debug("protocol of proxy used is: Socks");
                proxyURL = returnParsedURL(proxyStarturl.concat(finalProxyString.split("://")[1]));
                return proxyURL != null ? new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyURL.getHost(), proxyURL.getPort())) : null;
            }
        } catch (MalformedURLException | URISyntaxException mUE) {
            LOG.error("Can not configure Proxy because URL {} is incorrect: " + mUE, finalProxyString);
        }

        return null;
    }

    private static URL returnParsedURL(String urlString) throws MalformedURLException, URISyntaxException {
        if (urlString != null) {
            URL url = new URL(urlString);
            url.toURI();
            LOG.info("System has been set to use proxy. Hence, configuring proxy URL: {}", urlString);
            return url;
        }
        return null;
    }
}
