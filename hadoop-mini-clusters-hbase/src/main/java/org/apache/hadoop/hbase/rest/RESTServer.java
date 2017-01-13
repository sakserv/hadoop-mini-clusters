/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.rest.filter.AuthFilter;
import org.apache.hadoop.hbase.rest.filter.RestCsrfPreventionFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import java.util.*;
import java.util.Map.Entry;

/**
 * Main class for launching REST gateway as a servlet hosted by Jetty.
 * <p>
 * The following options are supported:
 * <ul>
 * <li>-p --port : service port</li>
 * <li>-ro --readonly : server mode</li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class RESTServer implements Constants {
    static Log LOG = LogFactory.getLog("RESTServer");

    static String REST_CSRF_ENABLED_KEY = "hbase.rest.csrf.enabled";
    static boolean REST_CSRF_ENABLED_DEFAULT = false;
    static boolean restCSRFEnabled = false;
    static String REST_CSRF_CUSTOM_HEADER_KEY = "hbase.rest.csrf.custom.header";
    static String REST_CSRF_CUSTOM_HEADER_DEFAULT = "X-XSRF-HEADER";
    static String REST_CSRF_METHODS_TO_IGNORE_KEY = "hbase.rest.csrf.methods.to.ignore";
    static String REST_CSRF_METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";

    /**
     * Returns a list of strings from a comma-delimited configuration value.
     *
     * @param conf configuration to check
     * @param name configuration property name
     * @param defaultValue default value if no value found for name
     * @return list of strings from comma-delimited configuration value, or an
     *     empty list if not found
     */
    public static List<String> getTrimmedStringList(Configuration conf,
                                                    String name, String defaultValue) {
        String valueString = conf.get(name, defaultValue);
        if (valueString == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(StringUtils.getTrimmedStringCollection(valueString));
    }

    public static String REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY = "hbase.rest-csrf.browser-useragents-regex";

    public static void addCSRFFilter(Context context, Configuration conf) {
        restCSRFEnabled = conf.getBoolean(REST_CSRF_ENABLED_KEY, REST_CSRF_ENABLED_DEFAULT);
        if (restCSRFEnabled) {
            String[] urls = {"/*"};
            Set<String> restCsrfMethodsToIgnore = new HashSet<>();
            restCsrfMethodsToIgnore.addAll(getTrimmedStringList(conf,
                    REST_CSRF_METHODS_TO_IGNORE_KEY, REST_CSRF_METHODS_TO_IGNORE_DEFAULT));
            Map<String, String> restCsrfParams = RestCsrfPreventionFilter
                    .getFilterParams(conf, "hbase.rest-csrf.");
            HttpServer.defineFilter(context, "csrf", RestCsrfPreventionFilter.class.getName(),
                    restCsrfParams, urls);
        }
    }

    // login the server principal (if using secure Hadoop)
    public static Pair<FilterHolder, Class<? extends ServletContainer>> loginServerPrincipal(
            UserProvider userProvider, Configuration conf) throws Exception {
        Class<? extends ServletContainer> containerClass = ServletContainer.class;
        if (userProvider.isHadoopSecurityEnabled() && userProvider.isHBaseSecurityEnabled()) {
            String machineName = Strings.domainNamePointerToHostName(
                    DNS.getDefaultHost(conf.get(REST_DNS_INTERFACE, "default"),
                            conf.get(REST_DNS_NAMESERVER, "default")));
            String keytabFilename = conf.get(REST_KEYTAB_FILE);
            Preconditions.checkArgument(keytabFilename != null && !keytabFilename.isEmpty(),
                    REST_KEYTAB_FILE + " should be set if security is enabled");
            String principalConfig = conf.get(REST_KERBEROS_PRINCIPAL);
            Preconditions.checkArgument(principalConfig != null && !principalConfig.isEmpty(),
                    REST_KERBEROS_PRINCIPAL + " should be set if security is enabled");
            userProvider.login(REST_KEYTAB_FILE, REST_KERBEROS_PRINCIPAL, machineName);
            if (conf.get(REST_AUTHENTICATION_TYPE) != null) {
                containerClass = RESTServletContainer.class;
                FilterHolder authFilter = new FilterHolder();
                authFilter.setClassName(AuthFilter.class.getName());
                authFilter.setName("AuthenticationFilter");
                return new Pair<FilterHolder, Class<? extends ServletContainer>>(authFilter, containerClass);
            }
        }
        return new Pair<FilterHolder, Class<? extends ServletContainer>>(null, containerClass);
    }

}
