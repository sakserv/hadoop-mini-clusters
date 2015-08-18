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
package com.github.sakserv.minicluster.oozie.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.JPAService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class OozieConfigHelpers {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(OozieConfigHelpers.class);

    public static Configuration getOozieSite(Configuration configuration) {
        configuration.set("oozie.service.JPAService.jdbc.driver", "org.hsqldb.jdbcDriver");
        configuration.set("oozie.service.JPAService.jdbc.url", "jdbc:hsqldb:mem:oozie-db;create=true");
        configuration.set(JPAService.CONF_CREATE_DB_SCHEMA, "true");
        return configuration;
    }

    public static void writeXml(Configuration configuration, String outputLocation) throws IOException {
        new File(new File(outputLocation).getParent()).mkdirs();
        configuration.writeXml(new FileOutputStream(outputLocation));
    }

}
