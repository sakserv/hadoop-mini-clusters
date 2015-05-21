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
package com.github.sakserv.minicluster.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyParser {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyParser.class);

    private Properties props = new Properties();
    private String propFileName;

    public PropertyParser(String propFileName) {
        this.propFileName = propFileName;
    }

    public String getPropFileName() {
        return propFileName;
    }

    public void setPropFileName(String propFileName) {
        this.propFileName = propFileName;
    }

    public String getProperty(String key) {
        return props.get(key).toString();
    }

    public void parsePropsFile() throws IOException {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        try {
            if (null != inputStream) {
                props.load(inputStream);
            } else {
                throw new IOException("Could not load property file from the resources directory, trying local");
            }
        } catch(IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            try {
                inputStream = new FileInputStream(new File(propFileName).getAbsolutePath());
                props.load(inputStream);
            } catch (IOException ex) {
                LOG.error("Could not load property file at " + new File(propFileName).getAbsolutePath());
                ex.printStackTrace();
                throw ex;
            }
        }
    }
}
