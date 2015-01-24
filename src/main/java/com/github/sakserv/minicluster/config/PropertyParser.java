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

import java.io.*;
import java.util.Properties;

public class PropertyParser {

    private Properties props = new Properties();
    
    public PropertyParser(String propFileName) throws IOException {
        parsePropsFile(propFileName);
    }

    public String getProperty(String key) {
        return props.get(key).toString();
    }

    public void parsePropsFile(String propFileName) throws IOException {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            props.load(inputStream);
        } else {
            inputStream = new FileInputStream(new File(propFileName).getAbsolutePath());
            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("Property file not found in resources directory: " + propFileName);
            }
        }
    }

}
