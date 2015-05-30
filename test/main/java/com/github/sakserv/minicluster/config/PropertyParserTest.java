package com.github.sakserv.minicluster.config;
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PropertyParserTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(PropertyParserTest.class);

    private static final String propFileName = "default.properties";
    private static final String badPropFileName = "bad.properties";
    private static final String localPropFileName = "test/resources/localonly.properties";
    private static final String testPropValue = "embedded_zk";

    private PropertyParser propertyParser = new PropertyParser(propFileName);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testPropertyFileName() {
        propertyParser.setPropFileName(propFileName);
        assertEquals(propFileName, propertyParser.getPropFileName());
    }

    @Test
    public void testBadPropFileName() throws IOException {
        propertyParser.setPropFileName(badPropFileName);
        exception.expect(IOException.class);
        propertyParser.parsePropsFile();
    }

    @Test
    public void testLocalOnlyPropFileName() throws IOException {
        propertyParser.setPropFileName(localPropFileName);
        propertyParser.parsePropsFile();
    }

    @Test
    public void testGetProperty() throws Exception {
        propertyParser.setPropFileName(propFileName);
        propertyParser.parsePropsFile();
        assertEquals(testPropValue, propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY));
    }

}
