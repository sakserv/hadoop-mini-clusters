package com.github.sakserv.minicluster.yarn.util;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */public class ExecJavaCliParserTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ExecJavaCliParserTest.class);

    private static final String command = "java -Xmx=1024m -Dhadoop.user=foo com.test.foo";
    private static ExecJavaCliParser execJavaCliParser;

    @BeforeClass
    public static void setUp() throws Exception {
        execJavaCliParser = new ExecJavaCliParser(command);
    }

    @Test
    public void testGetXValues() throws Exception {
        assertEquals("mx=1024m", execJavaCliParser.getXValues()[0]);

    }

    @Test
    public void testGetSysProperties() throws Exception {
        assertEquals("hadoop.user=foo", execJavaCliParser.getSysProperties()[0]);

    }

    @Test
    public void testGetMainArguments() throws Exception {
        assertEquals("com.test.foo", execJavaCliParser.getMainArguments()[0]);
    }


    @Test
    public void testGetMain() throws Exception {
        assertEquals("java", execJavaCliParser.getMain());
    }

    @Test
    public void noMain() throws Exception {
        ExecJavaCliParser execJavaCliParser = new ExecJavaCliParser("");
        assertEquals(null, execJavaCliParser.getMain());
    }
}