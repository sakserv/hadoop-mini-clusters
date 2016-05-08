package com.github.sakserv.minicluster.yarn.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
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
 */

public class ExecShellCliParserTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ExecShellCliParserTest.class);

    private static final String command = "whoami";
    private static final String stdoutFile = "./target/com.github.sakserv.minicluster.impl.YarnLocalCluster/com.github.sakserv.minicluster.impl.YarnLocalCluster-logDir-nm-0_0/application_1431983196063_0001/container_1431983196063_0001_01_000002/stdout";
    private static final String stderrFile = "./target/com.github.sakserv.minicluster.impl.YarnLocalCluster/com.github.sakserv.minicluster.impl.YarnLocalCluster-logDir-nm-0_0/application_1431983196063_0001/container_1431983196063_0001_01_000002/stderr";
    private static final String cliString = command + " 1>" + stdoutFile + " 2>" + stderrFile;

    private static ExecShellCliParser execShellCliParser;

    @Before
    public void setUp() {
        execShellCliParser = new ExecShellCliParser(cliString);
    }

    @Test
    public void testGetCommand() {
        assertEquals(command, execShellCliParser.getCommand());

    }

    @Test
    public void testGetStdoutPath() {
        assertEquals(stdoutFile, execShellCliParser.getStdoutPath());
    }

    @Test
    public void testGetStderrPath() {
        assertEquals(stderrFile, execShellCliParser.getStderrPath());
    }

    @Test
    public void testRunCommand() throws Exception {
        assertEquals(0, execShellCliParser.runCommand());
    }
}
