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
package com.github.sakserv.minicluster.yarn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ExecShellCliParser {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ExecShellCliParser.class);

    private String cliString;

    public ExecShellCliParser(String cliString) {
        this.cliString = cliString;
    }

    public String getCommand() {
        String[] cliStringParts = cliString.split(" ");
        StringBuilder sb = new StringBuilder();
        for (String part: cliStringParts) {
            if (!part.startsWith("1>") && !part.startsWith("2>") && !part.startsWith(">")) {
                sb.append(part);
                sb.append(' ');
            }
        }
        return sb.toString().trim();
    }

    public String getStdoutPath() {
        String[] cliStringParts = cliString.split(" ");
        StringBuilder sb = new StringBuilder();
        for (String part : cliStringParts) {
            if (part.startsWith("1>")) {
                sb.append(part.replace("1>", ""));
                sb.append(' ');
            }
        }
        return sb.toString().trim();
    }

    public String getStderrPath() {
        String[] cliStringParts = cliString.split(" ");
        StringBuilder sb = new StringBuilder();
        for (String part : cliStringParts) {
            if (part.startsWith("2>")) {
                sb.append(part.replace("2>", ""));
                sb.append(' ');
            }
        }
        return sb.toString().trim();
    }

    public int runCommand() throws Exception {
        String command = getCommand();
        String stdoutFile = getStdoutPath();
        String stderrFile = getStderrPath();

        Process p = Runtime.getRuntime().exec(command.split(" "));

        String stdout = getOutput(p.getInputStream());
        String stderr = getOutput(p.getErrorStream());

        writeOutputToFile(stdout, new File(stdoutFile));
        writeOutputToFile(stderr, new File(stderrFile));

        p.waitFor();
        return p.exitValue();
    }

    public String getOutput(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        while((line = bufferedReader.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    public void writeOutputToFile(String output, File outputFile) throws IOException {
        File parentDir = outputFile.getAbsoluteFile().getParentFile();
        parentDir.mkdirs();
        org.apache.commons.io.FileUtils.writeStringToFile(outputFile, output);
    }

}
