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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ExecJavaCliParser {

    private final OptionParser optionParser;

    private final OptionSpec<String> xVmOptions;

    private final OptionSpec<String> sysPropOptions;

    private final OptionSpec<String> mainArgumentsOptions;

    private final OptionSet optionSet;

    private final List<String> mainArguments;

    private String main;

    // TODO move to isolated test
    public static void main(String[] args) {
        String line = "-Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN -Xmx200m -Dlog4j.configuration=tez-container-log4j.properties "
                + "-Dyarn.app.container.log.dir=/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-1_0/application_1401456720720_0001/container_1401456720720_0001_01_000002 "
                + "-Dtez.root.logger=INFO,CLA  -Djava.io.tmpdir=$PWD/tmp org.apache.hadoop.mapred.YarnTezDagChild 192.168.15.101 52598 container_1401456720720_0001_01_000002 application_1401456720720_0001 1 "
                + "1>/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-1_0/application_1401456720720_0001/container_1401456720720_0001_01_000002/stdout "
                + "2>/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-1_0/application_1401456720720_0001/container_1401456720720_0001_01_000002/stderr ";

        String line2 = "-Dlog4j.configuration=tez-container-log4j.properties "
                + "-Dyarn.app.container.log.dir=/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-0_0/application_1401456720720_0001/container_1401456720720_0001_01_000001 "
                + "-Dtez.root.logger=INFO,CLA  -Xmx1024m -Dsun.nio.ch.bugLevel='' org.apache.tez.dag.app.DAGAppMaster --session "
                + "1>/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-0_0/application_1401456720720_0001/container_1401456720720_0001_01_000001/stdout "
                + "2>/Users/ozhurakousky/dev/on-tez/tez-mini-cluster/target/MINI_TEZ_CLUSTER/MINI_TEZ_CLUSTER-logDir-nm-0_0/application_1401456720720_0001/container_1401456720720_0001_01_000001/stderr ";

        ExecJavaCliParser parser = new ExecJavaCliParser(line2);
        System.out.println(Arrays.asList(parser.getSysProperties()));
        System.out.println(Arrays.asList(parser.getXValues()));
        System.out.println(parser.getMain());
        System.out.println(Arrays.asList(parser.getMainArguments()));

//		parser = new ExecJavaCliParser(line);
//		System.out.println(Arrays.asList(parser.getSysProperties()));
//		System.out.println(Arrays.asList(parser.getXValues()));
//		System.out.println(parser.getMain());
//		System.out.println(Arrays.asList(parser.getMainArguments()));
    }

    /**
     *
     * @param line
     */
    public ExecJavaCliParser(String line){
        this.optionParser = new OptionParser("D:X:");
        this.optionParser.allowsUnrecognizedOptions();
        this.xVmOptions = this.optionParser.accepts("X").withRequiredArg();
        this.sysPropOptions = this.optionParser.accepts("D").withRequiredArg();
        this.mainArgumentsOptions = this.optionParser.nonOptions();
        this.optionSet = this.optionParser.parse(line.split(" "));
        Pattern p = Pattern.compile("([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*");
        this.mainArguments = new ArrayList<String>(this.mainArgumentsOptions.values(this.optionSet));
        Iterator<String> mainArgumentsIter = this.mainArguments.iterator();
        boolean mainFound = false;
        while (mainArgumentsIter.hasNext()){
            String value = mainArgumentsIter.next();
            Matcher m = p.matcher(value);
            boolean matches = m.matches();
            if (matches && !mainFound){
                mainFound = true;
                this.main = value;
                mainArgumentsIter.remove();
                break;
            }
            else if (!mainFound){
                mainArgumentsIter.remove();
            }
        }
    }

    public String[] getXValues(){
        return this.xVmOptions.values(this.optionSet).toArray(new String[]{});
    }

    public String[] getSysProperties(){
        return this.sysPropOptions.values(this.optionSet).toArray(new String[]{});
    }

    public String getMain() {
        return this.main;
    }

    public String[] getMainArguments() {
        return this.mainArguments.toArray(new String[]{});
    }
}