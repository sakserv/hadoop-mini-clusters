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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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