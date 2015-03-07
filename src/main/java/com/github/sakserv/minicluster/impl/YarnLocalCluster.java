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
package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

public class YarnLocalCluster implements MiniCluster {

    MiniYARNCluster miniYARNCluster = new MiniYARNCluster("blah", 1, 1, 1, 1);;

    public void configure() {
        try {
            miniYARNCluster.serviceInit(new Configuration());
        } catch(Exception e) {
            e.printStackTrace();
        }
        
    }
    public void stop() {
        miniYARNCluster.stop();
        
    }
    public void start() {
        configure();
        miniYARNCluster.start();
    }
    
}
