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

package com.github.sakserv.minicluster;

import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperLocalClusterTest {

    ZookeeperLocalCluster zkCluster;

    @Before
    public void setUp() {
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();
    }

    @After
    public void tearDown() {
        zkCluster.stop(true);
    }

    @Test
    public void testZookeeperCluster() {
        zkCluster.dumpConfig();
    }

}
