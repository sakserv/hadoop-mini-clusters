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

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HdfsLocalClusterTest {

    // Logger
    private static final Logger LOG = Logger.getLogger(HdfsLocalCluster.class);

    private HdfsLocalCluster dfsCluster;

    @Before
    public void setUp(){
        dfsCluster = new HdfsLocalCluster();
        dfsCluster.start();
    }

    @After
    public void tearDown(){
        dfsCluster.stop(true);
    }

    @Test
    public void testDfsClusterStart() {
        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        try {
            FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/testing"));
            writer.writeUTF("This is a test");
            writer.close();

            FSDataInputStream reader = hdfsFsHandle.open(new Path("/tmp/testing"));
            LOG.info("HDFS READ: Output from test file: " + reader.readUTF());
            reader.close();

            hdfsFsHandle.close();
        } catch(IOException e) {
            e.printStackTrace();
        }

    }
}
