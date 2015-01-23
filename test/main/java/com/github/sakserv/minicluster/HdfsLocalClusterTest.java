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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.io.IOException;

public class HdfsLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HdfsLocalClusterTest.class);

    private static final String TEST_STRING = "TESTING";

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
    public void testDfsClusterStart() throws IOException {
        
        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/testing"));
        writer.writeUTF(TEST_STRING);
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path("/tmp/testing"));
        assertEquals(reader.readUTF(), TEST_STRING);
        reader.close();
        hdfsFsHandle.close();

    }
}
