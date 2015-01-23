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

import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import com.mongodb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class MongodbLocalServerTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongodbLocalServerTest.class);
    
    private static final String DEFAULT_DATABASE_NAME = "test_database";
    private static final String DEFAULT_COLLECTION_NAME = "test_collection";
    private static final String DEFAULT_IP = "127.0.0.1";
    private static final int DEFAULT_MONGOD_PORT = 12345;
    
    private MongodbLocalServer mongodbServer;

    @Before
    public void setUp() throws Exception {
        mongodbServer = new MongodbLocalServer(DEFAULT_IP, DEFAULT_MONGOD_PORT);
        mongodbServer.start();
    }

    @After
    public void tearDown() throws Exception {
        mongodbServer.stop();
    }

    @Test
    public void testMongodbLocalServer() throws UnknownHostException {
        
        mongodbServer.dumpConfig();

        MongoClient mongo = new MongoClient(DEFAULT_IP, DEFAULT_MONGOD_PORT);

        DB db = mongo.getDB(DEFAULT_DATABASE_NAME);
        DBCollection col = db.createCollection(DEFAULT_COLLECTION_NAME, new BasicDBObject());
        
        col.save(new BasicDBObject("testDoc", new Date()));
        LOG.info("MONGODB: Number of items in collection: " + col.count());
        assertEquals(1, col.count());
        
        DBCursor cursor = col.find();
        while(cursor.hasNext()) {
            LOG.info("MONGODB: Document output: " + cursor.next());
        }
        cursor.close();
    }
}
