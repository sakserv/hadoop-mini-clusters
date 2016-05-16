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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongodbLocalServerIntegrationTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongodbLocalServerIntegrationTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;
    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }
    
    private static MongodbLocalServer mongodbLocalServer;

    @BeforeClass
    public static void setUp() throws Exception {
        mongodbLocalServer = new MongodbLocalServer.Builder()
                .setIp(propertyParser.getProperty(ConfigVars.MONGO_IP_KEY))
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)))
                .build();
        mongodbLocalServer.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mongodbLocalServer.stop();
    }

    @Test
    public void testMongodbLocalServer() throws Exception {
        MongoClient mongo = new MongoClient(mongodbLocalServer.getIp(), mongodbLocalServer.getPort());

        DB db = mongo.getDB(propertyParser.getProperty(ConfigVars.MONGO_DATABASE_NAME_KEY));
        DBCollection col = db.createCollection(propertyParser.getProperty(ConfigVars.MONGO_COLLECTION_NAME_KEY),
                new BasicDBObject());
        
        col.save(new BasicDBObject("testDoc", new Date()));
        LOG.info("MONGODB: Number of items in collection: {}", col.count());
        assertEquals(1, col.count());
        
        DBCursor cursor = col.find();
        while(cursor.hasNext()) {
            LOG.info("MONGODB: Document output: {}", cursor.next());
        }
        cursor.close();
    }
}
