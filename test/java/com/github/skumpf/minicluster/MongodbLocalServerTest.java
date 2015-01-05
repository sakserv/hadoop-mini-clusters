package com.github.skumpf.minicluster;

import com.github.skumpf.minicluster.impl.MongodbLocalServer;
import com.mongodb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Date;

/**
 * Created by skumpf on 1/5/15.
 */
public class MongodbLocalServerTest {
    
    private static final String DEFAULT_DATABASE_NAME = "test_database";
    private static final String DEFAULT_COLLECTION_NAME = "test_collection";
    private static final int DEFAULT_MONGOD_PORT = 12345;
    
    private MongodbLocalServer mongodbServer;

    @Before
    public void setUp() throws Exception {
        mongodbServer = new MongodbLocalServer(DEFAULT_DATABASE_NAME, DEFAULT_COLLECTION_NAME, DEFAULT_MONGOD_PORT);
        mongodbServer.start();
    }

    @After
    public void tearDown() throws Exception {
        mongodbServer.stop();
    }

    @Test
    public void testMongodbLocalServer() throws UnknownHostException {

        MongoClient mongo = new MongoClient("localhost", DEFAULT_MONGOD_PORT);

        DB db = mongo.getDB(DEFAULT_DATABASE_NAME);
        DBCollection col = db.createCollection(DEFAULT_COLLECTION_NAME, new BasicDBObject());
        
        col.save(new BasicDBObject("testDoc", new Date()));
        System.out.println("MONGODB: Number of items in collection: " + col.count());
        
        DBCursor cursor = col.find();
        while(cursor.hasNext()) {
            System.out.println("MONGODB: Document output: " + cursor.next());
        }
        cursor.close();
    }
}
