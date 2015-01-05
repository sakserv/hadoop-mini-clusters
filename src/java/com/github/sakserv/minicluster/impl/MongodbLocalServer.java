package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.MiniCluster;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;

/**
 * Created by skumpf on 12/20/14.
 */
public class MongodbLocalServer implements MiniCluster {

    private static final String DEFAULT_DATABASE_NAME = "test_database";
    private static final String DEFAULT_COLLECTION_NAME = "test_collection";
    private static final int DEFAULT_PORT = 12345;

    private String dbName;
    private String collName;
    private int port;
    
    private MongodStarter starter;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private IMongodConfig conf;
    
    public MongodbLocalServer() {
        dbName = DEFAULT_DATABASE_NAME;
        collName = DEFAULT_COLLECTION_NAME;
        port = DEFAULT_PORT;
        configure();
    }
    
    public MongodbLocalServer(String dbName, String collName, int port) {
        this.dbName = dbName;
        this.collName = collName;
        this.port = port;
        configure();
    }
    
    public void start() {
        try {
            starter = MongodStarter.getDefaultInstance();
            mongodExe = starter.prepare(conf);
            mongod = mongodExe.start();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    public void stop() {
        mongod.stop();
        mongodExe.stop();
    }

    public void configure() {
        try {
            conf = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(port, Network.localhostIsIPv6()))
                    .build();
        } catch(IOException e) {
            e.printStackTrace();
        }
        
    }
    
    public void dumpConfig() {
        System.out.println("MONGODB: CONFIG: " + conf.toString());
        
    }
}
