package com.psaraf.cosmostomongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class MongoConnection {
    MongoClient mongoClient = null;
    private String connectionString = null;
    private String user;
    private String pass;
    private Logger logger;

    public MongoConnection(String connectionString) {
        logger = LoggerFactory.getLogger(MongoConnection.class);
        this.connectionString = connectionString;
    }

    public MongoClient Connect(String user, String pass) {
        try {
            //logger.info("Connecting to " + connectionString);
            ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").
                    setLevel(Level.ERROR);

            mongoClient = new MongoClient(new MongoClientURI(connectionString));

            mongoClient.getDatabase("admin")
                    .runCommand(new Document("ping", 1));
            return mongoClient;

        } catch (Exception e) {
            logger.error("Unable to connect to MongoDB");
            logger.error(e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public void RunQuery(String sql, ArrayList<String> params, Document parent) {

    }
}
