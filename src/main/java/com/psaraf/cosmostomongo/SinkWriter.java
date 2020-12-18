package com.psaraf.cosmostomongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkWriter {
    private Logger logger;
    MongoClient mongoClient = null;
    MongoDatabase database = null;
    MongoCollection<Document> collection = null;
    MongoBulkWriter bulkWriter = null;
    Long totalCount = 0L;

    public SinkWriter(MongoClient mongoClient) {
        logger = LoggerFactory.getLogger(MongoConnection.class);
        this.mongoClient = mongoClient;
    }

    public void prepareTargetCollection(String databaseName, String collectionName) {
        database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
        bulkWriter = new MongoBulkWriter(collection);
        totalCount = 0L;
    }

    public void writeDocument(Document doc) {
        bulkWriter.Save(doc);
        totalCount ++;
    }

    public void flushOps() {
        bulkWriter.flushOps();

    }


    public Long getTotalCount() {
        return totalCount;
    }

    public long getDocumentsCount() {
        return collection.count();
    }

}
