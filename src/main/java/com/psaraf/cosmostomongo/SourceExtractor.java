package com.psaraf.cosmostomongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SourceExtractor {
    private Logger logger;
    public SourceExtractor() {
        logger = LoggerFactory.getLogger(MongoConnection.class);
    }

    public void ExtractData(SinkWriter sinkWriter, MongoClient mongoClient, List<String> databases) {
        List<String> databasesToExport = new ArrayList<String>();
        if (databases.isEmpty()) {
            MongoCursor<String> iterator = mongoClient.listDatabaseNames().iterator();
            while(iterator.hasNext()) {
                databasesToExport.add(iterator.next());
            }
        }

        for (String databaseToExport : databasesToExport) {
            System.out.println();
            System.out.println("migrating database : " + databaseToExport);
            System.out.println("******************************************");
            System.out.println();
            System.out.println("Collection\t# of Documents from source\t# of documents written to target");
            System.out.println("__________________________________________________________________________________________");
            MongoIterable<String> collectionsToExport = mongoClient.getDatabase(databaseToExport).listCollectionNames();
            MongoCursor<String> iterator = collectionsToExport.iterator();
            List<String> collectionsToExportList = new ArrayList<String>();
            while (iterator.hasNext()) {
                collectionsToExportList.add(iterator.next());
            }
            for (String collectionToExport : collectionsToExportList) {
                loadCollectionData(sinkWriter, databaseToExport, collectionToExport, mongoClient);
            }
        }

    }

    private void loadCollectionData(SinkWriter sinkWriter, String databaseName, String collectionName, MongoClient mongoClient) {
        MongoCollection<Document> coll = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        Long count = coll.count();

        MongoCursor<Document> cursor = coll.find().iterator();
        sinkWriter.prepareTargetCollection(databaseName, collectionName);
        while(cursor.hasNext()) {
            Document doc = cursor.next();
            sinkWriter.writeDocument(doc);
        }
        sinkWriter.flushOps();
        //Verify document count
        System.out.println(databaseName + "." + collectionName + "\t\t\t" + count + "\t\t\t" + sinkWriter.getDocumentsCount());

    }
}
