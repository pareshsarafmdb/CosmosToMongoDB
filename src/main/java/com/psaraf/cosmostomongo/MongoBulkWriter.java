package com.psaraf.cosmostomongo;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class MongoBulkWriter {
	static final int BATCHSIZE=100;
	Logger logger;	
	MongoClient mongoClient=null;
	MongoDatabase db = null;
	MongoCollection<Document>  collection = null;
	List<WriteModel<Document>> ops;
	int nops =0;
	
	public MongoBulkWriter(MongoCollection<Document> collection)
	{
		logger = LoggerFactory.getLogger(MongoBulkWriter.class);
		this.collection = collection;
		ops = new ArrayList<WriteModel<Document>>();
	}
	
	public Document FindOne(Document query,Document fields,Document order)
	{
		Document rval = null;
		
		if(query == null) { query = new Document();}
		
		FindIterable<Document> fi = collection.find(query,Document.class);
	
		if(fields != null) {	logger.info("project:" + fields.toJson());;
		fi.projection(fields);}
		if(order != null) { fi.sort(order);}
		MongoCursor<Document> c = fi.iterator();
		if(c.hasNext()) { rval=c.next();}
		return rval;
		
	}
	
	//Document shoudl have a 'find' field
	public void Update(Document doc, boolean upsert)
	{
		Document find;
		UpdateOptions uo = new UpdateOptions();
		uo.upsert(upsert);
		if(doc.containsKey("$find")) {
			find = (Document)doc.get("$find");
			doc.remove("$find");
		
			ops.add(new UpdateOneModel<Document>(find,doc,uo));
		} else {
			logger.error("No $find section defined");
			System.exit(1);
		}
		FlushOpsIfFull(false);
	}
	
	public void Save(Document doc) {
		if (!doc.containsKey("_id")) {
			Create(doc);
			return;
		}
		Document find = new Document("_id", doc.get("_id"));
		UpdateOptions uo = new UpdateOptions();
		uo.upsert(true);
		ops.add(new ReplaceOneModel<Document>(find, doc, uo));
		FlushOpsIfFull(false);
	}

	public void flushOps() {
		FlushOpsIfFull(true);
	}
	
	private void FlushOpsIfFull(boolean force)
	{
		boolean fatalerror = false;
		if(ops.size() > MongoBulkWriter.BATCHSIZE || force )
		{
			try {
				//Now NOT ordered
				collection.bulkWrite(ops, new BulkWriteOptions().ordered(false));
			}  catch (com.mongodb.MongoBulkWriteException err) {
				//  Duplicate inserts are not an error if retrying
				for (BulkWriteError bwerror : err.getWriteErrors()) {
		
					if (bwerror
							.getCategory() != ErrorCategory.DUPLICATE_KEY) {
						logger.error(bwerror.getMessage());
						fatalerror = true;
						break;
					} else {
						logger.warn("Attempt to load records with DUPLICATE _id fields");
						logger.warn(bwerror.getMessage());
					}
				}
			} catch (MongoException err) {
				// This is some other type of error not a BulkWriteError
				// object just sleep for 3 seconds and retry - this covers network Outages, elections etc.
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
					fatalerror = true;
				}
				logger.error("Error: " + err.getMessage());
			}
			ops.clear();
		}
	}
	//Maps to batched inserts
	
	public void Create(Document doc)
	{
		
		ops.add(new  InsertOneModel<Document>(doc));
		FlushOpsIfFull(false);
	}
	
	//Add updates here
	
	public void close()
	{
		if(ops.size()>0 ){
			collection.bulkWrite(ops);
			ops.clear();
		}
	}
}
