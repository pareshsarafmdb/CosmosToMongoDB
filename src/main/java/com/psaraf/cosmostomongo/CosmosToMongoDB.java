package com.psaraf.cosmostomongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.logging.LogManager;

public class CosmosToMongoDB {
    private static final String version = "0.1.1";
    private static CommandLineOptions options;
    private static JobDescription jobdesc;

    public static void main(String[] args) {

        LogManager.getLogManager().reset();
        Logger logger = LoggerFactory.getLogger(CosmosToMongoDB.class);
        logger.info("CosmosToMongoDB Version " + version);

        try {
            options = new CommandLineOptions(args);
        } catch (ParseException e) {
            logger.error("Failed to parse command line options");
            logger.error(e.getMessage());
            System.exit(1);
        }

        if (options.isHelpOnly()) {
            System.exit(0);
        }

        if (options.getConfigFile() == null) {
            logger.error("No config file supplied");
            System.exit(1);
        }

        try {
            logger.info("config file : " + options.getConfigFile());
            jobdesc = new JobDescription(options.getConfigFile());
        } catch (FileNotFoundException e) {
            logger.error("Failed to parse config file");
            logger.error(e.getMessage());
            System.exit(1);
        }

        String sourceConnString = jobdesc.getSourceConnectionString();
        String destConnString = jobdesc.getDestConnectionString();
        logger.info("source    " + sourceConnString);
        logger.info("Target    " +destConnString);

        MongoConnection srcMongoConnection = new MongoConnection(sourceConnString);
                //new MongoConnection(
                //"mongodb://mongodbtestparesh:dDGKEZk8YsJ5Oxo0KhjdHXt2Aeoue5Dl3bFi" +
                //"wgZBKgDqcDqRi2Pr6VG8rcls0gZc9YfUqFKKghFuZd3mecPSWw==@mongodbtestparesh.mongo.cosmos.azure.com:" +
                //"10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@mongodbtestparesh@");

        MongoConnection destMongoConnection = new MongoConnection(destConnString);
                //("mongodb+srv://main_user:pare1212@test.mk1cl.mongodb.net/test?retryWrites=true&w=majority");

        SinkWriter sinkWriter = new SinkWriter(destMongoConnection.Connect("",""));
        SourceExtractor sourceExtractor = new SourceExtractor();
        sourceExtractor.ExtractData(sinkWriter, srcMongoConnection.Connect("",""), Arrays.asList());
    }
}
