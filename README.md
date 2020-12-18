# CosmosToMongoDB

## Introduction

This is a tool  designed specifically to migrate data from CosmosDB API for MongoDB to MongoDB Atlas. This tool for now can be used for initial bulk transfer of data. This tool doesn't contain explicit Change Data Capture (CDC) capability. 

The tool is good for quick POCs and small scale migrations. There is no graphical UI for this tool yet.

## Command Line

```
java -jar CosmosToMongoDB.jar -c path_for_config_file
```

## Configuration

Tool takes a json configuration file which takes source CosmosDB URI, Destination MongoDB Atlas URI and list of databases to be migrated. If the list is empty it will migrate all the databases.

```
 {
 	"source": {
 		"uri": "Cosmos URI here"
 	},
 	"destination": {

 		"uri": "MongoDB Atlas URI here"
 	},
 	"databases": []
 }
```
## Output

As the script is running it will give a complete report of no. of collections migrated, no. of documents read from each collection and no. of documents insterted into MongoDB Atlas. Below is the screenshot. 

![Topics](/docs/output.png)












