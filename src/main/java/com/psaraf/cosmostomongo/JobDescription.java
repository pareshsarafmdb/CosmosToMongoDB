package com.psaraf.cosmostomongo;

import org.bson.Document;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class JobDescription {
	private Document jobDesc;
	Logger logger;

	JobDescription(String configFile) throws FileNotFoundException {
		logger = LoggerFactory.getLogger(JobDescription.class);
		String config = "";
		try {
			config = new String(Files.readAllBytes(Paths.get(configFile)),
					StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error(e.getMessage());

			System.exit(1);
		}
		
		// Handle \ followed by newline as line continuation
		config = config.replaceAll("\\\\\n", "");
		
		// Better errors from this parser
		try {
			@SuppressWarnings("unused")
			JSONObject obj = new JSONObject(config);
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
		jobDesc = Document.parse(config);

	}


	public String getSourceConnectionString() {
		Document source = (Document) jobDesc.get("source");
		return source.getString("uri");
	}

	public String getDestConnectionString() {
		Document dest = (Document) jobDesc.get("destination");
		return dest.getString("uri");
	}

	public List<String> getDbsToMigrate() {
		List<String> dbsToMigrate = jobDesc.getList("databases", String.class);
		return dbsToMigrate;
	}

	public Map<String, Object> getJobDesc() {
		return jobDesc;
	}

	public Document getSection(String heading) {
		if (heading == null) {
			return jobDesc.get("start", Document.class);
		}
		return jobDesc.get(heading, Document.class);
	}

}
