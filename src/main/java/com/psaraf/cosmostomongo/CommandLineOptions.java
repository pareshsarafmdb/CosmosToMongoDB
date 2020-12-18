package com.psaraf.cosmostomongo;


import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommandLineOptions {
	private boolean helpOnly = false;
	private String configFile = null;
	private String newConfigFile = null;

	public CommandLineOptions(String[] args) throws ParseException {
		Logger logger = LoggerFactory.getLogger(CommandLineOptions.class);
		logger.info("Parsing Command Line");

		CommandLineParser parser = new DefaultParser();

		Options cliopt;
		cliopt = new Options();

		cliopt.addOption("h", "help", false, "Show Help");
		cliopt.addOption("c", "config", true, "Configuration File");
		cliopt.addOption("n", "newconfig", true, "Generate new Configuration File");
	
		CommandLine cmd = parser.parse(cliopt, args);
		
		if (cmd.hasOption("c")) {
			configFile = cmd.getOptionValue("c");
		}
		
		if (cmd.hasOption("n")) {
			newConfigFile = cmd.getOptionValue("n");
		}
		
		if (cmd.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("MongoSyphon", cliopt);
			helpOnly = true;
		}
	}


	public boolean isHelpOnly() {
		return helpOnly;
	}


	public String getNewConfigFile() {
		return newConfigFile;
	}	
	
	public String getConfigFile() {
		return configFile;
	}	
}
