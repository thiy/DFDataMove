package com.temenos.df;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Config {
    private static Logger logger = Logger.getLogger(Config.class);

    private static Properties props = loadConfiguration();
	private static Properties loadConfiguration() {
			Properties props = new Properties();
			try {
				props.load(Config.class.getResourceAsStream("/config.properties"));
			} catch (IOException e) {
				logger.error("Error while reading the config file ", e);
			}
			return props;
	}
	
	public static String get(String key){
		return props.getProperty(key);
	}

}
