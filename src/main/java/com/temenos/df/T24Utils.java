package com.temenos.df;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class T24Utils {

    private static Logger logger = Logger.getLogger(T24Utils.class);
	
	public static void main(String[] args) {

		resetTimePolledColumn();

		
	}
	
	private static List<String> resetTimePolledColumn() {
		List<String> ids = new ArrayList<String>();
		logger.debug("======================================");
		logger.debug("CALLING JDBC");
		logger.debug(new Date());
		logger.debug("======================================");
		
		try {
			Class.forName(Config.get("df.t24.jdbc.driverclass"));
			Connection conn = DriverManager.getConnection(Config.get("df.t24.jdbc.url"), Config.get("df.t24.jdbc.username"), Config.get("df.t24.jdbc.password"));
			Statement stmt = conn.createStatement(); 

			 String query = "";
			 
			 if(Config.get("df.t24.db").equals("ORACLE")) {
				 query =  "UPDATE \"F_DATA_EVENTS\" SET TS = NULL";
			 } else {
				 query = "UPDATE \"F.DF.DATA.EVENTS\" SET TS = NULL";
			 }
			 
			 ResultSet rset = stmt.executeQuery (query); 

			 stmt.close();
		} catch (Exception e) {
			logger.error("Error while doing T24 JDBC Call ", e);
		}

		return ids;
	}

}
