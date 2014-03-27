package com.temenos.df;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class T24Utils {

    private static Logger logger = Logger.getLogger(T24Utils.class);
	
	static ComboPooledDataSource dimCPDS = null; //getDimConnectionDS();

    
	public static void main(String[] args) throws Exception {

		resetTimePolledColumn();
//		testJDBC();
		
	}
	
	private static void testJDBC() throws SQLException {
		Stopwatch stopwatch = Stopwatch.createStarted();

		Connection dimConn = dimCPDS.getConnectionPoolDataSource().getPooledConnection().getConnection();
		Statement stmt = dimConn.createStatement();
		int i =0;
while(true) {
	if(i>1000) {
		break;
	}
		String query = "INSERT INTO \"DIMCONTACT\" (ContactID,Active,Effdate,EffEndDate,SourceCustomerID,TransactionTime) VALUES (SYS_GUID(),'Y',to_timestamp('13:28:02:745 25 MAR 2014','HH24:MI:SS:FF DD MON YYYY'),to_timestamp('13:28:02:745 25 MAR 2014','HH24:MI:SS:FF DD MON YYYY'),'1000241091','13:28:02:745 25 MAR 2014')";
		stmt.execute(query);
		System.out.println(i++);
}
		dimConn.close();
		stmt.close();
		stopwatch.stop();
		logger.debug("Time Take for complete process " + stopwatch.elapsed(TimeUnit.SECONDS) + " Seconds");

	}
	
	
	
	
	private static ComboPooledDataSource  getDimConnectionDS() {
		if(dimCPDS != null) {
			return dimCPDS;
		}

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass( Config.get("df.dim.jdbc.driverclass") );
		} catch (PropertyVetoException e) {
			logger.error("Error while creating connection pool ", e);
		}             
		cpds.setJdbcUrl( Config.get("df.dim.jdbc.url") );
		cpds.setUser(Config.get("df.dim.jdbc.username"));                                  
		cpds.setPassword(Config.get("df.dim.jdbc.password"));  

		cpds.setMinPoolSize(50);                                     
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(25);
		return cpds;
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
				 query =  "UPDATE \"F_DF_FILE_DEFINITIONS\" SET TABLE.NAME = 'STMT.ENTRY.DETAILS' WHERE RECID = '5e3baf44-db49-4256-8123-d7ba1caec014'";
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
