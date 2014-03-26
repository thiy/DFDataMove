package com.temenos.df;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;


public class DBConnectionFactory {
    private static Logger logger = Logger.getLogger(DBConnectionFactory.class);

	private final static ComboPooledDataSource dimCPDS = getDimConnectionDS();
	private final static ComboPooledDataSource selCPDS = getSelConnectionDS();
	
	public synchronized static Connection getDimDBConnection() throws Exception {
		Class.forName (Config.get("df.dim.jdbc.driverclass")).newInstance();
		Connection conn = DriverManager.getConnection (Config.get("df.dim.jdbc.url"), Config.get("df.dim.jdbc.username"), Config.get("df.dim.jdbc.password"));
		return conn;
	}
    
	public synchronized static Connection getSelDBConnection() throws Exception {
		Class.forName (Config.get("df.sel.jdbc.driverclass")).newInstance();
		Connection conn = DriverManager.getConnection (Config.get("df.sel.jdbc.url"), Config.get("df.sel.jdbc.username"), Config.get("df.sel.jdbc.password"));
		return conn;
	}
    
	private static ComboPooledDataSource  getDimConnectionDS() {
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass( Config.get("df.dim.jdbc.driverclass") );
		} catch (PropertyVetoException e) {
			logger.error("Error while creating connection pool ", e);
		}             
		cpds.setJdbcUrl( Config.get("df.dim.jdbc.url") );
		cpds.setUser(Config.get("df.dim.jdbc.username"));                                  
		cpds.setPassword(Config.get("df.dim.jdbc.password"));  

		cpds.setMinPoolSize(10);                                     
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(50);
		return cpds;
	}
	
	private static ComboPooledDataSource  getSelConnectionDS() {
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(Config.get("df.sel.jdbc.driverclass") );
		} catch (PropertyVetoException e) {
			logger.error("Error while creating connection pool ", e);
		}             
		cpds.setJdbcUrl( Config.get("df.sel.jdbc.url") );
		cpds.setUser(Config.get("df.sel.jdbc.username"));                                  
		cpds.setPassword(Config.get("df.sel.jdbc.password"));  

		cpds.setMinPoolSize(10);                                     
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(50);
		return cpds;
	}

}
