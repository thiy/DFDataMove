package com.temenos.df;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.jbase.jremote.DefaultJConnectionFactory;
import com.jbase.jremote.JConnection;
import com.jbase.jremote.JConnectionFactory;
import com.jbase.jremote.JRemoteException;

public class T24ConnectionFactory {

    private static Logger logger = Logger.getLogger(T24ConnectionFactory.class);

	private static  List<JConnection> jConnections = new ArrayList<JConnection>();
	private static int currentConnectionIndex = 0;
	private static int noOfConnection = 0;

	public static void createConnection(int noOfConnection) {
		T24ConnectionFactory.noOfConnection = noOfConnection;
		for(int i = 0; i<noOfConnection + 10;i++) {
			jConnections.add(getT24JConnection());
		}
	}
	
	
	public synchronized static JConnection getJConnectionFromPool() {
		JConnection connection = jConnections.get(currentConnectionIndex);
		currentConnectionIndex++;
		if(currentConnectionIndex >= noOfConnection) {
			currentConnectionIndex = 0;
		}
		return connection;
	}
	
	public static JConnection getT24JConnection() {
		return new T24ConnectionFactory().createConnection(Config.get("df.t24agent.host"), Integer.parseInt(Config.get("df.t24agent.port")));
		 
	}
	private JConnection createConnection(String t24Host, int portNumber) {
		
		try {
			return getConnection(t24Host, portNumber, Config.get("df.t24.ofs.source"));
		} catch (JRemoteException e) {
			logger.error("Error while creating the connection ", e);
		}
		return null;
	}
	private JConnection getConnection(String t24Host, int portNumber,
			String ofsId) throws JRemoteException {
		JConnectionFactory jConnectionFactory = new DefaultJConnectionFactory();

		// set the host to the factory
		((DefaultJConnectionFactory) jConnectionFactory).setHost(t24Host);
		// set the port to the factory
		((DefaultJConnectionFactory) jConnectionFactory).setPort(portNumber);
			// create new properties
			Properties properties = new Properties();
			// add ofs source id properties
			properties.put("env.OFS_SOURCE", ofsId);
			// trying to create the connection using jremote connection factory
			// which has the respective T24 area details.
			
			JConnection jConnection = jConnectionFactory.getConnection(Config.get("df.t24.username"), Config.get("df.t24.password"), properties);
			// call the initialize subroutine to initialize the common variables
			// in T24.
			jConnection.call("JF.INITIALISE.CONNECTION", null);
			// return the created connection.
			return jConnection;
	}

}
