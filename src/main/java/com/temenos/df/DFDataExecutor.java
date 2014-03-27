package com.temenos.df;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Stopwatch;

public class DFDataExecutor {
	
    private static Logger logger = Logger.getLogger(DFDataProcessor.class);

	private static ExecutorService executor = null;
	private static volatile List<Future> getDFDataResults = new ArrayList<Future>();
	final static int NO_OF_RECORDS = Integer.parseInt(Config.get("df.noofrecords"));
	final static int NO_OF_EVENTS_PER_THREAD = Integer.parseInt(Config.get("df.noofeventsperthread"));
	final static int MAX_NO_OF_THREADS = Integer.parseInt(Config.get("df.thread.max"));
	final static int DELAY_TIME_IN_SECONDS = Integer.parseInt(Config.get("df.delay.timeinseconds"));
	static int TOTAL_NO_OF_PROCESSED_RECORDS = 0;
	static int TOTAL_TIME_FOR_PROCESSING = 0;
	public static void main(String[] args) throws Exception {
		T24ConnectionFactory.createConnection(MAX_NO_OF_THREADS);
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		executor = Executors.newFixedThreadPool(MAX_NO_OF_THREADS);
		
		while (true)
		{
			if(checkTasks()) {
				if(stopwatch.isRunning()) {					
					stopwatch.stop();
					TOTAL_TIME_FOR_PROCESSING += stopwatch.elapsed(TimeUnit.SECONDS);
					logger.debug("======================================");
					logger.debug("Time Take for complete process " + stopwatch.elapsed(TimeUnit.SECONDS) + " Seconds");
					logger.debug("Time Take for GetDFData process " + DFDataProcessor.totalGetDFDataTime + " Seconds");
					logger.debug("Time Take for Transformer process " + DFDataProcessor.totalTransformerTime + " Seconds");
					logger.debug("Time Take for dbupdate process " + DFDataProcessor.totaldbupdatetime + " Seconds");
					logger.debug("Time Take for UpdateTimePolled process " + DFDataProcessor.totalUpdateTimePolledTime + " Seconds");
					logger.debug("Total Time Take to process  " +TOTAL_NO_OF_PROCESSED_RECORDS +" records is " + formatIntoHHMMSS(TOTAL_TIME_FOR_PROCESSING) + " Seconds");
					logger.debug("======================================");
					
					stopwatch.reset();
					DFDataProcessor.totalGetDFDataTime = 0;
					DFDataProcessor.totalTransformerTime = 0;
					DFDataProcessor.totaldbupdatetime = 0;
					DFDataProcessor.totalUpdateTimePolledTime = 0;
				}
				stopwatch.start();
				List<String> ids = fetchDfDataEventIds();
/*				for(String id:ids){
					logger.debug("ids fetched -" + id);
				}*/
				int batchSize = 100;
				if(ids.size() > 0) {
		/*			for(int i =0; i<ids.size();i += batchSize) {
//						System.out.println(i +"_"+ (i + batchSize -1));
						if((i + batchSize -1) < ids.size()) {
							updateProcessingDataEventIds(ids.subList(i, (i + batchSize -1)) );
						} else {
							updateProcessingDataEventIds(ids.subList(i, (ids.size() -1)) );
						}
					}*/
					processGetDFData(ids);
					TOTAL_NO_OF_PROCESSED_RECORDS +=ids.size();
				} else {
					Thread.sleep(DELAY_TIME_IN_SECONDS * 1000);
				}
			}
		}
	}
	
	private static List<String> fetchDfDataEventIds() {
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
			 query =  "SELECT RECID FROM \"F_DATA_EVENTS\" WHERE TS IS NULL"; // AND ISPROCESSING IS NULL
		 } else {
			 query = "SELECT DF_DATA_EVENT_ID FROM \"F.DF.DATA.EVENTS\" where TIME_POLLED is null";
		 }
		 ResultSet rset = stmt.executeQuery (query); 
		 int i = 1;

		 while (rset.next()) 
		 { 
		 String dataEventId = rset.getString(1); 
		 ids.add(dataEventId);
		 i++;
		 if(i > NO_OF_RECORDS) {
			 break;
		 }

		 } 
		 stmt.close();
		 conn.close();
		} catch (Exception e) {
			logger.error("Error while doing T24 JDBC Call ", e);
		}

		return ids;
	}

	public static void processGetDFData(List<String> ids) {
		logger.debug("===========================");
		logger.debug("Processing "+ids.size()+ " ids started");
		logger.debug("===========================");
		int i = 0;
		DFDataProcessor dataProcessor = new DFDataProcessor();
		for(String id:ids) {
			dataProcessor.addId(id);
			i++;
			if(i==NO_OF_EVENTS_PER_THREAD) {
				i = 0;
				getDFDataResults.add(executor.submit(dataProcessor));
				dataProcessor = new DFDataProcessor();
			}
		}
		if(dataProcessor.ids.size() > 0) {
			getDFDataResults.add(executor.submit(dataProcessor));
		}
	}
	
	
	private static List<String> updateProcessingDataEventIds(List<String> ids) {
		if(ids.size() > 0) {
			try {
				Class.forName(Config.get("df.t24.jdbc.driverclass"));
				Connection conn = DriverManager.getConnection(Config.get("df.t24.jdbc.url"), Config.get("df.t24.jdbc.username"), Config.get("df.t24.jdbc.password"));
				 Statement stmt = conn.createStatement(); 
				 String updateQuery;
				 if(Config.get("df.t24.db").equals("ORACLE")) {
					 updateQuery =  "UPDATE \"F_DATA_EVENTS\" SET ISPROCESSING = CURRENT_TIMESTAMP where RECID in (";
				 } else {
					 updateQuery = "UPDATE \"F.DF.DATA.EVENTS\" SET TIME_POLLED = '1393855502.1297' where DF_DATA_EVENT_ID in (";
				 }
	
				 for(int i = 0; i<ids.size();i++) {
					 if(i == ids.size() -1) {
						 updateQuery = updateQuery + "'"+ids.get(i) + "')";
				 } else {
					 updateQuery = updateQuery + "'"+ids.get(i) + "' ,";
					 }
				 }
				 System.out.println(updateQuery);
				 stmt.execute(updateQuery);
				 
				 stmt.close();
				 conn.close();
			} catch (Exception e) {
				logger.error("Error while updating T24 event records ", e);
			}
		}
		return ids;
	}

	private static boolean checkTasks() throws Exception {
		
		boolean isAllDone = true;
		for(Future getDFDataResult: getDFDataResults) {
			if (!(getDFDataResult == null
					|| getDFDataResult.isDone()
					|| getDFDataResult.isCancelled()))
			{
				isAllDone = false;
			}

		}
		
		return isAllDone;
		
	}
	
	static String formatIntoHHMMSS(int secsIn) 
	{ 

	int hours = secsIn / 3600, 
	remainder = secsIn % 3600, 
	minutes = remainder / 60, 
	seconds = remainder % 60; 

	return ( (hours < 10 ? "0" : "") + hours 
	+ ":" + (minutes < 10 ? "0" : "") + minutes 
	+ ":" + (seconds< 10 ? "0" : "") + seconds ); 

	} 
}

