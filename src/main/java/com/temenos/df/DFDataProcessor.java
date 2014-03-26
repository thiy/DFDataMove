package com.temenos.df;
import java.beans.PropertyVetoException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.base.Stopwatch;
import com.jbase.jremote.JDynArray;
import com.jbase.jremote.JSubroutineParameters;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DFDataProcessor implements Runnable {
    private static Logger logger = Logger.getLogger(DFDataProcessor.class);

    public static final ThreadLocal<Stopwatch> THREAD_LOCAL_SELECT_STOP_WATCH = new ThreadLocal<Stopwatch>();
    public static final ThreadLocal<Stopwatch> THREAD_LOCAL_DIM_STOP_WATCH = new ThreadLocal<Stopwatch>();

	static List<Object> connections = new ArrayList<Object>();

	List<String> ids = new ArrayList<String>();


	public void addId(String dfDataEventId) {
		ids.add(dfDataEventId);
	}
	
	public static long totaldbupdatetime = 0;
	public static long totalTransformerTime = 0;
	public static long totalGetDFDataTime = 0;
	public static long totalUpdateTimePolledTime = 0;

	public void run() {
		try {
			Stopwatch getDFDataStopwatch = Stopwatch.createStarted();
			String dfDataXML = getDFData();
			getDFDataStopwatch.stop();

			THREAD_LOCAL_SELECT_STOP_WATCH.set(Stopwatch.createUnstarted());
			THREAD_LOCAL_DIM_STOP_WATCH.set(Stopwatch.createUnstarted());
			Stopwatch processStopwatch = Stopwatch.createStarted();

			Connection dimDBConnection = DBConnectionFactory.getDimDBConnection();
			Statement dimStatement = dimDBConnection.createStatement();
			Connection selDBConnection = DBConnectionFactory.getSelDBConnection();
			Statement selStatement = selDBConnection.createStatement();

			splitXML(dfDataXML, selStatement, dimStatement);
			
			if(dimDBConnection != null) {
				dimDBConnection.close();
			}
			if(dimStatement != null) {
				dimStatement.close();
			}
			if(selDBConnection != null) {
				selDBConnection.close();
			}
			if(selStatement != null) {
				selStatement.close();
			}
			processStopwatch.stop();
			
			long dbupdatetime = (THREAD_LOCAL_SELECT_STOP_WATCH.get().elapsed(TimeUnit.SECONDS) + THREAD_LOCAL_DIM_STOP_WATCH.get().elapsed(TimeUnit.SECONDS));
			Stopwatch updateTimepolledStopwatch = Stopwatch.createStarted();
			updateTimePooledDfDataEventIds(ids);
			updateTimepolledStopwatch.stop();
			
			synchronized (DFDataProcessor.class) {
				
			if(dbupdatetime > totaldbupdatetime) {
				totaldbupdatetime = dbupdatetime;
			}
			if((processStopwatch.elapsed(TimeUnit.SECONDS) - dbupdatetime) > totalTransformerTime) {
				totalTransformerTime = (processStopwatch.elapsed(TimeUnit.SECONDS) - dbupdatetime);
			}
			if( getDFDataStopwatch.elapsed(TimeUnit.SECONDS) > totalGetDFDataTime) {
				totalGetDFDataTime =  getDFDataStopwatch.elapsed(TimeUnit.SECONDS);
			}
			if(updateTimepolledStopwatch.elapsed(TimeUnit.SECONDS) > totalUpdateTimePolledTime) {
				totalUpdateTimePolledTime = updateTimepolledStopwatch.elapsed(TimeUnit.SECONDS);
			}
			}
			
			
		} catch (Exception e) {
			logger.error("Error while processing the data ---- "+ e.getMessage());

		}
	}
	
	
	public void splitXML(String dfDataXML, Statement selStatement, Statement dimStatement) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document doc;
		NodeList nodes = null;
		try {
			doc = dbf.newDocumentBuilder().parse(
					new InputSource(new StringReader(dfDataXML)));
			XPath xpath = XPathFactory.newInstance().newXPath();

			nodes = (NodeList) xpath.evaluate("//Transaction", doc,
					XPathConstants.NODESET);
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		for (int i = 0; i < nodes.getLength(); i++) {
			try {
				transformAndLoad(nodeToString(nodes.item(i)), dimStatement, selStatement);
			} catch (Exception e) {
				logger.error("Error while processing ", e);
			}
		}

	}
	
	
	private static boolean transformAndLoad(String selectXML, Statement dimStmt, Statement selStmt) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document doc = dbf.newDocumentBuilder().parse(
				new InputSource(new StringReader(selectXML)));
		XPath xpath = XPathFactory.newInstance().newXPath();
//		NodeList searchNodes = (NodeList) xpath.evaluate("//Search", doc,
//				XPathConstants.NODESET);
//		List<String> searchStatements = new ArrayList<String>();
//		solrInsert(searchStatements);
		{
			NodeList selectNodes = (NodeList) xpath.evaluate("//Select", doc, XPathConstants.NODESET);
			List<String> selectStatements = transformUsingXSLT(selectNodes, XSLTTransformer.DFTransformer.SEL_T24_TO_COMMON.getTransformer(), XSLTTransformer.DFTransformer.SEL_COMMON_TO_DML.getTransformer());
			 if(!jdbcInsert(selectStatements, selStmt, "SELECT")) {
//				 return false;
			 }

		}
		{
			 NodeList dimNodes = (NodeList) xpath.evaluate("//Dimension", doc, XPathConstants.NODESET);
			 List<String> dimStatements = transformUsingXSLT(dimNodes, XSLTTransformer.DFTransformer.DIM_T24_TO_COMMON.getTransformer(), XSLTTransformer.DFTransformer.DIM_COMMON_TO_DML.getTransformer());
			 boolean dimFlag = false;
			 boolean factFlag = false;
			 for(String dimSql:dimStatements) {
				 if(dimFlag && factFlag) {
					 break;
				 }
				 if(dimSql.startsWith("INSERT INTO \"FACT")) {
					factFlag = true; 
				 } else {
					 dimFlag = true;
				 }
			 }
			 if(dimFlag && factFlag) {
				 Iterator<String> iterator = dimStatements.iterator();
				 while(iterator.hasNext()) {
					 if(!iterator.next().startsWith("INSERT INTO \"DIM")) {
						 iterator.remove();
					 }
				 }
			 }

			 if(!jdbcInsert(dimStatements, dimStmt, "DIM")) {
//				 return false;
			 }

		}
		return true;

		
	}
	
	private static List<String> transformUsingXSLT(NodeList nodes, Transformer t24ToCommonTransformer, Transformer commonToDBTransformer) throws Exception {
		
		List<String> sqls = new ArrayList<String>();
		for (int i = 0; i < nodes.getLength(); i++) {
			String xml = nodeToxsltString(nodes.item(i), t24ToCommonTransformer);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			Document doc1 = dbf.newDocumentBuilder().parse(
					new InputSource(new StringReader(xml)));
			XPath xpath1 = XPathFactory.newInstance().newXPath();
			NodeList nodes1 = (NodeList) xpath1.evaluate("//table", doc1,
					XPathConstants.NODESET);
			
			for (int j = 0; j < nodes1.getLength(); j++) {
				String sql = nodeToxsltString(nodes1.item(j), commonToDBTransformer);
				sqls.add(sql);
			}
		}
		return sqls;
	}
	
	
	static Statement selStmt = null;
	static Statement dimStmt = null;
	
	private static boolean  jdbcInsert(List<String> insertStatements, Statement stmt, String db) {
		boolean flag = true;
		if("SELECT".equals(db)) {
			THREAD_LOCAL_DIM_STOP_WATCH.get().start();
		} else {
			THREAD_LOCAL_SELECT_STOP_WATCH.get().start();
		}

		try {
			for (String insertStatement:insertStatements) {
				stmt.execute(insertStatement);
			}
		} catch (Exception e) {
			logger.error(insertStatements);
			logger.error("Error while doing DB JDBC call ", e);
			flag = false;
		} finally {
			if("SELECT".equals(db)) {
				THREAD_LOCAL_DIM_STOP_WATCH.get().stop();
			} else {
				THREAD_LOCAL_SELECT_STOP_WATCH.get().stop();
			}
		}
		
		return flag;
	}
	

	private static String nodeToString(Node node) {
		StringWriter sw = new StringWriter();
		try {
			Transformer t = TransformerFactory.newInstance().newTransformer();
			t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			t.transform(new DOMSource(node), new StreamResult(sw));
		} catch (TransformerException te) {
			logger.error("nodeToString Transformer Exception ", te);
		}
		return sw.toString();
	}

	private static String nodeToxsltString(Node node, Transformer xsltTransformer) throws Exception{
		StringWriter sw = new StringWriter();
//		System.out.println(nodeToString(node));
		try {
			DocumentBuilderFactory dbf1 = DocumentBuilderFactory.newInstance();
			Document doc1 = dbf1.newDocumentBuilder().parse(
					new InputSource(new StringReader(nodeToString(node))));
/*			Source xslt = new StreamSource(xsltFile);
			Transformer t = TransformerFactory.newInstance().newTransformer(
					xslt);
			t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
*/			xsltTransformer.transform(new DOMSource(doc1), new StreamResult(sw));

		} catch (TransformerException te) {
			logger.error("nodeToString Transformer Exception ", te);
		}
		return sw.toString();
	}
	
	public String getStringFromDoc(org.w3c.dom.Document doc)    {
        try
        {
           DOMSource domSource = new DOMSource(doc);
           StringWriter writer = new StringWriter();
           StreamResult result = new StreamResult(writer);
           TransformerFactory tf = TransformerFactory.newInstance();
           Transformer transformer = tf.newTransformer();
           transformer.transform(domSource, result);
           writer.flush();
           return writer.toString();
        }
        catch(TransformerException ex)
        {
			logger.error("nodeToString Transformer Exception ", ex);
           return null;
        }
    }

	private String getDFData() throws Exception {
		JSubroutineParameters subroutineRequest = new JSubroutineParameters();
		JDynArray array = new JDynArray();
		int i = 1;
		for(String id:ids) {
			array.insert(id, 1, i++);
		}
		subroutineRequest.add(array);
		subroutineRequest.add(new JDynArray(""));
		subroutineRequest.add(new JDynArray(""));
		JSubroutineParameters response = null;
		try {
//			response = T24ConnectionFactory.getJConnectionFromPool().call("T24DataFrameworkPollingServiceImpl.getDfDataUnique", subroutineRequest);

			response = T24ConnectionFactory.getJConnectionFromPool().call("T24DataFrameworkPollingServiceImpl.getDfDataJDBC", subroutineRequest);
		} catch (Exception e) {
//			logger.error("Exception during getDFData call ", e);
			throw e;
		}
//		System.out.println(response.get(1).get(1));
		StringBuffer buffer = new StringBuffer();
		buffer.append("<database>");
		buffer.append(response.get(1).get(1));
		buffer.append("</database>");
		return buffer.toString();
	}
	
	private List<String> updateTimePooledDfDataEventIds(List<String> ids) {
		try {
			Class.forName(Config.get("df.t24.jdbc.driverclass"));
			Connection conn = DriverManager.getConnection(Config.get("df.t24.jdbc.url"), Config.get("df.t24.jdbc.username"), Config.get("df.t24.jdbc.password"));
			 Statement stmt = conn.createStatement(); 
			 String updateQuery;
			 if(Config.get("df.t24.db").equals("ORACLE")) {
				 updateQuery =  "UPDATE \"F_DATA_EVENTS\" SET TS = CURRENT_TIMESTAMP where RECID in (";
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
			 stmt.execute(updateQuery);
			 
			 stmt.close();
			 conn.close();
		} catch (Exception e) {
			logger.error("Error while updating T24 event records ", e);
		}

		return ids;
	}

}