package com.temenos.df;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

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

public class XSLTTransformer {

/*	public static enum DFTransformer {
		DIM_T24_TO_COMMON("DIM_T24_TO_COMMON"),
		SEL_T24_TO_COMMON("SEL_T24_TO_COMMON"),
		DIM_COMMON_TO_DML("DIM_COMMON_TO_DML"),
		SEL_COMMON_TO_DML("SEL_COMMON_TO_DML"),
		T24_COMMON_TO_SOLR("T24_COMMON_TO_SOLR");
		
		String type;	
		Transformer transformer;
		private DFTransformer (String type) {
			if(type.equals("DIM_T24_TO_COMMON")) {
				transformer = dimT24ToCommonTransformer;
			} else if(type.equals("SEL_T24_TO_COMMON")) {	
				transformer = selectT24ToCommonTransformer;
			} else if(type.equals("DIM_COMMON_TO_DML")) {	
				transformer = dimCommonToDBTransformer;
			} else if(type.equals("SEL_COMMON_TO_DML")) {	
				transformer = selectCommonToDBTransformer;
			} else if(type.equals("T24_COMMON_TO_SOLR")) {	
				transformer = solrCommonToDBTransformer;
			}
		}
		public Transformer getTransformer() {
			return transformer;
		}
		
	}*/
	
	public static void main(String[] args) {
		System.out.println("1-------");
		String xmlFile = "C:\\Users\\tselvaraj\\workspace1\\T24DataModelResearch\\sam.xml";
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document doc;
		NodeList nodes = null;
		try {
			InputStream inputStream= new FileInputStream(xmlFile);
			Reader reader = new InputStreamReader(inputStream,"UTF-8");

			doc = dbf.newDocumentBuilder().parse(
					new InputSource(reader));
			XPath xpath = XPathFactory.newInstance().newXPath();

			nodes = (NodeList) xpath.evaluate("//Transaction", doc,
					XPathConstants.NODESET);
			for (int i = 0; i < nodes.getLength(); i++) {
				String xml = nodeToxsltString(nodes.item(i), getxsltTransformer("xslts/dimT24xmlToCommonxml.xslt"));
				System.out.println(xml);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public enum DB {
		SEARCH, SELECT, DIM;
	}
    private static Logger logger = Logger.getLogger(DFDataProcessor.class);

/*	private static Transformer dimT24ToCommonTransformer = getxsltTransformer("xslts/dimT24xmlToCommonxml.xslt");
	private static Transformer dimCommonToDBTransformer = getxsltTransformer(DB.DIM);
	
	private static Transformer selectT24ToCommonTransformer = getxsltTransformer("xslts/selectT24xmlToCommonxml.xslt");
	private static Transformer selectCommonToDBTransformer = getxsltTransformer(DB.SELECT);
	private static Transformer solrCommonToDBTransformer = getxsltTransformer("xslts/t24xmlToSolrxml.xslt");
*/	

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


	public static Transformer getxsltTransformer(DB db) {
		
		switch (db){
		case SELECT:
			 if(Config.get("df.sel.db").equals("ORACLE")) {
				 return getxsltTransformer("xslts/OracleSelectCommonXmlToDMLStatements.xslt");
			 } else if(Config.get("df.sel.db").equals("MSSQL")) {
				 return getxsltTransformer("xslts/MSSqlSelectCommonXmlToDMLStatements.xslt");
			 }
			 break;
		case DIM:
			 if(Config.get("df.dim.db").equals("ORACLE")) {
				 return getxsltTransformer("xslts/OracleDimCommonXmlToDMLStatements.xslt");
			 } else if(Config.get("df.dim.db").equals("MSSQL")) {
				 return getxsltTransformer("xslts/MSSqlDimCommonXmlToDMLStatements.xslt");
			 }
			 break;
		default:
			break;
		}
		
		return null;
		
	}
	
	public static Transformer getxsltTransformer(String xsltFileName){
		InputStream t24ToCommonXSLT = XSLTTransformer.class.getClassLoader().getResourceAsStream(xsltFileName);
		Source xslt = new StreamSource(t24ToCommonXSLT);
		Transformer t = null;
		try {
			t = TransformerFactory.newInstance().newTransformer(
					xslt);
		} catch (Exception e) {
			logger.error("Transformer Exception ", e);
		}
		t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

		return t;
	}
	
	
}
