package com.temenos.df;

import java.io.InputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.log4j.Logger;

public class XSLTTransformer {

	public static enum DFTransformer {
		DIM_T24_TO_COMMON("DIM_T24_TO_COMMON"),SEL_T24_TO_COMMON("SEL_T24_TO_COMMON"),DIM_COMMON_TO_DML("DIM_COMMON_TO_DML"),SEL_COMMON_TO_DML("SEL_COMMON_TO_DML");
		
		String type;	
		Transformer transformer;
		private DFTransformer (String type) {
			if(type.endsWith("DIM_T24_TO_COMMON")) {
				transformer = dimT24ToCommonTransformer;
			} else if(type.endsWith("SEL_T24_TO_COMMON")) {	
				transformer = selectT24ToCommonTransformer;
			} else if(type.endsWith("DIM_COMMON_TO_DML")) {	
				transformer = dimCommonToDBTransformer;
			} else if(type.endsWith("SEL_COMMON_TO_DML")) {	
				transformer = selectCommonToDBTransformer;
			}
		}
		public Transformer getTransformer() {
			return transformer;
		}
		
	}
	enum DB {
		SEARCH, SELECT, DIM;
	}
    private static Logger logger = Logger.getLogger(DFDataProcessor.class);

	private static Transformer dimT24ToCommonTransformer = getxsltTransformer("xslts/dimT24xmlToCommonxml.xslt");
	private static Transformer dimCommonToDBTransformer = getxsltTransformer(DB.DIM);
	
	private static Transformer selectT24ToCommonTransformer = getxsltTransformer("xslts/selectT24xmlToCommonxml.xslt");
	private static Transformer selectCommonToDBTransformer = getxsltTransformer(DB.SELECT);
	

	private static Transformer getxsltTransformer(DB db) {
		
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
	
	private static Transformer getxsltTransformer(String xsltFileName){
		InputStream t24ToCommonXSLT = DFDataProcessor.class.getClassLoader().getResourceAsStream(xsltFileName);
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
