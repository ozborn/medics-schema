package edu.uab.ccts.nlp.medics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLRecoverableException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.AnalysisEngineUtils;
import org.apache.uima.util.InvalidXMLException;

import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;
import edu.uab.ccts.nlp.medics.util.MedicsConstants;

import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

/**
 * Writes the document obtained from the CollectionReader to the Medics Database
 * OracleCollectionReader does this when it is being used
 * @author AD\josborne
 *
 */
public class MedicsCLOBsConsumer extends JCasAnnotator_ImplBase {
	public static final String PARAM_MEDICS_URL = "medicsConnectionString";
	public final Integer MEDICS_DOCUMENT_DEFAULT_DATE= 0;

	public UimaContext uContext = null;
	String insertSql = null;
	int analysisId = 0;
	int docsetId = 0;


	@ConfigurationParameter(
			name = PARAM_MEDICS_URL,
			description = "Medics URL for writing documents"
			)
	private String medicsConnectionString = null;


	public static AnalysisEngineDescription createAnnotatorDescription() 
			throws InvalidXMLException, IOException, ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(
				MedicsCLOBsConsumer.class);
	}


	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		try {
			uContext = aContext;
			//medicsConnectionString = (String) aContext.getConfigParameterValue(ConfigurationSingleton.PARAM_MEDICS_URL);
			aContext.getLogger().log(Level.INFO,"Medics Doc/Clob Writing URL is: "+medicsConnectionString+"\n");

			//Class.forName("oracle.jdbc.driver.OracleDriver");
			//InputStream stream = getContext().getResourceAsStream("clobsInsertSQL");
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream("sql/oracle/insertDocument.sql");
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			StringBuilder sb = new StringBuilder(20000);
			while(br.ready()){
				sb.append(br.readLine()+"\n");
			}
			insertSql=sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ResourceInitializationException(e);
		}
	}



	/**
	 * @deprecated
	 * @param con
	 * @param logger
	 */
	public void close(Connection con, Logger logger){
		try {
			if (con != null) { con.close(); 
			} else {
				uContext.getLogger().log(Level.WARNING,"No medicsConnectionString Connection to close");
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE,"Exception CLOSING connection to "+
					PARAM_MEDICS_URL+" to write extended hits.");
			e.printStackTrace();
		}
	}

	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		Logger logger = uContext.getLogger();
		NLP_Analysis anal = null;
		NLP_Clobs thedoc = null;

		try {
			FSIndex<Annotation> analIndex = jcas.getAnnotationIndex(NLP_Analysis.type);
			Iterator<Annotation> analIter = analIndex.iterator();
			anal = (NLP_Analysis) analIter.next();
			analysisId=anal.getAnalysisID();
			if(anal.getAnalysisDataSet()!=null) docsetId=Integer.parseInt(anal.getAnalysisDataSet());
		} catch (Exception e){
			uContext.getLogger().log(Level.SEVERE,
					"Could not determine the analysis_id or docsetId,"+
			" use AnalysisAnnotator to set this!");
			throw new AnalysisEngineProcessException(e);
		} 

		try {
			FSIndex<Annotation> medIndex = jcas.getAnnotationIndex(NLP_Clobs.type);
			Iterator<Annotation> medIter = medIndex.iterator();
			thedoc = (NLP_Clobs) medIter.next();
		} catch (Exception e) {
			uContext.getLogger().log(Level.SEVERE,
			"No medics document/clob annotation in "+jcas.getViewName()+", use"+
			" DocumentMetaDataAnnotator to create default MetaData!");
			throw new AnalysisEngineProcessException(e);
		}

		try {
			if(thedoc.getReportID()!=0) {
				uContext.getLogger().log(Level.INFO,
				"Document with source id:"+thedoc.getSourceID()+" has been "+
				"stored in medics database with ID:"+thedoc.getReportID()+
				", exiting not updating...");
				return;
			} else {
				uContext.getLogger().log(Level.FINE,
						"Document with source id:"+thedoc.getSourceID()+" has NOT been "+
						"stored in medics database, writing..."); 
				insertDocument(logger,thedoc.getSourceID(),Integer.toString(thedoc.getMRN()),
				convertStringToSqlDate(thedoc.getDateOfService(),"yyyy-MM-dd"),thedoc.getSource(),
				thedoc.getDocumentTypeAbbreviation(),thedoc.getDocumentSubType(),
				null,thedoc.getDocumentVersion(), jcas.getDocumentText(),analysisId);
			}
		} catch (Exception e) {
			throw(new AnalysisEngineProcessException(e));
		}
		logger.log(Level.FINE,"Finished processing with MedicsCLOBsConsumer");
	}


	/**
	 * @deprecated
	 * @param doctext
	 * @param thedoc
	 * @param logger
	 * @param mcon
	 * @throws AnalysisEngineProcessException
	 */
	public void writeDocument(String doctext,NLP_Clobs thedoc,
			Logger logger, JCas jcas) 
					throws AnalysisEngineProcessException {
		PreparedStatement insertdoc = null;
		Connection mcon = null;
		String md5sum="";
		try {
			try {
				mcon =  DriverManager.getConnection(medicsConnectionString);
			} catch (SQLRecoverableException e) {
				String cp = System.getProperty("java.class.path");
				logger.log(Level.WARNING,"Recoverable exception getting connection to "+
						medicsConnectionString+" , trying again to write extended CLOBS with classpath"+cp);
				Thread.sleep(1000);
				mcon =  DriverManager.getConnection(medicsConnectionString);
			} catch (Exception e) {
				String cp = System.getProperty("java.class.path");
				logger.log(Level.SEVERE,"Not recoverable exception getting connection to "+
						medicsConnectionString+" to write extended CLOBS with classpath"+cp);
				throw(new AnalysisEngineProcessException(e));
			}
			MessageDigest algorithm = MessageDigest.getInstance("MD5");
			algorithm.reset();
			algorithm.update(doctext.getBytes("UTF-8")); //Close enough to Oracle AL32UTF8
			byte[] md5bytes = algorithm.digest();
			BigInteger bigint = new BigInteger(1,md5bytes);
			md5sum = bigint.toString(16);
			insertdoc = mcon.prepareStatement(insertSql);
			insertdoc.setInt(1, thedoc.getReportID());
			insertdoc.setClob(2, new StringReader(doctext));
			insertdoc.setString(3, (new Integer(thedoc.getMRN())).toString());
			insertdoc.setDate(4,convertStringToSqlDate(thedoc.getDateOfService(),"yyyy-MM-dd"));
			insertdoc.setString(5, thedoc.getSource());
			insertdoc.setString(6, thedoc.getDocumentTypeAbbreviation());
			insertdoc.setString(7, thedoc.getDocumentSubType());
			insertdoc.setInt(8, thedoc.getDocumentVersion());
			insertdoc.setTimestamp(9, getCurrentTimestamp());
			insertdoc.setString(10, thedoc.getSourceID());
			insertdoc.setString(11, md5sum);
			insertdoc.setInt(12, analysisId);
			insertdoc.addBatch();
			insertdoc.executeBatch();
			if(insertdoc!=null) insertdoc.close();

		} catch (java.sql.BatchUpdateException bue) {
			if(bue.getMessage()!=null && bue.getMessage().indexOf("_PK) violated")!=-1) {
				logger.log(Level.WARNING,thedoc.getReportID()+" with md5sum ("+
						md5sum+") already exists in medics.NLP_CLOBS");
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE,"Failed to write document with id "+thedoc.getReportID()+
					" and md5sum ("+md5sum+")");
			try {
				LegacyMedicsTools.updateDocumentHistory(mcon,
						thedoc.getReportID(), analysisId, MedicsConstants.DOCUMENT_DATABASE_WRITE_FAIL);
			} catch (Exception wf) {
				wf.printStackTrace();
				logger.log(Level.SEVERE,thedoc.getReportID()+" could not update document history");
			}
		} finally { 
			close(mcon,logger); 
		}
	}

	private int insertDocument(Logger logger, String source_id,
			String mrn,java.sql.Date dn_update_datetime, String source,
			String type, String subtype, String mimetype, Integer version,
			String converted_doc,int analysis_id)
					throws SQLException, NoSuchAlgorithmException, UnsupportedEncodingException {
		int documentIdentifier = -1;
		if(mimetype==null) mimetype="text";
		String md5sum ="";
		MessageDigest algorithm;
		/*
		String insertTableSQL = "INSERT INTO NLP_DOCS"
				+" (NC_SOURCE_ID,NC_PT_MRN,NC_DOS,NC_SOURCE,NC_TYPE,NC_SUBTYPE,"+
				"NC_VERSION, NC_CLCONT, NC_MD5_DOC_HASH, NC_IMPORT_ANALYSIS,NC_MEDICS_ARRIVAL_TIME) "+
				"VALUES (?,?,?,?,?,?,?,?,?,?,SYSDATE) ";
		*/
		String doc_metadata = "document source id:"+source_id+
				" from source "+source+" with version "+version;
		try (
				Connection conn =  DriverManager.getConnection(medicsConnectionString);
				PreparedStatement preparedStatement = conn.prepareStatement(insertSql, new String[]{"NC_REPORTID"})
			){
			algorithm = MessageDigest.getInstance("MD5");
			algorithm.reset();
			algorithm.update(converted_doc.getBytes("UTF-8")); //Close enough to Oracle AL32UTF8
			byte[] md5bytes = algorithm.digest();
			BigInteger bigint = new BigInteger(1,md5bytes);
			String md5nolead = bigint.toString(16);
			md5sum = ("00000000000000000000000000000000"+md5nolead).substring(md5nolead.length());
			doc_metadata += " and md5sum "+md5sum;

			preparedStatement.setString(1, source_id );
			preparedStatement.setString(2, mrn );
			preparedStatement.setDate(3, dn_update_datetime );
			preparedStatement.setString(4, source);
			preparedStatement.setString(5, type );
			preparedStatement.setString(6, subtype );
			preparedStatement.setInt(7, version );
			preparedStatement.setClob(8, new StringReader(converted_doc));
			preparedStatement.setString(9, md5sum );
			preparedStatement.setInt(10, analysis_id );
			preparedStatement.executeUpdate();
			ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
			generatedKeys.next();
			documentIdentifier = generatedKeys.getInt(1);
			generatedKeys.close();
		} catch (SQLIntegrityConstraintViolationException sql_integrity) {
			//Trying to insert duplicate document
			try (
					Connection conn =  DriverManager.getConnection(medicsConnectionString);
					PreparedStatement pst = conn.prepareStatement
					("SELECT NC_REPORTID FROM NLP_DOCS WHERE "+
							"NC_SOURCE=? AND NC_SOURCE_ID=? AND "+
							"NC_VERSION=? AND NC_MD5_DOC_HASH=?")) {
				pst.setString(1,source);
				pst.setString(2,source_id);
				pst.setInt(3,version);
				pst.setString(4,md5sum);
				ResultSet resultset = (ResultSet) pst.executeQuery();
				resultset.next();
				documentIdentifier = resultset.getInt(1);
				resultset.close();
				LegacyMedicsTools.updateDocumentHistory(conn,
						documentIdentifier, analysisId, MedicsConstants.DOCUMENT_DATABASE_WRITE_SUCCESS);
				
			} catch (SQLException e) {
				logger.log(Level.WARNING,doc_metadata+
				" - could not find document that violated constraints for "
				+source_id+" ; "+e.getMessage());
				throw (e);				
			}
		} catch (Exception fail) {
			logger.log(Level.WARNING,"Initial failed query was:"+insertSql+
					"\nWith values: source_id:"+source_id+" mrn:"+mrn+
					" dos:"+dn_update_datetime+" type:"+type+" subtype:"+subtype+
					" version:"+version+" md5sum: "+md5sum);
			throw new SQLException(fail);
		}
		if(documentIdentifier==-1) logger.log(Level.WARNING,"Got bad (-1) value for "+
		" generated medics document identifier");
		return documentIdentifier;
	}



	private java.sql.Timestamp getCurrentTimestamp(){
		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());		
	}


	protected java.sql.Date convertStringToSqlDate(String input_date, String format) 
			throws ParseException {
		java.sql.Date sdate = null;
		try {
			DateFormat df = new SimpleDateFormat(format); 
			java.util.Date date = new Date(0);
			if(input_date!=null) date = df.parse(input_date);
			sdate = new java.sql.Date(date.getTime());
		} catch (Exception pe) {
			uContext.getLogger().log(Level.WARNING,"Failed to parse date:"+input_date+
			"with format "+format);
			pe.printStackTrace();
			throw pe;
		}
		return sdate;
	}
	

	
	public static AnalysisEngineDescription createAnnotatorDescription(
			String dbUrl) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(MedicsCLOBsConsumer.class,
				PARAM_MEDICS_URL,
				dbUrl);
	}

}
