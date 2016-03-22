package edu.uab.ccts.nlp.medics;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;

import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;
import edu.uab.ccts.nlp.medics.util.MedicsConstants;

import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

/**
 * Writes NLP_Clobs to Medics database. If it fails (document present)
 * it updates NLP_Clobs with the document identifier in Medics
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


	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		try {
			uContext = aContext;
			//medicsConnectionString = (String) aContext.getConfigParameterValue(ConfigurationSingleton.PARAM_MEDICS_URL);
			aContext.getLogger().log(Level.INFO,"Medics Doc/Clob Writing URL is: "+medicsConnectionString+"\n");

			//InputStream stream = getContext().getResourceAsStream("clobsInsertSQL");
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream("sql/oracle/insertDocument.sql");
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			StringBuilder sb = new StringBuilder(20000);
			while(br.ready()){
				sb.append(br.readLine()+"\n");
			}
			insertSql=sb.toString();
		} catch (Exception e) {
			aContext.getLogger().log(Level.SEVERE,
			"Can not instanitate MedicsCLOBsConsumer, bad path to sql?"+e.getMessage());
			throw new ResourceInitializationException(e);
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
			docsetId=anal.getAnalysisDataSet();
		} catch (Exception e){
			uContext.getLogger().log(Level.SEVERE,
					"Could not determine the analysis_id or docsetId,"+
			" use AnalysisAnnotator to set this!");
			throw new AnalysisEngineProcessException(e);
		} 

			Collection<NLP_Clobs> mdocs = JCasUtil.select(jcas, NLP_Clobs.class);
			if(mdocs==null || mdocs.size()!=1) {
				getContext().getLogger().log(Level.SEVERE,"Did not find 1 metadocument!");
			}
			thedoc = (NLP_Clobs) mdocs.iterator().next();

		try {
			if(thedoc.getReportID()!=0) {
				uContext.getLogger().log(Level.INFO,
				"Document with source id:"+thedoc.getSourceID()+" has been "+
				"stored in medics database with ID:"+thedoc.getReportID()+
				", exiting not updating...");
				return;
			} else {
				uContext.getLogger().log(Level.INFO,
						"Document with MRN/source id/md5sum:"+thedoc.getMRN()+"/"
				+thedoc.getSourceID()+"/"+thedoc.getMd5Sum()+" and URI "+
						thedoc.getURL()+" has NOT been "+
						"stored in medics database, writing..."); 
				int docid = insertDocument(logger,thedoc.getSourceID(),Integer.toString(thedoc.getMRN()),
				convertStringToSqlDate(thedoc.getDateOfService(),"yyyy-MM-dd"),thedoc.getSource(),
				thedoc.getDocumentTypeAbbreviation(),thedoc.getDocumentSubType(),
				null,thedoc.getDocumentVersion(), jcas.getDocumentText(), thedoc.getURL(),
				thedoc.getMd5Sum());
				thedoc.setReportID(docid);
			}
		} catch (Exception e) {
			throw(new AnalysisEngineProcessException(e));
		}
		logger.log(Level.FINE,"Finished processing with MedicsCLOBsConsumer");
	}


	private int insertDocument(Logger logger, String source_id,
			String mrn,java.sql.Date dn_update_datetime, String source,
			String type, String subtype, String mimetype, Integer version,
			String converted_doc, String docurl, String md5sum)
					throws SQLException, NoSuchAlgorithmException, UnsupportedEncodingException {
		int documentIdentifier = -1;
		if(mimetype==null) mimetype="text";
		String doc_metadata = "document source id:"+source_id+
				" from source "+source+" with version "+version+
				" and md5sum "+md5sum;
		try (
				Connection conn =  DriverManager.getConnection(medicsConnectionString);
				PreparedStatement preparedStatement = conn.prepareStatement(insertSql, new String[]{"NC_REPORTID"})
			){
			preparedStatement.setString(1, source_id );
			preparedStatement.setString(2, mrn );
			preparedStatement.setDate(3, dn_update_datetime );
			preparedStatement.setString(4, source);
			preparedStatement.setString(5, type );
			preparedStatement.setString(6, subtype );
			preparedStatement.setInt(7, version );
			preparedStatement.setClob(8, new StringReader(converted_doc));
			preparedStatement.setString(9, md5sum );
			preparedStatement.setInt(10, analysisId );
			preparedStatement.setTime(11, getCurrentTime() );
			preparedStatement.setString(12, docurl );
			preparedStatement.executeUpdate();
			ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
			generatedKeys.next();
			documentIdentifier = generatedKeys.getInt(1);
			generatedKeys.close();
			LegacyMedicsTools.insertDocumentHistory(conn,documentIdentifier,analysisId,1);
			LegacyMedicsTools.insertDocumentDocSetMap(conn,docsetId,documentIdentifier,mrn);
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
				logger.log(Level.INFO,"Trying to insert duplicate document "+documentIdentifier);
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



	/**
	 * May need this if doing 
	 * @return
	 */
	private java.sql.Time getCurrentTime(){
		java.util.Date today = new java.util.Date();
		return new java.sql.Time(today.getTime());		
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
