package edu.uab.ccts.nlp.medics;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.component.ViewCreatorAnnotator;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.apache.uima.resource.ResourceInitializationException;

import edu.uab.ccts.nlp.medics.util.MedicsConstants;


/**
 * Upon initialization, collection identifiers from database
 * to read. Identifiers are stored in the defaultCAS under the
 * DOCUMENT_IDENTIFIER_VIEW 
 * Documents should be selected as either CLOB_DOCUMENT or
 * BLOB_DOCUMENT
 * Does not populate metadata...
 * @author AD\josborne
 *
 */
public class DatabaseIdentifierCollectionReader extends CasCollectionReader_ImplBase {

	public static final String PARAM_DOCID_SQL = "idSql";
	public static final String PARAM_JDBC_CONNECTION_STRING = "jdbcString";

	@ConfigurationParameter(
			name = PARAM_DOCID_SQL,
			mandatory = true,
			description = "SQL to retrieve list of document ids to query"
					+ " where document id is selected as document_id")
	String docIdSql = null;
	String sql = null;
	@ConfigurationParameter(
			name = PARAM_JDBC_CONNECTION_STRING,
			mandatory = false,
			description = "SQL to retrieve list of documents to analyze")
	String jdbcString = null;

	Integer currentDoc=0, totalDocs=0;

	UimaContext uc = null;
	List<String> documentIds; //Unique document identifier in database

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		System.out.println("Logging config file can be found at:"+
				System.getProperty("java.util.logging.config.file"));
		System.out.flush();
		uc = context;

		if(!(jdbcString.toLowerCase().startsWith("jdbc"))) {
			System.err.println("JDBC URL is expected to start with jdbc");
		}

		documentIds = new ArrayList<String>(); 
		try (Connection con =  DriverManager.getConnection(jdbcString);
				Statement st = con.createStatement();
				ResultSet rs = st.executeQuery(docIdSql)){
			while(rs.next()) {
				documentIds.add(rs.getString("document_id"));
			}
			totalDocs = documentIds.size();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ResourceInitializationException();
		}
	}



	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		try {
			JCas jdbcView = ViewCreatorAnnotator.createViewSafely(aCAS.getJCas(), MedicsConstants.JDBC_VIEW);
			JCas docidview = ViewCreatorAnnotator.createViewSafely(aCAS.getJCas(), MedicsConstants.DOCUMENT_IDENTIFIER_VIEW);
			jdbcView.setDocumentText(jdbcString);
			docidview.setDocumentText(documentIds.get(currentDoc));
			currentDoc++;
		} catch (Exception e) { throw new CollectionException(e); }
	}


	@Override
	/** Returns progress
	 */
	public Progress[] getProgress() {
		return new Progress[]{
				new ProgressImpl(currentDoc,totalDocs,Progress.ENTITIES)};
	}


	@Override
	public boolean hasNext() {
		if(currentDoc>=totalDocs) return false;
		return true;
	}

	
	public static CollectionReaderDescription getDescriptionFromJDBC(
			String jdbc, String sql) throws ResourceInitializationException {
		return CollectionReaderFactory.createReaderDescription(
				DatabaseIdentifierCollectionReader.class, 
				PARAM_JDBC_CONNECTION_STRING,jdbc,
				PARAM_DOCID_SQL,sql
		);
	}

}
