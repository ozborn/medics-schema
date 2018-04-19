package edu.uab.ccts.nlp.medics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.component.ViewCreatorAnnotator;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

import edu.uab.ccts.nlp.medics.util.MedicsConstants;
import edu.uab.ccts.nlp.medics.util.MedicsTools;
import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

/**
 * Requires MedicsConstants.DOCUMENT_IDENTIFIER_VIEW and
 * MedicsConstants.JDBC_VIEW to be populated
 * 
 * Uses JDBC_VIEW to get JDBC Connection String
 * 
 * Creates a UTF16View with the Document stored as a Java UTF16 String
 * Generates based on parameters some metadata about the document being analyzed
 * which can be saved in the Medics schema
 * Generic code to interrogate the document for metadata should go here
 * @author josborne
 *
 */
public class DatabaseUTF16DocumentAnnotator extends JCasAnnotator_ImplBase {

	public static final String PARAM_DOC_SQL ="documentPreparedQuerySql";

	public static final String PARAM_DOC_COLUMN ="docColumn";
	public static final String PARAM_MRN_COLUMN = "mrnColumn";
	public static final String PARAM_DOC_VERSION_COLUMN = "versionColumn";
	public static final String PARAM_SOURCE_COLUMN = "sourceColumn";
	public static final String PARAM_DOC_TYPE_COLUMN = "typeColumn";
	public static final String PARAM_DOC_SUBTYPE_COLUMN = "subtypeColumn";
	public static final String PARAM_DOCUMENT_CREATION_DATE_COLUMN = "creationDateColumn";

	public static final String PARAM_DOCSET_ID = "docSetID";
	public static final String PARAM_IMPORT_ANALYSIS_ID = "importAnalysisId";


	@ConfigurationParameter(
			name = PARAM_DOC_SQL,
			mandatory = true,
			description = "SQL usable for prepared statement creation where "
					+ "a single ? corresponds to the unique document identifier")
	protected String documentPreparedQuerySql = null;

	@ConfigurationParameter(
			name = PARAM_DOC_COLUMN,
			mandatory = true,
			description = "Name of column containing document")
	protected String docColumn = null;


	@ConfigurationParameter(
			name = PARAM_MRN_COLUMN,
			mandatory = false,
			description = "Name of column containing patient identifier/mrn")
	protected String mrnColumn = null;

	@ConfigurationParameter(
			name = PARAM_DOC_VERSION_COLUMN,
			mandatory = false,
			description = "Document version column name")
	protected String versionColumn;

	@ConfigurationParameter(
			name = PARAM_SOURCE_COLUMN,
			mandatory = false,
			description = "Column name containing source of document, "
					+"ex) icda, cflo, semeval")
	protected String sourceColumn = null;

	@ConfigurationParameter(
			name = PARAM_DOC_TYPE_COLUMN,
			mandatory = false,
			description = "Name of column in SQL containing document type")
	protected String typeColumn = null;

	@ConfigurationParameter(
			name = PARAM_DOC_SUBTYPE_COLUMN,
			mandatory = false,
			description = "Name of column in SQL containing document subtype")
	protected String subtypeColumn = null;

	@ConfigurationParameter(
			name = PARAM_DOCUMENT_CREATION_DATE_COLUMN,
			mandatory = false,
			description = "Document creation or signing date column")
	protected String creationDateColumn = null;

	@ConfigurationParameter(
			name = PARAM_DOCSET_ID,
			mandatory = false,
			description = "Document version")
	protected int docsetId= 0;

	@ConfigurationParameter(
			name = PARAM_IMPORT_ANALYSIS_ID,
			mandatory = false,
			description = "Import analysis that generated this document.")
	protected int importAnalysisId;



	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		String jdbcString=null, id=null;
		JCas utfView = null;
		try {
			JCas jdbcView = jcas.getView(MedicsConstants.JDBC_VIEW);
			JCas docidview = jcas.getView(MedicsConstants.DOCUMENT_IDENTIFIER_VIEW);
			utfView = ViewCreatorAnnotator.createViewSafely(jcas, MedicsConstants.UTF_VIEW);
			jdbcString = jdbcView.getDocumentText();
			id = docidview.getDocumentText();
		} catch (Exception ejcas) {
			this.getContext().getLogger().log(Level.SEVERE,
					"Could not retrieve JDBC string or document id");
			ejcas.printStackTrace();
			throw new AnalysisEngineProcessException(ejcas);
		}
		this.getContext().getLogger().log(Level.FINE,"jdbcstring:"+jdbcString+" docid:"+id);
		
		NLP_Clobs pprop =  new NLP_Clobs(utfView);
		String doc = getDocumentData(utfView, jdbcString, id, pprop);
		utfView.setDocumentText(doc);
		pprop.addToIndexes();
		this.getContext().getLogger().log(Level.INFO,
				"Set Document with ID:"+id+" with MRN "+pprop.getMRN() +
				" at JDBC URL:"+ jdbcString+ " in view "+utfView.getViewName());
	}


	/**
	 * Does not calculate md5sum for UTF16 documents
	 * @param jcas
	 * @param jdbcString
	 * @param id
	 * @param pprop
	 * @return
	 * @throws AnalysisEngineProcessException
	 */
	private String getDocumentData(JCas jcas, String jdbcString, String id,
			NLP_Clobs pprop)
					throws AnalysisEngineProcessException {
		String doc = null;

		try (Connection con =  DriverManager.getConnection(jdbcString);
				PreparedStatement prepst = createPreparedStatement(con, id);
				ResultSet rs = prepst.executeQuery()){
			int doc_count=0;
			while(rs.next()) {
				doc_count++;
				if(doc_count>1) break;
				doc = populateNlpClobs(jcas, pprop, rs);
			}
			if(doc_count!=1) {
				this.getContext().getLogger().log(Level.WARNING,
						"Multiple documents found for document id:"+id+" in "+jdbcString);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new AnalysisEngineProcessException(e);
		}
		return doc;
	}


	private String getDocumentString(ResultSet rs, String docColumn) {
		String doc = null, type=null;
		MedicsTools mt = new MedicsTools();
		try {
			type = getDocumentStorageType(rs, docColumn);
			switch(type){
			case ("java.sql.Clob"):
				doc = mt.fetchDoc(rs.getClob(docColumn));
			break;
			case("java.sql.Blob"):
				doc = mt.fetchDoc(rs.getBlob(docColumn));
			break;
			default: doc = rs.getString(docColumn);
			}
		} catch (Exception e) {
			this.getContext().getLogger().log(Level.WARNING,
					"Failed to retrieve document from column "+docColumn+
					" stored as a "+type);
			e.printStackTrace();
		}
		return doc;

	}


	private String getDocumentStorageType(ResultSet rs, String docColumnName) 
			throws SQLException {
		String type = null;
		ResultSetMetaData metaData = rs.getMetaData();
		int columncount = metaData.getColumnCount();
		for(int i=1;i<= columncount;i++) {
			String colname = metaData.getColumnName(i);
			if(colname.equalsIgnoreCase(docColumnName)) {
				type = metaData.getColumnTypeName(i);
				break;
			}

		}
		return type;
	}


	/**
	 * 
	 * @param jcas
	 * @param pprop
	 * @param rs
	 * @return The Java UTF16 String representing the document
	 * @throws SQLException
	 */
	private String populateNlpClobs(JCas jcas, NLP_Clobs pprop, ResultSet rs) throws SQLException {
		String doc = getDocumentString(rs, docColumn);
		if(mrnColumn!=null) pprop.setMRN(rs.getInt(mrnColumn));
		if(versionColumn!=null) {
		String version = null;
		try {
			version = rs.getString(versionColumn);
		} catch (SQLException sqe) {
			version = (new Integer(rs.getInt(versionColumn))).toString();
		}
			pprop.setDocumentVersion(Integer.parseInt(version));
		}
		if(sourceColumn!=null)pprop.setSource(rs.getString(sourceColumn));
		if(typeColumn!=null)pprop.setDocumentTypeAbbreviation(rs.getString(typeColumn));
		if(subtypeColumn!=null)pprop.setDocumentSubType(rs.getString(subtypeColumn));

		Collection<NLP_Analysis> anals = JCasUtil.select(jcas, NLP_Analysis.class);
		if(anals!=null && anals.size()==1) {
			importAnalysisId = anals.iterator().next().getAnalysisID();
			pprop.setImportAnalysis(importAnalysisId);
		} else if(importAnalysisId>0) {
			pprop.setImportAnalysis(importAnalysisId);
		} else this.getContext().getLogger().log(Level.CONFIG,"Analysis ID could not be determined");
		return doc;

	}




	private PreparedStatement createPreparedStatement(Connection conn,
			String userid) throws SQLException {
		this.getContext().getLogger().log(Level.INFO,documentPreparedQuerySql);
		PreparedStatement ps = conn.prepareStatement(documentPreparedQuerySql);
		ps.setString(1, userid);
		return ps;
	}
	
	
	public static String getDefaultMedicsDocPreparedStatementSql() {
		return
				"SELECT " + 
				"NC_REPORTID, " + 
				",NC_CLCONT as docColumn" + 
				",NC_DOS as creationDateColumn" + 
				",NC_PT_MRN as mrnColumn" + 
				",NC_TYPE as typeColumn" + 
				",NC_SUBTYPE as subtypeColumn" + 
				",NC_SOURCE as source" + 
				" FROM NLP_DOCS" + 
				" WHERE NC_REPORTID=?";
	}


	public static AnalysisEngineDescription createAnnotatorDescription(
			String sql, String docCol,
			String type, String ver, String src) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(DatabaseUTF16DocumentAnnotator.class,
				PARAM_DOC_SQL,
				sql,
				PARAM_DOC_COLUMN,
				docCol,
				PARAM_DOC_TYPE_COLUMN,
				type,
				PARAM_DOC_VERSION_COLUMN,
				ver,
				PARAM_SOURCE_COLUMN,
				src
				);
	}


	public static AnalysisEngineDescription createAnnotatorDescription(
			String sql) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(DatabaseUTF16DocumentAnnotator.class,
				PARAM_DOC_SQL,
				sql);
	}

}
