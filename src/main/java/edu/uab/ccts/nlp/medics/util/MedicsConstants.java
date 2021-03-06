package edu.uab.ccts.nlp.medics.util;

import org.apache.uima.cas.CAS;

/**
 * Medics Related configuration information, should externalize to property file
 * @author josborne
 *
 */
public class MedicsConstants {
	public final static double ANALYSIS_DOC_PASS_RATE_REQUIRED=90.0; //Percent of documents required
	
	public static final int ANALYSIS_FAIL_STATUS=1;
	public static final int ANALYSIS_PARTIAL_STATUS=2;
	public static final int ANALYSIS_SUCCESS_STATUS=3;
	public static final int ANALYSIS_EMPTY_STATUS=4;
	public static final int ANALYSIS_MOSTLY_WORKED_STATUS=5;
	public static final int ANALYSIS_INITATED_STATUS=6;

	public static final int DOCUMENT_PROCESSING_INITIATED=1;
	public static final int DOCUMENT_CONVERTED=2;
	public static final int DOCUMENT_DATABASE_WRITE_SUCCESS=16; // Wrote document to Medics NLP_DOCS
	public static final int DOCUMENT_ANALYSIS_COMPLETE=3;
	public static final int DOCUMENT_CONVERSION_FAIL=5;
	public static final int DOCUMENT_METAMAP_OTHER_PROCESS_FAIL=6;
	public static final int DOCUMENT_METAMAP_SERVER_DOWN=7;
	public static final int DOCUMENT_METAMAP_PROLOG_EXPRESSION_FAIL=8;
	public static final int DOCUMENT_METAMAP_DOCTEXT_FAIL=9;
	public static final int DOCUMENT_DOS_INSERT_FAIL=10;
	public static final int DOCUMENT_METAMAP_OPTION_SETUP_FAILURE=11;
	public static final int DOCUMENT_METAMAP_PROCESS_NOT_FOUND=12;
	public static final int DOCUMENT_NULL_BLOB_FAILURE=13;
	public static final int DOCUMENT_DONE_ELSEWHERE=14; //- Document done in another equivalent analysis
	public static final int DOCUMENT_DATABASE_WRITE_FAIL=15; // Failed to write document to Medics Database
	
	public static final int DEFAULT_DOCUMENT_MRN_SENTINEL_VALUE=0;
	public static final int DEFAULT_DOCUMENT_VERSION_SENTINEL_VALUE= 0;
	public static final int DEFAULT_ANALYSIS_SENTINEL_VALUE= 0;
	public static final String DEFAULT_ANALYSIS_VERSION= "0";

	public final static int MAX_ORACLE_VARCHAR2=3999;
	public final static int MAX_ORACLE_WHERECLAUSE=500;
	
	
	//Due to an unfortunate shortcut, analysis types use negative integers
	public static final int EMPTY_ANALYSIS=-15;
	public static final int COPD_ANALYSIS=-14;
	public static final int WORD2VEC_CUI_MODEL_CREATION_ANALYSIS=-13;
	public static final int EEG_SEIZURE_ANALYSIS=-12;	
	public static final int WORD2VEC_MODEL_CREATION_ANALYSIS=-11;
	public static final int UPDATE_HASHCODE_ANALYSIS=-10;
	public static final int BONE_LESION_DETECTION_ANALYSIS_ID=-9;
	public static final int SHARECLEF_2014_POST_COORDINATION_ANALYSIS=-8;
	public static final int SHARECLEF_2014_ANALYSIS=-7;
	public static final int SEMEVAL_2014_TASK7_ANALYSIS=-6;
	public static final int MRN_CRCP_ANALYSIS=-5;
	public static final int OSTEOPENIA_DETECTION_ANALYSIS_ID=-4;
	public static final int MELANOMA_EXTRACTION_ANALYSIS_ID=-3;
	public static final int MELANOMA_DETECTION_ANALYSIS_ID=-2;
	public static final int PATH_REPORT_ANALYSIS_ID=-1;

	
	//VIEWS
	public static final String JDBC_VIEW = "jdbc_view";
	public static final String DOCUMENT_IDENTIFIER_VIEW = "document_identifier_view";

	public static final String ASCI_VIEW = "ascii_view";
	//public static final String UTF_VIEW = "utf_view";
	public static final String UTF_VIEW = CAS.NAME_DEFAULT_SOFA;

}
