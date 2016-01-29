package edu.uab.ccts.nlp.medics.util;

/**
 * Medics Related configuration information, should externalize to property file
 * @author josborne
 *
 */
public class MedicsConstants {
	//public static final boolean _IS_DEV = true;

	public final static double ANALYSIS_DOC_PASS_RATE_REQUIRED=90.0; //Percent of documents required
	
	public static final int ANALYSIS_FAIL_STATUS=1;
	public static final int ANALYSIS_PARTIAL_STATUS=2;
	public static final int ANALYSIS_SUCCESS_STATUS=3;
	public static final int ANALYSIS_EMPTY_STATUS=4;
	public static final int ANALYSIS_MOSTLY_WORKED_STATUS=5;
	public static final int ANALYSIS_INITATED_STATUS=6;

	public static final int DOCUMENT_PROCESSING_INITIATED=1;
	public static final int DOCUMENT_CONVERTED=2;
	public static final int DOCUMENT_HITS_PROCESSING_COMPLETE=3;
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
}
