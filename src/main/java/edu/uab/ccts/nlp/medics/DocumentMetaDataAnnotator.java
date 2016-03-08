package edu.uab.ccts.nlp.medics;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates based on parameters some metadata about the document being analyzed
 * which can be saved in the Medics schema
 * Generic code to interrogate the document for metadata should go here
 * @author josborne
 *
 */
public class DocumentMetaDataAnnotator extends JCasAnnotator_ImplBase {
	private static final Logger LOG  = LoggerFactory.getLogger(DocumentMetaDataAnnotator.class);

	public static final String PARAM_PATIENT_IDENTIFIER = "patientIdentifier";
	public static final String PARAM_VERSION = "version";
	public static final String PARAM_SOURCE = "source";
	public static final String PARAM_SOURCE_IDENTIFIER = "sourceIdentifier";
	public static final String PARAM_TYPE = "type";
	public static final String PARAM_SUBTYPE = "subtype";
	public static final String PARAM_DOCUMENT_CREATION_DATE = "documentCreationDate";
	public static final String PARAM_DOCSET_ID = "docSetID";

	//UIMA-FIT Parameter Assignment
	@ConfigurationParameter(
			name = PARAM_PATIENT_IDENTIFIER,
			mandatory = false,
			description = "Patient identifier or MRN to analyze")
	String patientIdentifier = null;

	@ConfigurationParameter(
			name = PARAM_VERSION,
			mandatory = false,
			description = "Document version")
	int version = 0;


	@ConfigurationParameter(
			name = PARAM_SOURCE,
			mandatory = false,
			description = "Source of document, ex) icda, cflo, semeval")
	String source = null;


	@ConfigurationParameter(
			name = PARAM_SOURCE_IDENTIFIER,
			mandatory = false,
			description = "Document version")
	String sourceIdentifier = null;

	@ConfigurationParameter(
			name = PARAM_TYPE,
			mandatory = false,
			description = "Document type")
	String type = null;

	@ConfigurationParameter(
			name = PARAM_SUBTYPE,
			mandatory = false,
			description = "Document subtype")
	String subtype = null;

	@ConfigurationParameter(
			name = PARAM_DOCUMENT_CREATION_DATE,
			mandatory = false,
			description = "Document subtype")
	String documentCreationDate = null;

	@ConfigurationParameter(
			name = PARAM_DOCSET_ID,
			mandatory = false,
			description = "Document version")
	int docsetId= 0;


	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		NLP_Clobs pprop =  new NLP_Clobs(jcas);
		if(patientIdentifier!=null) pprop.setMRN(Integer.parseInt(patientIdentifier));
		if(documentCreationDate!=null) pprop.setDateOfService(documentCreationDate);
		pprop.setDocumentTypeAbbreviation(type);
		pprop.setDocumentSubType(subtype);
		pprop.setDocumentVersion(version);
		pprop.setSource(source);
		if(sourceIdentifier!=null) { pprop.setSourceID(sourceIdentifier); return; } 
		parseDocumentMetaDataFromUrl(jcas,pprop);
		return;
	}


	//FIXME - Should inherit from this annotator and put in UAB NLP Clients
	protected void parseDocumentMetaDataFromUrl(JCas jcas, NLP_Clobs metadoc) {
		String uri_string =metadoc.getURI().toString();
		LOG.info("Got uri string of:"+uri_string);
		if(source.equals("Cerner PowerInsight") && type.equalsIgnoreCase("eeg")){
			String[] fields = uri_string.split("_");
			LOG.info("Got "+fields.length+" fields");
		}
		metadoc.setSourceID(metadoc.getURI().toString());
	}


	public static AnalysisEngineDescription createAnnotatorDescription(
			String type, int ver, String src ) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_TYPE,
				type,
				PARAM_VERSION,
				ver,
				PARAM_SOURCE,
				src
				);
	}


}
