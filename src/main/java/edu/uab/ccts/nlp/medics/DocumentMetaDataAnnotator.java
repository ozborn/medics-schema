package edu.uab.ccts.nlp.medics;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;

import edu.uab.ccts.nlp.medics.util.MedicsConstants;
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
		setDefaultMetaData(jcas,pprop);
		if(sourceIdentifier!=null) pprop.setSourceID(sourceIdentifier); 
		if(patientIdentifier!=null) pprop.setMRN(Integer.parseInt(patientIdentifier));
		if(documentCreationDate!=null) pprop.setDateOfService(documentCreationDate);
		if(type!=null) pprop.setDocumentTypeAbbreviation(type);
		if(subtype!=null) pprop.setDocumentSubType(subtype);
		if(version!=0) pprop.setDocumentVersion(version);
		if(source!=null) pprop.setSource(source);
		pprop.addToIndexes();
		LOG.info("Finished setting document meta-data properties");
		return;
	}


	/**
	 * This can be overridden for populating document meta information from variable
	 * format URI's
	 * @param jcas
	 * @return the source id
	 */
	protected void setDefaultMetaData(JCas jcas, NLP_Clobs doc) {
		doc.setMRN(MedicsConstants.MRN_ANONYMOUS_SENTINEL_VALUE);
		doc.setDocumentVersion(MedicsConstants.MEDICS_DOCUMENT_DEFAULT_VERSION);

		String sourceid = null;
		FSIndex<Annotation> srcdocIndex = jcas.getAnnotationIndex(SourceDocumentInformation.type);
		if(srcdocIndex!=null) {
			Iterator<Annotation> srcdocIter = srcdocIndex.iterator();
			sourceid = ((SourceDocumentInformation) srcdocIter.next()).getUri().trim();
			if(sourceid.indexOf(File.separatorChar)!=-1){
				sourceid=sourceid.substring(sourceid.lastIndexOf(File.separatorChar));
			}
			doc.setSourceID(sourceid);
		} else {
			doc.setSourceID(jcas.getSofaDataURI().toString());
		}
	}

	
	public java.util.Date convertStringToJavaDate(String input_date, String format) 
			throws ParseException {
		java.util.Date date = null;
		try {
			DateFormat df = new SimpleDateFormat(format); 
			date = df.parse(input_date);
		} catch (ParseException pe) {
			throw pe;
		}
		return date;
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
