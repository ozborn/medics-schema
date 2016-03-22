package edu.uab.ccts.nlp.medics;

import java.util.Collection;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import edu.uab.ccts.nlp.medics.util.MedicsConstants;
import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;
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
	public static final String PARAM_IMPORT_ANALYSIS_ID = "importAnalysisId";

	//UIMA-FIT Parameter Assignment
	@ConfigurationParameter(
			name = PARAM_PATIENT_IDENTIFIER,
			mandatory = false,
			description = "Patient identifier or MRN to analyze")
	protected String patientIdentifier = null;

	@ConfigurationParameter(
			name = PARAM_VERSION,
			mandatory = false,
			description = "Document version")
	protected int version;


	@ConfigurationParameter(
			name = PARAM_SOURCE,
			mandatory = false,
			description = "Source of document, ex) icda, cflo, semeval")
	protected String source = null;

	@ConfigurationParameter(
			name = PARAM_SOURCE_IDENTIFIER,
			mandatory = false,
			description = "Source Identifier, perhaps a URL or database primary key")
	protected String sourceIdentifier = null;

	@ConfigurationParameter(
			name = PARAM_TYPE,
			mandatory = false,
			description = "Document type")
	protected String type = null;

	@ConfigurationParameter(
			name = PARAM_SUBTYPE,
			mandatory = false,
			description = "Document subtype")
	protected String subtype = null;

	@ConfigurationParameter(
			name = PARAM_DOCUMENT_CREATION_DATE,
			mandatory = false,
			description = "Document subtype")
	protected String documentCreationDate = null;

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
		NLP_Clobs pprop =  new NLP_Clobs(jcas);
		guessSourceId(jcas,pprop);
		pprop.setMRN(MedicsConstants.DEFAULT_DOCUMENT_MRN_SENTINEL_VALUE);
		try {
			JCas uriview = jcas.getView("UriView");
			if(uriview!=null && uriview.getSofaDataURI()!=null){
				pprop.setURL(uriview.getSofaDataURI());
			}
		} catch (Exception e) {LOG.debug("No UriView found");}
		if(version!=MedicsConstants.DEFAULT_DOCUMENT_VERSION_SENTINEL_VALUE){
			pprop.setDocumentVersion(version);
		}
		if(patientIdentifier!=null) pprop.setMRN(Integer.parseInt(patientIdentifier));
		if(documentCreationDate!=null) pprop.setDateOfService(documentCreationDate);
		if(type!=null) pprop.setDocumentTypeAbbreviation(type);
		if(subtype!=null) pprop.setDocumentSubType(subtype);
		if(source!=null) pprop.setSource(source);
		Collection<NLP_Analysis> anals = JCasUtil.select(jcas, NLP_Analysis.class);
		if(anals!=null && anals.size()==1) {
			importAnalysisId = anals.iterator().next().getAnalysisID();
			pprop.setImportAnalysis(importAnalysisId);
		} else if(importAnalysisId>0) {
			pprop.setImportAnalysis(importAnalysisId);
		} else LOG.warn("Import analysis ID unknown");
		pprop.addToIndexes();
		int snippetend = jcas.getDocumentText().length();
		snippetend = (100 > snippetend) ? 100 : snippetend;
		LOG.info("Set MRN/Source id:"+pprop.getMRN()+"/"+pprop.getSourceID()+" URL:"+pprop.getURL()+
		"in view "+jcas.getViewName()+" and text snippet:"+jcas.getDocumentText().substring(0, snippetend));
		return;
	}



	/**
	 * This can be overridden for populating document meta information from variable
	 * format URI's
	 * @param jcas
	 * @return the source id
	 */
	public void guessSourceId(JCas jcas, NLP_Clobs doc) {
		if(sourceIdentifier==null) {
			LOG.debug("No Source identifier provided");
			try {
				if(jcas.getSofaDataURI()!=null && !jcas.getSofaDataURI().isEmpty()) {
					doc.setSourceID(jcas.getSofaDataURI().toString());
				} else {
					JCas uriview = jcas.getView("UriView");
					if(uriview!=null){
						sourceIdentifier = uriview.getSofaDataURI().toString();
						doc.setSourceID(sourceIdentifier);
						LOG.info(sourceIdentifier+" source id set from uri");
					}
				}
			} catch (CASException e) {
				LOG.warn("Could not determine source identifier");
			}
		} else {
			LOG.info("Source identifier "+sourceIdentifier+" was provided");
			doc.setSourceID(sourceIdentifier); 
		}
	}



	public static AnalysisEngineDescription createAnnotatorDescription(
			String type, int ver, String src, int importId ) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(DocumentMetaDataAnnotator.class,
				PARAM_TYPE,
				type,
				PARAM_VERSION,
				ver,
				PARAM_SOURCE,
				src,
				PARAM_IMPORT_ANALYSIS_ID,
				importId
				);
	}


}
