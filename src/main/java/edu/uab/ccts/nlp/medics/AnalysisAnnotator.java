package edu.uab.ccts.nlp.medics;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;
import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;

/**
 * Generates based on parameters some metadata about the analysis being performed
 * and saves it in the medics database
 * @author josborne
 *
 */
public class AnalysisAnnotator extends JCasAnnotator_ImplBase {

	private Logger log;

	public static final String PARAM_ANALYSIS_TYPE = "analysisType";
	public static final String PARAM_ANALYSIS_DESCRIPTION = "analysisDescription";
	public static final String PARAM_ANALYSIS_SOFTWARE = "analysisSoftware";
	public static final String PARAM_ANALYSIS_SOFTWARE_VERSION = "analysisSoftwareVersion";
	public static final String PARAM_ANALYSIS_ID = "analysisID";
	public static final String PARAM_DOCUMENT_SOURCE = "documentSource";
	public static final String PARAM_DOCSET_ID = "docSetId";
	public static final String PARAM_MEDICSURL = "medicsConnectionString";
	
	public static final String APPLICATION_PROPERTIES_PATH="application.properties";

	//UIMA-FIT Parameter Assignment
	@ConfigurationParameter(
			name = PARAM_ANALYSIS_SOFTWARE,
			mandatory = false,
			description = "Analysis Software")
	String analysisSoftware;

	@ConfigurationParameter(
			name = PARAM_ANALYSIS_SOFTWARE_VERSION,
			mandatory = false,
			description = "Analysis Software Version")
	String analysisSoftwareVersion;

	@ConfigurationParameter(
			name = PARAM_ANALYSIS_ID,
			mandatory = false,
			description = "input analysis id, negative results used to indicate certain analysis, now use analysis type "
			//,defaultValue = "0" //Does not work?
			)
	int analysisID;
	
	@ConfigurationParameter(
			name = PARAM_DOCSET_ID,
			mandatory = false,
			description = "input docset id, inserted by client"
			)
	int docSetId;


	static final String ANALYSIS_TYPE_DESCRIPTION
	= "Type of analysis being run, see Medics Type System (MedicsConstants) for options";
	@ConfigurationParameter(
			name = PARAM_ANALYSIS_TYPE,
			mandatory = false,
			description = ANALYSIS_TYPE_DESCRIPTION)
	private Integer analysisType;

	@ConfigurationParameter(
			name = PARAM_MEDICSURL,
			mandatory = false,
			description = "Medics database to write analysis progress"
			)
	private String medicsConnectionString = null;

	@ConfigurationParameter(
			name = PARAM_ANALYSIS_DESCRIPTION,
			mandatory = false,
			description = "Description of the analysis done"
			)
	private String analysisDescription = null;

	@ConfigurationParameter(
			name = PARAM_DOCUMENT_SOURCE,
			mandatory = false,
			description = "Description of the documents used, for docset"
			)
	private String documentDescription = null;


	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		log = aContext.getLogger();
		log.log(Level.INFO,"Running "+analysisDescription+" saving to "+medicsConnectionString);
	}


	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		try {
		int ainfo[] = LegacyMedicsTools.insertClientAnalysis
		(medicsConnectionString,
				analysisType.intValue(),null //MRN
				,null,null //Comparison Dates 1 and 2
				,2 //Day back offset
				,null,null //cflo, icda record types
				,analysisDescription
				,null //Docset Description
				,analysisSoftware,analysisSoftwareVersion
				,1 //thread count
				,1 //pipeline type
				);
		docSetId = ainfo[0];
		analysisID = ainfo[1];
		NLP_Analysis nlpan = new NLP_Analysis(jcas);
		nlpan.setMedicsURL(medicsConnectionString);
		nlpan.setAnalysisDescription(analysisDescription);
		nlpan.setAnalysisType(analysisType);
		nlpan.setAnalysisSoftware(analysisSoftware);
		nlpan.setAnalysisSoftwareVersion(analysisSoftwareVersion);
		nlpan.setDocumentSource(documentDescription);
		nlpan.setAnalysisDataSet(docSetId);
		nlpan.setAnalysisID(analysisID);
		nlpan.addToIndexes(jcas);
		log.log(Level.FINER,"Wrote analysis "+analysisID+" to "
				+jcas.getViewName()+" of type "+analysisType);
		} catch (Exception e) { throw new AnalysisEngineProcessException(e); }
	}


	
	public static AnalysisEngineDescription createAnnotatorDescription(int id, int type, 
			String url, String software, String softwareversion,String aDescription, 
			String docsetDescription, int docsetid ) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_ID,
				id,
				PARAM_ANALYSIS_TYPE,
				type,
				PARAM_MEDICSURL,
				url,
				PARAM_ANALYSIS_SOFTWARE,
				software,
				PARAM_ANALYSIS_SOFTWARE_VERSION,
				softwareversion,
				PARAM_ANALYSIS_DESCRIPTION,
				aDescription,
				PARAM_DOCUMENT_SOURCE,
				docsetDescription,
				PARAM_DOCSET_ID,
				docsetid
				);
	}

	public static AnalysisEngineDescription createAnnotatorDescription(int type) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_TYPE,
				type
				);
	}


}
