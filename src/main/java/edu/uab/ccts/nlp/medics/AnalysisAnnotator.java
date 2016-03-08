package edu.uab.ccts.nlp.medics;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;

/**
 * Generates based on parameters some metadata about the analysis being performed
 * which can be saved in the Medics schema
 * @author josborne
 *
 */
public class AnalysisAnnotator extends JCasAnnotator_ImplBase {
	public static final String PARAM_ANALYSIS_TYPE = "analysisType";
	public static final String PARAM_ANALYSIS_ID = "analysisID";
	public static final String PARAM_MEDICSURL = "medicsConnectionString";
	
	//UIMA-FIT Parameter Assignment
	@ConfigurationParameter(
			name = PARAM_ANALYSIS_ID,
			mandatory = false,
			description = "input analysis id, negative results used to indicate certain analysis, now use analysis type ")
	int analysisID = 0;

	static final String ANALYSIS_TYPE_DESCRIPTION
	= "Type of analysis being run, see Medics Type System (MedicsConstants) for options";
	@ConfigurationParameter(
			name = PARAM_ANALYSIS_TYPE,
			mandatory = true,
			description = ANALYSIS_TYPE_DESCRIPTION)
	int analysisType = 0;

	@ConfigurationParameter(
			name = PARAM_MEDICSURL,
			mandatory = false,
			description = "Medics database to write analysis progress"
			)
	String medicsConnectionString = null;
	
	
	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		NLP_Analysis nlpan = new NLP_Analysis(jcas);
		nlpan.setAnalysisID(analysisID);
		nlpan.setMedicsURL(medicsConnectionString);
		nlpan.setAnalysisType(analysisType);
		nlpan.addToIndexes(jcas);
	}


	public static AnalysisEngineDescription createAnnotatorDescription(int id, int type) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_ID,
				id,
				PARAM_ANALYSIS_TYPE,
				type
				);
	}
	
	public static AnalysisEngineDescription createAnnotatorDescription(int type) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_TYPE,
				type
				);
	}

	
}
