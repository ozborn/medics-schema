package edu.uab.ccts.nlp.medics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Iterator;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;
import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;


/**
 * Based on the analysis type, decide if the analysis was successful on
 * this document
 * New analysis types should override the judge method
 * @author josborne
 *
 */
public class DocumentAnalysisJudgeAnnotator extends JCasAnnotator_ImplBase {

	@ConfigurationParameter(
			name = MedicsCLOBsConsumer.PARAM_MEDICS_URL,
			mandatory = false,
			description = "Medics database to write analysis progress"
			)
	private String medicsConnectionString = null;
	public UimaContext uContext = null;
	int analysisId = 0;
	int docsetId = 0;

	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		try {
			uContext = aContext;
			aContext.getLogger().log(Level.INFO,"Medics Doc/Clob Writing URL is: "+medicsConnectionString+"\n");
		} catch (Exception e) { throw new ResourceInitializationException(); }
	}

	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		NLP_Analysis anal = null;
		NLP_Clobs thedoc = null;

		try {
			FSIndex<Annotation> analIndex = jcas.getAnnotationIndex(NLP_Analysis.type);
			Iterator<Annotation> analIter = analIndex.iterator();
			anal = (NLP_Analysis) analIter.next();
			analysisId=anal.getAnalysisID();
			docsetId=anal.getAnalysisDataSet();
			Collection<NLP_Clobs> mdocs = JCasUtil.select(jcas, NLP_Clobs.class);
			if(mdocs==null || mdocs.size()!=1) {
				getContext().getLogger().log(Level.SEVERE,"Did not find 1 metadocument!");
			}
			thedoc = (NLP_Clobs) mdocs.iterator().next();
			
			Integer cur_state = LegacyMedicsTools.getDocumentWorkStatus(
				medicsConnectionString, anal.getAnalysisID(), 
				thedoc.getReportID());
			Integer status = judgeDocumentAnalysis(cur_state);
			try (Connection conn = DriverManager.getConnection(medicsConnectionString)){
				LegacyMedicsTools.updateDocumentHistory(conn, 
						thedoc.getReportID(), anal.getAnalysisID(), 
						status);
			} catch (Exception e) { throw new AnalysisEngineProcessException(); }


		} catch (Exception e){
			uContext.getLogger().log(Level.SEVERE,
					"Could not determine the analysis_id , docsetId, or"+
							" document id. Analysis and docsetID should be set by AnalysisAnnotator (UIMA-FIT) or"
							+ "OracleTextCollectionReader (Legacy)");
			throw new AnalysisEngineProcessException(e);
		} 
	}


	/**
	 * Given current state of document analysis, decide
	 * whether analysis is complete
	 * @param doc
	 * @param analysis
	 * @return
	 */
	protected int judgeDocumentAnalysis(int cur_state)
			throws AnalysisEngineProcessException {
		return cur_state;
	}

}
