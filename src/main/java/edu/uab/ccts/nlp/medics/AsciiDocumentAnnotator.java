package edu.uab.ccts.nlp.medics;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.component.ViewCreatorAnnotator;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.Level;

import edu.uab.ccts.nlp.medics.util.MedicsConstants;
import edu.uab.ccts.nlp.medics.util.MedicsTools;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

/**
 * Requires InitialView to be set with UTF16 (java String) text
 * 
 * Creates an ASCII (legacy) Document View for ASCII only software
 * like MetaMap
 * @author josborne
 *
 */
public class AsciiDocumentAnnotator extends JCasAnnotator_ImplBase {

	public static final String PARAM_UTF_VIEW ="utfView";

	@ConfigurationParameter(
			name = PARAM_UTF_VIEW,
			mandatory = true,
			description = "View with UTF document text")
					
	protected String utfView = null;

	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		JCas asciiView, sourceView;
		MedicsTools mt = new MedicsTools();
		try {
			//asciiView = ViewCreatorAnnotator.createViewSafely(jcas, MedicsConstants.UTF_VIEW);
			asciiView = jcas.createView(MedicsConstants.ASCI_VIEW);
			sourceView = jcas.getView(utfView);
			String asciiDoc = mt.convertUTFtoASCII(sourceView.getDocumentText());
			asciiView.setDocumentText(asciiDoc);
		} catch (Exception ejcas) {
			this.getContext().getLogger().log(Level.WARNING,
					"Could not convert document to ascii");
			ejcas.printStackTrace();
			throw new AnalysisEngineProcessException(ejcas);
		}
		
		CasCopier cc = new CasCopier(sourceView.getCas(),asciiView.getCas());
		Feature sofaFeature = jcas.getTypeSystem().getFeatureByFullName(CAS.FEATURE_FULL_NAME_SOFA);
		for(NLP_Clobs metadoc : JCasUtil.select(sourceView, NLP_Clobs.class)) {
			NLP_Clobs nc = (NLP_Clobs) cc.copyFs(metadoc);
			nc.setFeatureValue(sofaFeature, asciiView.getSofa());
			nc.setMd5Sum(mt.calculateMd5(sourceView.getDocumentText()));
			nc.addToIndexes();
		}
		
		//Create NLP_Clobs for ascii data, copy stuff
	}



	public static AnalysisEngineDescription createAnnotatorDescription(
			String viewname) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AsciiDocumentAnnotator.class,
				PARAM_UTF_VIEW,
				viewname
				);
	}


}
