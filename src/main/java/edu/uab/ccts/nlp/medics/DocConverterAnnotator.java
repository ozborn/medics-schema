package edu.uab.ccts.nlp.medics;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

import edu.uab.ccts.nlp.medics.util.MedicsTools;


/**
 * Converts pdf and word documents to text stored in a separate view
 * Should move to clearclinical once I build off a release instead of master
 * @author AD\josborne
 *
 */
public class DocConverterAnnotator extends JCasAnnotator_ImplBase {
	public final String PDF_VIEW="PDF";
	public final String DOC_VIEW="DOC";
	public final String CONVERTED_PDF_VIEW="PDF_VIEW";
	public final String CONVERTED_DOC_VIEW="DOC_VIEW";

	UimaContext ucon = null;

	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		ucon = aContext;
	}


	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		String charset="UTF-8";
		String mimetype = jcas.getSofaMimeType();
		int index = mimetype.toLowerCase().indexOf("charset");
		if(index!=-1) charset=mimetype.substring(index+6);
		convertDocumentDataString(jcas, charset, mimetype, PDF_VIEW,CONVERTED_PDF_VIEW,"pdf");
		convertDocumentDataString(jcas, charset, mimetype, DOC_VIEW,CONVERTED_DOC_VIEW,"doc");
	}


	private void convertDocumentDataString(JCas jcas, String charset, String mimetype
			,String source, String destination, String expected_type
			) {
		try {
			JCas target_jcas = jcas.getView(source);
			if(target_jcas.getSofaMimeType().toLowerCase().indexOf(expected_type)==-1) {
				ucon.getLogger().log(Level.WARNING,expected_type+" mimetype not set...");
			}
			String datastring = target_jcas.getSofaDataString();
			MedicsTools mt = new MedicsTools();
			try(InputStream is = new ByteArrayInputStream(datastring.getBytes(charset))){
				String converted_pdf = mt.tikaFetchDoc(null, is, new Metadata());
				JCas converted_view = target_jcas.createView(destination);
				converted_view.setSofaDataString(converted_pdf, mimetype);
			} catch (CASException ce) {
				ucon.getLogger().log(Level.WARNING,"Failed to created PDF converted view");
			} catch (Exception te) {
				ucon.getLogger().log(Level.WARNING,"Failed to convert PDF using charset "+charset);
			}
		} catch (CASException e) {
			ucon.getLogger().log(Level.WARNING,"No PDF view, PDF not converted...");
			e.printStackTrace();
		}
	}

}
