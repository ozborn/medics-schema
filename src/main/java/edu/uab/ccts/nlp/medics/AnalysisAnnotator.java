package edu.uab.ccts.nlp.medics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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

import edu.uab.ccts.nlp.medics.util.MedicsConstants;

/*
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 */

import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;

/**
 * Generates based on parameters some metadata about the analysis being performed
 * which can be saved in the Medics schema
 * @author josborne
 *
 */
public class AnalysisAnnotator extends JCasAnnotator_ImplBase {

	//Causing double initialization of AnalysisAnnotator?
	//private final Logger LOG  = LoggerFactory.getLogger(AnalysisAnnotator.class);
	private Logger log;

	public static final String PARAM_ANALYSIS_TYPE = "analysisType";
	public static final String PARAM_ANALYSIS_DESCRIPTION = "analysisDescription";
	public static final String PARAM_ANALYSIS_SOFTWARE = "analysisSoftware";
	public static final String PARAM_ANALYSIS_ID = "analysisID";
	public static final String PARAM_DOCUMENT_SOURCE = "documentSource";
	public static final String PARAM_MEDICSURL = "medicsConnectionString";

	//UIMA-FIT Parameter Assignment
	@ConfigurationParameter(
			name = PARAM_ANALYSIS_SOFTWARE,
			mandatory = false,
			description = "Analysis Software")
	String analysisSoftware;


	@ConfigurationParameter(
			name = PARAM_ANALYSIS_ID,
			mandatory = false,
			description = "input analysis id, negative results used to indicate certain analysis, now use analysis type "
			//,defaultValue = "0" //Does not work?
			)
	int analysisID;

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
		log.log(Level.INFO,"Medics URL initialized to:"+medicsConnectionString);
		log.log(Level.INFO,"Analysis ID initialized to:"+analysisID);
	}


	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		try {
		NLP_Analysis nlpan = new NLP_Analysis(jcas);
		if(analysisID==MedicsConstants.DEFAULT_ANALYSIS_SENTINEL_VALUE){
			insertAnalysis(nlpan);
		}
		int docSetId = insertDocSet(documentDescription);
		nlpan.setAnalysisDataSet(docSetId);
		nlpan.setDocumentSource(documentDescription);
		nlpan.setAnalysisID(analysisID);
		nlpan.setMedicsURL(medicsConnectionString);
		nlpan.setAnalysisType(analysisType);
		nlpan.setAnalysisSoftware(analysisSoftware);
		if(analysisDescription!=null) nlpan.setAnalysisDescription(analysisDescription);
		nlpan.addToIndexes(jcas);
		log.log(Level.INFO,"Wrote analysis "+analysisID+" to "
				+jcas.getViewName()+" of type "+analysisType);
		} catch (Exception e) { throw new AnalysisEngineProcessException(e); }
	}


	private void insertAnalysis(NLP_Analysis nlpanal) {
		String insertTableSQL = "INSERT INTO NLP_ANALYSIS"
				+ "(  ANALYSIS_TYPE, ANALYSIS_SOFTWARE, "+
				" ANALYSIS_DESCRIPTION, MACHINE, "+
				"ANALYSIS_START_DATE "+
				") VALUES (?,?,?,?,SYSDATE)  ";
		try (
				Connection conn =  DriverManager.getConnection(medicsConnectionString);
				PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, 
						new String[]{"ANALYSIS_ID"})
				)
		{
			preparedStatement.setString(1, String.valueOf(nlpanal.getAnalysisType()));
			preparedStatement.setString(2, nlpanal.getAnalysisSoftware() );
			preparedStatement.setString(3, nlpanal.getAnalysisDescription() );
			try {
				preparedStatement.setString(4, InetAddress.getLocalHost().getHostName() );
			} catch (UnknownHostException e) {
				preparedStatement.setString(4, "Unknown host");
				log.log(Level.WARNING,"InetAddress.getLocalHost().getHostName " + e.getMessage());
			}
			preparedStatement.executeUpdate();
			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()){
				if (generatedKeys.next()) {
					analysisID = generatedKeys.getInt(1);
				} else {
					throw new Exception("Creating NLP_ANALYSIS, no generated key obtained.");
				}
			} catch (Exception e) {
				throw e;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	private String getAnalysisDescription(int type) {
		String pipelineString = null;
		return pipelineString;

		/*
		if(analysisType==ConfigurationSingleton.PATH_REPORT_ANALYSIS_ID) {
			docsetdesc = "ICDA PTH Notes signed from ";
			analysisdesc = "Cancer Pipeline on ICDA PTH Notes signed on ";
			pipelinestring = "CancerDetectionPipeline";
			if(comparisonDate==null || comparisonDate.isEmpty()) {
				docsetdesc+=LegacyMedicsTools.getDaysAgoDateString(daybackoffset);
				analysisdesc+=LegacyMedicsTools.getDaysAgoDateString(daybackoffset)+
						" iniatated by CRCP PTH cron job";
			} else {
				if(comparisonDate2==null || comparisonDate2.isEmpty()){
					docsetdesc+=comparisonDate.toString();
					analysisdesc+=comparisonDate.toString();
				} else {
					docsetdesc+=comparisonDate.toString()+ " (midnight) to ";
					docsetdesc+=comparisonDate2.toString()+ " (midnight)";
					analysisdesc+=comparisonDate.toString()+ " (midnight) to ";
					analysisdesc+=comparisonDate2.toString()+ " (midnight)";
				}
			}
		} else if(analysisType == ConfigurationSingleton.SEMEVAL_2014_TASK7_ANALYSIS){
			pipelinestring = "SemEvalDetectionPipeline";
			docsetdesc = " SemEval Task 7 2014 Docs";
			analysisdesc = pipelinestring+" on "+datenow;
		} else if(analysisType == ConfigurationSingleton.SHARECLEF_2014_POST_COORDINATION_ANALYSIS){
			pipelinestring = "SemEvalPostCoordinationCUIlessPipeline";
			docsetdesc = "SemEval Task7 CUIless Concepts";
		} else if(analysisType == ConfigurationSingleton.MELANOMA_DETECTION_ANALYSIS_ID){
			pipelinestring = "MultipleMyelomaDetectionPipeline";
		} else if (analysisType == ConfigurationSingleton.MELANOMA_EXTRACTION_ANALYSIS_ID){
			pipelinestring = "MultipleMyelomaExtractionPipeline";
		} else if (analysisType == ConfigurationSingleton.OSTEOPENIA_DETECTION_ANALYSIS_ID){
			pipelinestring = "OsteopeniaDetectionPipeline";
		} else if (analysisType == ConfigurationSingleton.BONE_LESION_DETECTION_ANALYSIS_ID){
			pipelinestring = "BoneLesionLucencyDetectionPipeline";
		} else if (analysisType == ConfigurationSingleton.UPDATE_HASHCODE_ANALYSIS){
			pipelinestring = "UpdateHashCodePipeline";
		} else if (analysisType == ConfigurationSingleton.WORD2VEC_MODEL_CREATION_ANALYSIS){
			pipelinestring = "Word2VecModelCreationPipeline";
		} else {
			if(!(mrn==null || mrn.isEmpty())) {
				docsetdesc += " (MRN "+mrn+")";
			}
			if(analysis_description!=null) analysisdesc = analysis_description;
			if(docset_description!=null) docsetdesc = docset_description;
		}

		if(analysisType!=ConfigurationSingleton.PATH_REPORT_ANALYSIS_ID){
			docsetdesc += " up to "+datenow;
			analysisdesc = pipelinestring+" on "+datenow;
		}
		 */

	}


	public int insertDocSet(String docSetDescription
			) throws SQLException {
		int docSetID = -1;

		String insertTableSQL = "INSERT INTO NLP_DOCSET "
				+ "( DESCRIPTION ) VALUES (?)  ";
		try (
				Connection conn = DriverManager.getConnection(medicsConnectionString);
				PreparedStatement preparedStatement = 
						conn.prepareStatement(insertTableSQL, new String[]{"DOCSET_ID"});
				){
			preparedStatement.setString(1, docSetDescription );
			preparedStatement.executeUpdate();
			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys() ) {
				if (generatedKeys.next()) {
					docSetID = generatedKeys.getInt(1);
					generatedKeys.close();
				} else {
					throw new SQLException("Creating NLP_DOCSET, no generated key obtained.");
				}
			}
		} catch (Exception e) {
			throw(e);
		}
		return docSetID;
	}


	public static AnalysisEngineDescription createAnnotatorDescription(int id, int type, 
			String url, String software, String docsetDescription) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_ID,
				id,
				PARAM_ANALYSIS_TYPE,
				type,
				PARAM_MEDICSURL,
				url,
				PARAM_ANALYSIS_SOFTWARE,
				software,
				PARAM_DOCUMENT_SOURCE,
				docsetDescription
				);
	}

	public static AnalysisEngineDescription createAnnotatorDescription(int type) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_TYPE,
				type
				);
	}


}
