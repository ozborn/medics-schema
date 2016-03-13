package edu.uab.ccts.nlp.medics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
	public static final String PARAM_ANALYSIS_ID = "analysisID";
	public static final String PARAM_MEDICSURL = "medicsConnectionString";

	//UIMA-FIT Parameter Assignment
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


	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		log = aContext.getLogger();
		log.log(Level.INFO,"Medics URL initialized to:"+medicsConnectionString);
		log.log(Level.INFO,"Analysis ID initialized to:"+analysisID);
	}


	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		NLP_Analysis nlpan = new NLP_Analysis(jcas);
		if(analysisID==MedicsConstants.DEFAULT_ANALYSIS_SENTINEL_VALUE){
			insertAnalysis(nlpan);
		}
		nlpan.setAnalysisID(analysisID);
		nlpan.setMedicsURL(medicsConnectionString);
		nlpan.setAnalysisType(analysisType);
		nlpan.addToIndexes(jcas);
		log.log(Level.INFO,"Wrote analysis "+analysisID+" to "
		+jcas.getViewName()+" of type "+analysisType);
	}


	private void insertAnalysis(NLP_Analysis nlpanal) {
		String insertTableSQL = "INSERT INTO NLP_ANALYSIS"
				+ "(  ANALYSIS_TYPE, ANALYSIS_SOFTWARE, "+
				"ANALYSIS_START_DATE , "+
				" MACHINE "+
				") VALUES (?,?,SYSDATE,?)  ";
		try (
				Connection conn =  DriverManager.getConnection(medicsConnectionString);
				PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, 
						new String[]{"ANALYSIS_ID"})
				)
		{
			preparedStatement.setString(1, String.valueOf(nlpanal.getAnalysisType()));
			preparedStatement.setString(2, nlpanal.getAnalysisSoftware() );
			try {
				preparedStatement.setString(3, InetAddress.getLocalHost().getHostName() );
			} catch (UnknownHostException e) {
				preparedStatement.setString(3, "Unknown host");
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


	public static AnalysisEngineDescription createAnnotatorDescription(int id, int type, String url) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_ID,
				id,
				PARAM_ANALYSIS_TYPE,
				type,
				PARAM_MEDICSURL,
				url
				);
	}

	public static AnalysisEngineDescription createAnnotatorDescription(int type) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(AnalysisAnnotator.class,
				PARAM_ANALYSIS_TYPE,
				type
				);
	}


}
