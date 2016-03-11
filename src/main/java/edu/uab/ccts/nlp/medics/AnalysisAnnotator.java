package edu.uab.ccts.nlp.medics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uab.ccts.nlp.uima.ts.NLP_Analysis;

/**
 * Generates based on parameters some metadata about the analysis being performed
 * which can be saved in the Medics schema
 * @author josborne
 *
 */
public class AnalysisAnnotator extends JCasAnnotator_ImplBase {
	private static final Logger LOG  = LoggerFactory.getLogger(AnalysisAnnotator.class);

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
			mandatory = false,
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
		LOG.info("Medics URL is:"+medicsConnectionString);
		NLP_Analysis nlpan = new NLP_Analysis(jcas);
		nlpan.setAnalysisID(analysisID);
		nlpan.setMedicsURL(medicsConnectionString);
		nlpan.setAnalysisType(analysisType);
		nlpan.addToIndexes(jcas);
		insertAnalysis(nlpan);
		LOG.info("Wrote analysis to "+jcas.getViewName()+" of type "+analysisType);
	}


	Integer insertAnalysis(NLP_Analysis nlpanal) {
		Integer generatedAnalysisID = null;
		String insertTableSQL = "INSERT INTO NLP_ANALYSIS"
				+ "(  ANALYSIS_TYPE, ANALYSIS_SOFTWARE, "+
				"ANALYSIS_START_DATE , "+
				" MACHINE "+
				") VALUES (?,?,SYSDATE,?,?,?,?,?)  ";
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
				preparedStatement.setString(7, "InetAddress.getLocalHost().getHostName " + e.getMessage());
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
		return generatedAnalysisID;
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
