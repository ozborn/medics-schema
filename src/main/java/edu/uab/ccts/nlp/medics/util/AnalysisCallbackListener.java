package edu.uab.ccts.nlp.medics.util;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.collection.StatusCallbackListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalysisCallbackListener implements StatusCallbackListener {

	private static final Logger LOG  = LoggerFactory.getLogger(AnalysisCallbackListener.class);
	String medicsConnectionString;
	int analysisId,expectedDocumentCount;
	long stime, pauseTime, restartTime;

	public AnalysisCallbackListener(String medicsUrl, int analysisID,long startTime,
			int expectedDocCount){
		medicsConnectionString = medicsUrl;
		analysisId=analysisID;
		stime = startTime;
		expectedDocumentCount=expectedDocCount;
	}

	@Override
	public void collectionProcessComplete() {
		long totalTime = System.nanoTime() / 1000000 - stime;
		try (Connection con = DriverManager.getConnection(medicsConnectionString)){
			LegacyMedicsTools.checkAndUpdateAnalysisStatus(con, expectedDocumentCount, analysisId);
			LegacyMedicsTools.UpdateAnalysisProcessTime(con
					, totalTime, analysisId);
			LOG.info("Finished processing");
		} catch (Exception e) { e.printStackTrace();}

	}

	@Override
	public void aborted() {
		collectionProcessComplete();
		
	}

	@Override
	public void batchProcessComplete() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initializationComplete() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void paused() {
		pauseTime = System.nanoTime() / 1000000;
		
	}

	@Override
	public void resumed() {
		restartTime = System.nanoTime() / 1000000;
		long pausedFor = restartTime-pauseTime;
		stime = stime + pausedFor;
		
	}

	@Override
	public void entityProcessComplete(CAS arg0, EntityProcessStatus arg1) {
		// TODO Auto-generated method stub
		
	}

}
