package edu.uab.ccts.nlp.medics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Iterator;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;
import edu.uab.ccts.nlp.medics.util.MedicsConstants;
import edu.uab.ccts.nlp.uima.ts.*;

import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.descriptor.ConfigurationParameter;


public class MedicsConsumer extends JCasAnnotator_ImplBase {

	UimaContext theContext = null;
	int analysisId = 0;
	public static final String PARAM_WRITE_HIT_URL="ExtHitWriterConnectionString";

	static final String DBURL_DESCRIPTION="Oracle connection String for writing to database";
	@ConfigurationParameter(
			name = PARAM_WRITE_HIT_URL,
			description = DBURL_DESCRIPTION
			)
	private String ExtHitWriterConnectionString;


	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		theContext = aContext;
		theContext.getLogger().log(Level.CONFIG,"Medics Extended Hit URL is: "+ExtHitWriterConnectionString+"\n");
	}


	public static AnalysisEngineDescription getDescription() throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(MedicsConsumer.class);
	}



	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		FSIndex<Annotation> clobIndex = jcas.getAnnotationIndex(NLP_Clobs.type);
		Iterator<Annotation> clobIter = clobIndex.iterator();
		if(!clobIter.hasNext()) return;
		NLP_Clobs clob = (NLP_Clobs) clobIter.next();
		Integer doc_id= clob.getReportID();

		FSIndex<Annotation> analIndex = jcas.getAnnotationIndex(NLP_Analysis.type);
		Iterator<Annotation> analIter = analIndex.iterator();
		NLP_Analysis anal = (NLP_Analysis) analIter.next();
		analysisId = anal.getAnalysisID();
		theContext.getLogger().log(Level.FINE,"NLP_Analysis of "+analysisId);

		/*
		try {
			for(Iterator<JCas> vit = jcas.getViewIterator();vit.hasNext();){
				theContext.getLogger().log(Level.FINE,"Name of view discovered"
				+vit.next().getViewName());
			}
		} catch (CASException e1) {
			e1.printStackTrace();
		}
		 */

		FSIndex<Annotation> medIndex = jcas.getAnnotationIndex(NLP_Hits_Extended.type);
		Iterator<Annotation> medIter = medIndex.iterator();
		if(!medIter.hasNext()){ 
			theContext.getLogger().log(Level.WARNING,"Default CAS has no NLP_Hits_Extended");
			JCas systemView;
			try {
				systemView = jcas.getView(CAS.NAME_DEFAULT_SOFA);
			} catch (CASException e) {
				throw new AnalysisEngineProcessException(e);
			}
			medIndex = systemView.getAnnotationIndex(NLP_Hits_Extended.type);
			medIter = medIndex.iterator();
		}
		writeNLP_Hits_Extended2Database(medIter);


		try (Connection writeMedicsConn =  DriverManager.getConnection(ExtHitWriterConnectionString)){
			if (writeMedicsConn == null) {
				theContext.getLogger().log(Level.SEVERE,"Could not get connetion to "+
						ExtHitWriterConnectionString+" to write extended hits.");
			}
			int old_status = MedicsConstants.DOCUMENT_HITS_PROCESSING_COMPLETE;
			FSIndex<Annotation> drIndex = jcas.getAnnotationIndex(Document_Results.type);
			Iterator<Annotation> drIter = drIndex.iterator();
			if(drIter.hasNext()) {
				Document_Results dr = (Document_Results) drIter.next();
				old_status = dr.getStatusID();			
			}
			if(old_status<MedicsConstants.DOCUMENT_CONVERSION_FAIL) {
				LegacyMedicsTools.updateDocumentHistory(writeMedicsConn,
						doc_id.intValue(), analysisId, 
						MedicsConstants.DOCUMENT_HITS_PROCESSING_COMPLETE);
			}
		} catch (Exception e) {
			theContext.getLogger().log(Level.WARNING,"Exception getting connection to "+
					ExtHitWriterConnectionString+" to update Document History");
			throw(new AnalysisEngineProcessException(e));
		} 
	}


	/**
	 * Should write all the extended hits associated with this CAS, that is, from
	 * one document
	 * @param results
	 */
	private void writeNLP_Hits_Extended2Database(
			Iterator<Annotation> results) throws AnalysisEngineProcessException {
		String insert_string = "INSERT INTO NLP_HITS_EXTENDED "+
				"(ANALYSIS_ID, DOCUMENT_ID, HIT_TEXT, CONCEPT_TEXT,"+
				" UMLS_CUI, UMLS_SEMANTIC_TYPES, SCORE, MRN, external_hit_id, "+
				"HIT_TEXT_EXTENDED, is_negated, is_possible,SUBJECT, SNOMED_ID,"+
				" HIT_START_CHAR_INDEX, HIT_STOP_CHAR_INDEX,"+
				"COURSE, SEVERITY, IS_CONDITIONAL, IS_GENERIC"+
				", DOCUMENT_SECTION_NAME, MAPPING_SCORE, NER_TYPE ) "+
				"VALUES (?,?,?,?,?,?,?,?,?,?"+
				",?,?,?,?,?,?,?,?,?,?,?,?,?)";
		NLP_Hits_Extended hit = null;
		int rowcount = 0;
		try (
				Connection writeMedicsConn =  DriverManager.getConnection(ExtHitWriterConnectionString);
				PreparedStatement inserthit = writeMedicsConn.prepareStatement(insert_string);
				){
			while(results.hasNext()){
				hit = (NLP_Hits_Extended) results.next();
				Integer hitdocid = hit.getDocumentID();	
				if(analysisId!=0) {
					inserthit.setInt(1,analysisId);
				} else {
					inserthit.setInt(1,hit.getAnalysisID());
				}
				inserthit.setInt(2,hitdocid);
				String hitcheck; //Unsure what is causing this lack of hit text
				if(hit.getHitText()!=null) {
					if(hit.getHitText().equals("")) {
						hitcheck="Not applicable";		
						theContext.getLogger().log(Level.WARNING,"Empty hit in "+
								"analysis "+analysisId+" document "+hitdocid+" pos:"+
								hit.getBegin()+"/"+hit.getEnd());
					} else {
						int hitlen = hit.getHitText().length();
						if(hitlen>MedicsConstants.MAX_ORACLE_VARCHAR2){
							hitcheck = hit.getHitText().substring(0,MedicsConstants.MAX_ORACLE_VARCHAR2); 
						} else hitcheck = hit.getHitText();
					}
				} else {
					hitcheck="Not applicable";		
					theContext.getLogger().log(Level.WARNING,"NULL hit in "+
							"analysis "+analysisId+" document "+hitdocid+" pos:"+
							hit.getBegin()+"/"+hit.getEnd());
				}
				theContext.getLogger().log(Level.FINER,"hitcheck is "+hitcheck);

				inserthit.setString(3,hitcheck);
				inserthit.setString(4,hit.getConceptText());

				String cui = hit.getCUI();
				if(cui!=null) {	inserthit.setString(5,hit.getCUI()); 
				} else { inserthit.setNull(5,Types.VARCHAR);}

				String stype = hit.getUMLSSemanticTypes();
				if(stype!=null) {inserthit.setString(6,stype);
				} else {inserthit.setNull(6,Types.VARCHAR); }

				inserthit.setInt(7,hit.getScore());
				inserthit.setInt(8,hit.getMRN());

				String ehitid = hit.getExternalHitID();
				if(ehitid!=null) { inserthit.setString(9,ehitid);
				} else {inserthit.setNull(9,Types.VARCHAR); }

				String hitTextExtended = hit.getHitTextExtended();
				if(hitTextExtended!=null) { 
					int hl = hitTextExtended.length();
					if(hl>MedicsConstants.MAX_ORACLE_VARCHAR2){
						this.getContext().getLogger().log(Level.WARNING,
								"Truncating hit text extended in document "+
										hit.getDocumentID()+ " to "+
										MedicsConstants.MAX_ORACLE_VARCHAR2+
										" characters, from "+hl+" characters.");
						hitTextExtended = hitTextExtended.substring(0,
								MedicsConstants.MAX_ORACLE_VARCHAR2);
						inserthit.setString(10,hitTextExtended);
					} else {
						inserthit.setString(10, hitTextExtended);
					}
				} else {inserthit.setNull(10,Types.VARCHAR); }

				boolean neg = hit.getIsNegated();
				if(neg) { inserthit.setString(11,"Y"); }
				else { inserthit.setString(11,"N"); }

				boolean possible = hit.getIsPossible();
				if(possible) { inserthit.setString(12,"Y"); }
				else { inserthit.setString(12,"N"); }

				String subject = hit.getSubjectClass();
				if(subject==null||subject.isEmpty()) { inserthit.setString(13,"patient"); }
				else { inserthit.setString(13,subject); }

				String snomedid = hit.getSNOMEDID();
				if(snomedid!=null) { inserthit.setString(14,snomedid);
				} else {inserthit.setNull(14,Types.VARCHAR); }

				inserthit.setInt(15,hit.getBegin());
				inserthit.setInt(16,hit.getEnd());
				inserthit.setString(17,hit.getCourseClass());
				inserthit.setString(18,hit.getSeverityClass());
				inserthit.setInt(19,hit.getIsConditional());
				inserthit.setInt(20,hit.getIsGeneric());

				String docsection = hit.getSegmentName();
				if(docsection!=null) { inserthit.setString(21,docsection);
				} else {inserthit.setNull(21,Types.VARCHAR); }

				inserthit.setInt(22, hit.getMappingScore());

				String nertype = hit.getNERType();
				if(nertype!=null) { inserthit.setString(23,nertype);
				} else {inserthit.setNull(23,Types.VARCHAR); }

				// inserthit.addBatch(); //FIXME Oracle can't do PreparedStatment batch updates?
				rowcount = inserthit.executeUpdate();
				if(rowcount!=1) {
					theContext.getLogger().log(Level.WARNING,
							"Did NOT insert one row "+inserthit.toString());
				} else {
					theContext.getLogger().log(Level.FINER,"Inserted "+rowcount+
							" hit under analysis_id "+analysisId);

				}
			}
			if(rowcount==0) theContext.getLogger().log(Level.FINE,"No hits for document to insert");
		} catch (Exception e) {
			String message = "Failed to insert hits ";
			if(hit!=null) {
				message += " with good hit "+hit.toString();
				if(hit.getDocumentID()>0) {
					message +="for document "+hit.getDocumentID();
				}
				message += "Extended hit length was:"+hit.getHitTextExtended().length();
			}
			theContext.getLogger().log(Level.SEVERE,message);
			e.printStackTrace();
			throw(new AnalysisEngineProcessException(e));
		}
	}


	public static AnalysisEngineDescription createAnnotatorDescription(String
			dburl) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(
				MedicsConsumer.class,
				PARAM_WRITE_HIT_URL,dburl);
	}

}
