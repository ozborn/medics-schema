package edu.uab.ii.nlp.medics.test.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.JCasIterable;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.h2.jdbc.JdbcBatchUpdateException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uab.ccts.nlp.medics.AsciiDocumentAnnotator;
import edu.uab.ccts.nlp.medics.DatabaseIdentifierCollectionReader;
import edu.uab.ccts.nlp.medics.DatabaseUTF16DocumentAnnotator;
import edu.uab.ccts.nlp.medics.util.MedicsConstants;
import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

public class DatabaseIdentifierCollectionReaderIT {
	Connection con = null;
	String testjdbcstring = "jdbc:h2:~/test;USER=SA;PASSWORD=";
	
	@Before
	public void setUp() throws Exception {
		Class.forName("org.h2.Driver");
		con = DriverManager.getConnection("jdbc:h2:~/test","sa","");
		String createtable="CREATE TABLE IF NOT EXISTS DOCS(ID INT PRIMARY KEY, SOMEDOC VARCHAR(255))";
		String inserttable="INSERT INTO DOCS VALUES (?,?)";
		try (Statement st = con.createStatement()){
			st.executeUpdate(createtable);
		}
		try (PreparedStatement ps = con.prepareStatement(inserttable)) {
			ps.setInt(1, 1);
			ps.setString(2, "Test document one");
			ps.addBatch();
			ps.setInt(1, 2);
			ps.setString(2, "Test document two");
			ps.setInt(1, 3);
			ps.setString(2, "Test document three \uc2a9");
			ps.addBatch();
			ps.executeBatch();
			
		} catch (JdbcBatchUpdateException je) {
			if(je.getMessage().indexOf("primary key violation")==-1) {
				throw je;
			}
		}
	}
	
	
	@Test
	public void testDocumentIdFetch() {
		String sql = "SELECT id as document_id FROM DOCS"; 
		String docsql = "SELECT SOMEDOC, 'test' as sourcecol, 1 as "+
		"some_version FROM DOCS"+ " WHERE id=?";
		CollectionReaderDescription crd =  null;
		CollectionReader cr = null;
		try {
			crd=DatabaseIdentifierCollectionReader.getDescriptionFromJDBC(
				testjdbcstring, sql);
			cr = CollectionReaderFactory.createReader(crd);
			if(cr==null) Assert.fail();
			AggregateBuilder builder = new AggregateBuilder();
			AnalysisEngineDescription aed = DatabaseUTF16DocumentAnnotator
				.createAnnotatorDescription(
			docsql,"SOMEDOC",null,"some_version", "sourcecol" );
			AnalysisEngineDescription ascianno = AsciiDocumentAnnotator
				.createAnnotatorDescription(
						MedicsConstants.UTF_VIEW);
			builder.add(aed);
			builder.add(ascianno);

            for (JCas jCas : new JCasIterable(crd, builder.createAggregateDescription()))
            {
            	JCas utfview = jCas.getView(MedicsConstants.UTF_VIEW);
            	JCas asciiview = jCas.getView(MedicsConstants.ASCI_VIEW);
            	Collection<NLP_Clobs> docmetadata = JCasUtil.select(utfview, NLP_Clobs.class);
            	Collection<NLP_Clobs> adocmetadata = JCasUtil.select(asciiview, NLP_Clobs.class);
            	String athesource = adocmetadata.iterator().next().getSource();
            	String thesource = docmetadata.iterator().next().getSource();
            	Integer atheversion = adocmetadata.iterator().next().getDocumentVersion();
            	Integer theversion = docmetadata.iterator().next().getDocumentVersion();
            	Assert.assertTrue(thesource.equals("test"));
            	Assert.assertTrue(theversion==1);
            	Assert.assertTrue(athesource.equals("test"));
            	Assert.assertTrue(atheversion==1);
            	String utext = utfview.getDocumentText();
            	String atext = asciiview.getDocumentText();
            	Assert.assertTrue(utext.startsWith("Test document"));
            	Assert.assertTrue(atext.startsWith("Test document"));
            	if(utext.startsWith("Test document three")) {
            		Assert.assertTrue(atext.length()==utext.length()-1);
            	}
            }
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

}
