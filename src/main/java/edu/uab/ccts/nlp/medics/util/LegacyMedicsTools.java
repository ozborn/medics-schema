package edu.uab.ccts.nlp.medics.util;

import edu.uab.ccts.nlp.uima.ts.NLP_Clobs;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;

import java.text.Normalizer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;

import org.apache.poi.hwpf.extractor.WordExtractor;

import org.apache.uima.collection.CollectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Was CollectionReaderTools with all simple CRUD or CRUD-like operations
 * on medics extracted with the Oracle dependency removed.
 * Does NOT figure out what Metamap port to use
 * Does NOT insert a MedicsAnalysis due to tangled logic
 * Does NOT include analysisAlreadyRun due to software version issues
 * Does NOT include insertMedicsAnalysis
 * Does NOT include insertNovelAnalysis
 * @author josborne
 *
 */
public class LegacyMedicsTools {
	
    private static final Logger logger = LoggerFactory.getLogger(LegacyMedicsTools.class);


	/**
	 * Fetches a String in its original coding with
	 * newlines converted to \n from a Clob
	 * @param ptl
	 * @return
	 * @throws Exception
	 */
	public static String fetchDoc(Clob someclob) throws Exception {
		String line;
		String s;
		StringBuffer wholedoc = new StringBuffer((int)someclob.length());
		try (Reader reader = someclob.getCharacterStream()) {
			BufferedReader br = new BufferedReader(reader);
			while((line = br.readLine()) != null) {
				wholedoc.append(line+"\n");
			}
		} catch (Exception e) {
			// Problem with Word conversion from Clob approach - try extracting text via Tika
			logger.warn("Character stream converseion failed for Clob, trying Tika approach..");
			try(InputStream is = someclob.getAsciiStream()){
				Metadata metadata = new Metadata();
				s = tikaFetchDoc(null, is, metadata);
				wholedoc = new StringBuffer(s);
			}
			catch(Exception te) {
				logger.error("Tika exception after Tika asci extraction failure");
				te.printStackTrace();
				throw te;
			} 
		}
		return wholedoc.toString();
	}


	public static String fetchDoc(Blob pdf) throws Exception {
		String s = null;
		try (InputStream is = pdf.getBinaryStream())
		{
			BodyContentHandler bch = new BodyContentHandler();
			ParseContext pcontext = new ParseContext();
			Metadata md = new Metadata();
			PDFParser parser = new PDFParser();
			// Slows it down by about 10 percent
			parser.getPDFParserConfig().setSortByPosition(true);
			parser.parse(is, bch, md,pcontext);
			s = bch.toString();
		} catch (Exception e) {
			try{
				Metadata metadata = new Metadata();
				s = tikaFetchDoc(pdf, null, metadata);								
			}
			catch(Exception te) {
				logger.warn("PDFBox and Tika Blob extraction failures");
				te.printStackTrace();
				throw te;
			}
		}
		return s;
	}


	/**
	 * Uses tika to autodetect the document type and return a String. If blob
	 * is set to null it uses the InputStream iis
	 * @param blob
	 * @param iis Non-blob input stream for Tika to parse, will be closed
	 * @param metaData
	 * @return
	 * @throws IOException
	 * @throws SAXException
	 * @throws TikaException
	 * @throws Exception
	 */
	public static String tikaFetchDoc(Blob blob, InputStream iis, Metadata metaData) throws IOException,SAXException, TikaException , Exception{
		String s = null;
		AutoDetectParser parser = new AutoDetectParser();
		BodyContentHandler handler = new BodyContentHandler(-1); // writeLimit = size of internal String buffer
		InputStream is = null;
		try {
			if(blob != null) {
				is = blob.getBinaryStream();
				parser.parse(is, handler, metaData, new ParseContext());
			}
			else
				parser.parse(iis, handler, metaData, new ParseContext());

			s = handler.toString();
		} catch (IOException ioe) { 
			logger.error("Tika IO Exception on input stream: "+ioe.getMessage());
			throw(ioe);
		} catch (SAXException saxe) {
			logger.error("Tika Sax Exception - SAX events could not be processed");
			throw(saxe);
		} catch (TikaException tikae) {
			logger.error("Tika Exception - the document could not be parsed - may be corrupt");
			throw(tikae);
		} catch (Exception e) {
			logger.error("Other Tika related exception "+e.getMessage());
			throw e;
		} finally {
			if(is != null) is.close();
			if(iis != null) iis.close();
		}
		return s;
	}


	/**
	 * Assumes we know the Clob is 1917 HTML format lacking HTML tags
	 * If we fail, just return the original text
	 * @param broken_html
	 */
	public static String detagHTML(String broken_html){
		String plainText = null;
		broken_html = "<HTML>"+broken_html+"</HTML>";
		try (InputStream input = new ByteArrayInputStream(broken_html.getBytes("UTF-8"))){
			ContentHandler handler = new BodyContentHandler();
			Metadata metadata = new Metadata();
			new HtmlParser().parse(input, handler, metadata, new ParseContext());
			plainText = handler.toString();
		} catch (UnsupportedEncodingException e) {
			logger.warn("Detagging HTML failed, cant' handle UTF-8");
			e.printStackTrace();
			return broken_html;
		} catch (Exception e) {
			e.printStackTrace();
			return broken_html;
		}
		return plainText;
	}



	/**
	 * Uses Apache POI WordExtractor to process Word Documents
	 * @param wordBlob
	 * @return
	 * @throws Exception
	 */
	public static String fetchWordDoc(Blob wordBlob) throws Exception {
		String s = null;
		try (   InputStream is = wordBlob.getBinaryStream();
				WordExtractor wordExt = new WordExtractor(is);){
			s = wordExt.getText();
			wordExt.close();
		} catch (Exception e) {
			logger.warn("Word text extraction exception (Blob) - trying Tika approach");
			try{
				Metadata metadata = new Metadata();
				s = tikaFetchDoc(wordBlob, null, metadata);								
			}
			catch(Exception te) {
				logger.error("Tika failed to convert (Word) blob.."+te.getMessage());
				te.printStackTrace();
				throw te;
			}			
		}
		return s;
	}
	
	

	/**
	 * Want to convert a UTF String to ASCII and strip control characters
	 * 1) Normalize to handle diacritical marks, etc..
	 * 2) Get only the ASCII characters
	 * 3) Remove ASCII control characters 
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public static String convertUTFtoASCII(String input) throws Exception {
		String not_ascii = "[^\\x00-\\x7F]";
		//String control = "\\p{Cntrl}";	
		//String bad_control1 = "[\u0000-\u0009]";
		//String bad_control2 = "[\u000e-\u001f]";
		String bestfix = input;
		try {
			String s1 = convertUTF16Whitespace2AsciSpace(input);
			String s2 =  Normalizer.normalize(s1,Normalizer.Form.NFKD);
			bestfix = s2.replaceAll(not_ascii, "");
			//String s3 = s2.replaceAll(bad_control1, "");
			//bestfix = s3.replaceAll(bad_control2, "");
		} catch (Exception e) {
			System.err.println("Failed to convert to ASCII!");
		}
		return bestfix;
	}

	/**
	 * See http://www.cs.tut.fi/~jkorpela/chars/spaces.html
	 * Java natively encodes in UTF-16 and does not recognize or
	 * convert non-ASCII whitespace. I give whitespace ASCII
	 * approximations. See also 
	 * http://stackoverflow.com/questions/4731055/whitespace-matching-regex-java
	 * where some code was taken and modified
	 * @param input
	 * @return
	 */
	public static String convertUTF16Whitespace2AsciSpace(String input) {
		//FIXME whitespace_chars exists only for documentation
		String whitespace_chars = 
				"\\u0085" // NEXT LINE (NEL) 
				+ "\\u00A0" // NO-BREAK SPACE
				+ "\\u1680" // OGHAM SPACE MARK
				+ "\\u180E" // MONGOLIAN VOWEL SEPARATOR
				+ "\\u2000" // EN QUAD 
				+ "\\u2001" // EM QUAD 
				+ "\\u2002" // EN SPACE
				+ "\\u2003" // EM SPACE
				+ "\\u2004" // THREE-PER-EM SPACE
				+ "\\u2005" // FOUR-PER-EM SPACE
				+ "\\u2006" // SIX-PER-EM SPACE
				+ "\\u2007" // FIGURE SPACE
				+ "\\u2008" // PUNCTUATION SPACE
				+ "\\u2009" // THIN SPACE
				+ "\\u200A" // HAIR SPACE
				+ "\\u2028" // LINE SEPARATOR
				+ "\\u2029" // PARAGRAPH SEPARATOR
				+ "\\u202F" // NARROW NO-BREAK SPACE
				+ "\\u205F" // MEDIUM MATHEMATICAL SPACE
				+ "\\u3000" // IDEOGRAPHIC SPACE
		;
		//String whitespace_charclass = "["  + whitespace_chars + "]";
		String s = input;
		s = s.replaceAll("\\u0085","\r\n");
		s = s.replaceAll("\\u00A0"," ");
		s = s.replaceAll("\\u1680","-");
		s = s.replaceAll("\\u180E"," ");
		s = s.replaceAll("\\u2000","  ");
		s = s.replaceAll("\\u2001","    ");
		s = s.replaceAll("\\u2002"," ");
		s=s.replaceAll("\\u2003","  ");
		s=s.replaceAll("\\u2004","   ");
		s=s.replaceAll("\\u2005","    ");
		s=s.replaceAll("\\u2006","      ");
		s=s.replaceAll("\\u2007"," ");
		s=s.replaceAll("\\u2008"," ");
		s=s.replaceAll("\\u2009"," ");
		s=s.replaceAll("\\u200A"," ");
		s=s.replaceAll("\\u2028","\r\n");
		s=s.replaceAll("\\u2029","\r\n\r\n");
		s=s.replaceAll("\\u202F"," ");
		s=s.replaceAll("\\u205F"," ");
		s=s.replaceAll("\\u3000"," ");
		return s;
	}


	public static int countDocuments(String url, String target, 
			String whereClause) throws Exception{
		Connection con = null;
		Statement st = null;
		int number = 0;
		String query = null;
		try {
			con =  DriverManager.getConnection(url);
			st = con.createStatement();
			query = "SELECT COUNT(*) FROM "+target+" "+whereClause;
			ResultSet resultset = (ResultSet) st.executeQuery(query);
			resultset.next();
			number = resultset.getInt(1);
			resultset.close();
			System.out.println("Count Query:"+query);
		} catch (Exception e) {
			System.err.println("Failed query was:"+query);
			throw e; 
		} finally {
			con.close();
			st.close();
		}
		return number;
	}


	/**
	 * For counting using database links
	 * @param url
	 * @param target
	 * @param whereClause
	 * @param hint
	 * @return
	 * @throws Exception
	 */
	public static int countDocumentsRemote(String url, String target, 
			String whereClause, String hint) throws Exception{
		Connection con = null;
		Statement st = null;
		int number = 0;
		String query = null;
		try {
			con =  DriverManager.getConnection(url);
			st = con.createStatement();
			query = "SELECT "+hint+" COUNT(*) FROM "+target+" "+whereClause;
			ResultSet resultset = (ResultSet) st.executeQuery(query);
			resultset.next();
			number = resultset.getInt(1);
			resultset.close();
			System.out.println("Count Query:"+query);
		} catch (Exception e) {
			System.err.println("Failed query was:"+query);
			throw e; 
		} finally {
			con.close();
			st.close();
		}
		return number;
	}


	public static int countDocuments(Connection con, String target, 
			String whereClause) throws SQLException{
		Statement st = null;
		int number = 0;
		String query = null;
		try {
			st = con.createStatement();
			query = "SELECT COUNT(*) FROM "+target+" "+whereClause;
			ResultSet resultset = (ResultSet) st.executeQuery(query);
			resultset.next();
			number = resultset.getInt(1);
			System.out.println("Count Query:"+query);
			resultset.close();
		} catch (SQLException e) {
			System.err.println("Failed query was:"+query);
			throw e; 
		} finally {
			st.close();
		}
		return number;
	}


	/**
	 * Retrieve the current day - offsetdays date in Oracle format in the 1st element
	 * The 2nd element contains trunc(sysdate)
	 * @param url
	 * @param offset
	 * @return
	 * @throws Exception
	 */
	public static Date getOracleSysDate(String url, int offset) throws Exception{
		Connection con = null;
		Statement st = null;
		String query = null;
		Date result = null;
		try {
			con =  DriverManager.getConnection(url);
			st = con.createStatement();
			query ="SELECT trunc(sysdate-"+(new Integer(offset)).toString()+"), trunc(sysdate) FROM dual";
			ResultSet resultset = (ResultSet) st.executeQuery(query);
			resultset.next();
			result = resultset.getDate(1);
			resultset.close();
		} catch (Exception e) {
			System.err.println("Failed date query with offset:"+offset+" on url:"+url);
			System.err.println("Query was:"+query+" on url:"+url);
			e.printStackTrace();
			throw e; 
		} finally {
			con.close();
			st.close();
		}
		return result;
	}


	/**
	 * Gets Integer identifiers from Medics (or other source) as the first column
	 * returned from the SQL and returns all results as a HashSet
	 * @param sql
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static HashSet<Integer> getIntegerIdentifiers(String sql, String url)
			throws Exception {
		HashSet<Integer> hs = new HashSet<Integer>();
		try (Connection con =  DriverManager.getConnection(url);
			Statement st = con.createStatement();){
			ResultSet resultset = st.executeQuery(sql);
			while(resultset.next()){
				String s = resultset.getString(1);
				hs.add(Integer.parseInt(s));
			}
			resultset.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw e; 
		} 
		return hs;
	}

	public static HashSet<Double> getDoubleIdentifiers(String sql, String url)
			throws Exception {
		HashSet<Double> hs = new HashSet<Double>();
		try (Connection con =  DriverManager.getConnection(url);
			Statement st = con.createStatement();){
			ResultSet resultset = st.executeQuery(sql);
			while(resultset.next()){
				String s = resultset.getString(1);
				hs.add(Double.parseDouble(s));
			}
			resultset.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw e; 
		} 
		return hs;
	}

	
	
	

	/**
	 * Get a Medics document from the database as a string,
	 * ensures that any subsequently document analysis is 
	 * using the same document and no database conversions
	 * have occurred
	 * @param id
	 * @return
	 */
	public static String fetchMedicsClobAsString(int id,
			String url) throws Exception, SQLException {
		Connection con = null;
		Statement st = null;
		String query = null;
		String outstring = null;
		try {
			con =  DriverManager.getConnection(url);
			st = con.createStatement();
			query = "SELECT NC_CLCONT FROM medics.NLP_DOCS WHERE NC_REPORTID="+
					id;
			ResultSet resultset = (ResultSet) st.executeQuery(query);
			resultset.next();
			Clob thetext = resultset.getClob(1);
			BufferedReader br = new BufferedReader(thetext.getCharacterStream());
			StringWriter sw = new StringWriter();
			int c = -1;
			while( (c=br.read())!=-1) {
				sw.write(c);
			}
			sw.flush();
			outstring = sw.toString();
			resultset.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to retrieved CLOB in NLP_DOCS for nc_reportid "+id);
			throw e;			 
		} finally {
			con.close();
			st.close();
		}
		return outstring;
	}

	
	
	


	/**
	 * Caller must close Connection, ResultSet and statement
	 * @param whereClause
	 * @return
	 */
	public static ResultSet fetchDocumentResultSet(Connection con, Statement _st,
			String whereClause, String url, String target, String select)
					throws SQLException {
		ResultSet resultset = null;
		try {
			if (con == null) {
				String deadconnection = "Dead (null) database connection!?!\n";
				logger.error(deadconnection);
				con =  DriverManager.getConnection(url);
			}
			_st = con.createStatement();
			String query ="SELECT "+select+" FROM "+target+" "+whereClause;
			logger.info("Resultset query was:"+query);
			resultset = (ResultSet) _st.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return resultset;
	}


	public static void updateDocumentHistory(Connection conn, int doc_id,
			int analysis_id, int status) throws SQLException {
		String updateTableSQL = "UPDATE NLP_DOC_HISTORY "
				+"SET DOC_FINISH_TIME = SYSDATE, STATUS_ID=? "+
				" WHERE ANALYSIS_ID= ? AND DOCUMENT_ID= ? ";
		PreparedStatement preparedStatement = conn.prepareStatement(updateTableSQL);
		preparedStatement.setInt(1, status );
		preparedStatement.setInt(2, analysis_id );
		preparedStatement.setInt(3, doc_id );
		preparedStatement.executeUpdate();
		preparedStatement.close();
	}
	
	
	/**
	 * @param conn
	 * @param doc_set_description
	 * @return
	 * @throws SQLException
	 */
	public static int InsertDocSet(Connection conn, String doc_set_description
			) throws SQLException {
		int docSetID = -1;

		String insertTableSQL = "INSERT INTO NLP_DOCSET "
				+ "( DESCRIPTION ) VALUES (?)  ";
		PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, new String[]{"DOCSET_ID"});
		preparedStatement.setString(1, doc_set_description );
		preparedStatement.executeUpdate();
		try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys() ) {
			if (generatedKeys.next()) {
				docSetID = generatedKeys.getInt(1);
				generatedKeys.close();
			} else {
				throw new SQLException("Creating NLP_DOCSET, no generated key obtained.");
			}
		}
		preparedStatement.close();
		return docSetID;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param analysis_type (ex. Cancer Detection Pipeline)
	 * @param analysis_software (ex UIET)
	 * @param dataset
	 * @param caspoolsize
	 * @param p_description
	 * @param p_status
	 * @param software_version (like 0.93)
	 * @return
	 * @deprecated
	 * @throws SQLException
	 */
	public static int InsertAnalysis(Connection conn,
			String analysis_type, String analysis_software,
			int dataset, int caspoolsize, String p_description, 
			int p_status, String software_version) throws SQLException {
		int analysisID = -1;

		String insertTableSQL = "INSERT INTO NLP_ANALYSIS"
				+ "(  ANALYSIS_TYPE, ANALYSIS_SOFTWARE, "+
				"ANALYSIS_DATASET, ANALYSIS_START_DATE , ANALYSIS_DESCRIPTION, ANALYSIS_STATUS, "+
				"ANALYSIS_SOFTWARE_VERSION, MACHINE, THREAD_COUNT "+
				") VALUES (?,?,?,SYSDATE,?,?,?,?,?)  ";
		PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, new String[]{"ANALYSIS_ID"});
		preparedStatement.setString(1, analysis_type );
		preparedStatement.setString(2, analysis_software );
		preparedStatement.setInt(3, dataset);
		preparedStatement.setString(4, p_description);
		preparedStatement.setInt(5, p_status);
		preparedStatement.setString(6, software_version);
		try {
			preparedStatement.setString(7, InetAddress.getLocalHost().getHostName() );
		} catch (UnknownHostException e) {
			preparedStatement.setString(7, "InetAddress.getLocalHost().getHostName " + e.getMessage());
		}
		preparedStatement.setInt(8, caspoolsize);

		preparedStatement.executeUpdate();
		try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()){
			if (generatedKeys.next()) {
				analysisID = generatedKeys.getInt(1);
			} else {
				throw new SQLException("Creating NLP_ANALYSIS, no generated key obtained.");
			}
		}
		preparedStatement.close();

		return analysisID;
	}


	/**
	 *
	 * @param conn
	 * @param worktime
	 * @param analysis_id
	 * @return
	 * @throws SQLException
	 */
	public static void UpdateAnalysisProcessTime(Connection conn, long worktime, int analysis_id  ) throws SQLException {
		String updateTableSQL = "UPDATE NLP_ANALYSIS"
				+ " SET PROCESSING_TIME = ? , ANALYSIS_STOP_DATE = SYSDATE  WHERE  ANALYSIS_ID = ? ";
		try (PreparedStatement preparedStatement = conn.prepareStatement(updateTableSQL)){
			preparedStatement.setLong(1, worktime);
			preparedStatement.setInt(2, analysis_id );
			preparedStatement.executeUpdate();
		}
	}

	public static void UpdateAnalysisStatus(Connection conn, int analysis_status, 
			int analysis_id) throws SQLException{
		String updateTableSQL = "UPDATE NLP_ANALYSIS"
				+ " SET ANALYSIS_STATUS = ? WHERE  ANALYSIS_ID = ? ";
		try (PreparedStatement preparedStatement = conn.prepareStatement(updateTableSQL)){
			preparedStatement.setInt(1, analysis_status);
			preparedStatement.setInt(2, analysis_id );
			preparedStatement.executeUpdate();
		}
	}


	/**
	 * @deprecated Use DocumentAnalysisJudgeAnnotator
	 * @param conn
	 * @param expected_docs
	 * @param analysis_id
	 * @throws SQLException
	 */
	public static void checkAndUpdateAnalysisStatus(Connection conn, int expected_docs, 
			int analysis_id) throws SQLException{
		int status=1;
		int observed_docs=-1;
		logger.info("Expecting "+expected_docs+" docs");
		if(expected_docs!=0){
			String target = " NLP_DOC_HISTORY ";
			String where_clause=" WHERE ANALYSIS_ID="+analysis_id+
					" AND (status_id=" +MedicsConstants.DOCUMENT_ANALYSIS_COMPLETE+
					" OR status_id="+MedicsConstants.DOCUMENT_DONE_ELSEWHERE+")";
			observed_docs = LegacyMedicsTools.countDocuments(conn,target,where_clause);
			if(observed_docs==expected_docs) {
				status=MedicsConstants.ANALYSIS_SUCCESS_STATUS;
			}
			else if(observed_docs==0) {
				status=MedicsConstants.ANALYSIS_FAIL_STATUS;
			}
			else {
				float worked=((float)observed_docs/(float)expected_docs)*100;
				if(worked>MedicsConstants.ANALYSIS_DOC_PASS_RATE_REQUIRED) {
					status=MedicsConstants.ANALYSIS_MOSTLY_WORKED_STATUS;
				} else {
					status=MedicsConstants.ANALYSIS_PARTIAL_STATUS;
				}
			}
		} else status=MedicsConstants.ANALYSIS_EMPTY_STATUS;
		UpdateAnalysisStatus(conn, status, analysis_id);
		logger.info("Updating analysis "+analysis_id+" with status "+status+"");
	}



	/**
	 * Connection recycled for speed
	 * @param conn
	 * @param doc_id
	 * @param analysis_id
	 * @param status
	 * @throws SQLException
	 */
	public static void insertDocumentHistory(Connection conn, int doc_id,
			int analysis_id, int status) throws SQLException {

		try{
			String insertTableSQL = "INSERT INTO NLP_DOC_HISTORY"
					+"(DOCUMENT_ID,ANALYSIS_ID,STATUS_ID,DOC_START_TIME)"+
					" VALUES (?,?,?,SYSDATE) ";
			PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL);
			preparedStatement.setInt(1, doc_id );
			preparedStatement.setInt(2, analysis_id );
			preparedStatement.setInt(3, status );
			preparedStatement.addBatch();
			preparedStatement.executeBatch();
			preparedStatement.close();
		}catch(SQLException sex){
			System.out.println("Exception in insertDocumentHistory doc_id=["+doc_id+"] analysis_id=["+analysis_id+"] status=["+status+"] "+sex.getMessage());
			throw sex;
		}
	}

	/**
	 * 
	 * Connection recycled for speed
	 * @param conn
	 * @param docset_id
	 * @param document_id
	 * @param doc_mrn
	 * @throws SQLException
	 */
	public static void insertDocumentDocSetMap(Connection conn, int docset_id, int document_id,
			String doc_mrn) throws SQLException {

		try{
			String insertTableSQL = "INSERT INTO NLP_DOCSET_MAP "
					+"(DOCSET_ID, DOCUMENT_ID, DOC_MRN )"+
					" VALUES (?,?,? ) ";
			PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL);
			preparedStatement.setInt(1, docset_id );
			preparedStatement.setInt(2, document_id );
			preparedStatement.setString(3, doc_mrn );
			preparedStatement.addBatch();
			preparedStatement.executeBatch();
			preparedStatement.close();
		}catch(SQLException sex){
			System.err.println("Exception in insertDocumentDocSetMap docset_id=["+docset_id+"] document_id=["+document_id+"] doc_mrn=["+doc_mrn+"] "+sex.getMessage());
			throw sex;
		}
	}



	public static String getAnalysisStartDate(Connection con, int analysis_id) {
		Date d=null;
		try (Statement st = con.createStatement()) {
			String query ="SELECT analysis_start_date FROM NLP_ANALYSIS WHERE "+
					" analysis_id="+analysis_id;
			System.out.println("Analysis start date query was:"+query);
			try (ResultSet resultset = (ResultSet) st.executeQuery(query)){
				if ( !resultset.next() ){
					return null;
				}
				d = resultset.getDate(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return d.toString();
	}



	public static Integer getAnalysisStatus(String url,int analysis_id) {
		Integer d=null;
		try (Connection con =  DriverManager.getConnection(url)){
			try (Statement st = con.createStatement()) {
				String query ="SELECT analysis_status FROM NLP_ANALYSIS WHERE "+
						" analysis_id="+analysis_id;
				try (ResultSet resultset = (ResultSet) st.executeQuery(query)){
					if ( !resultset.next() ){
						return null;
					}
					d = new Integer(resultset.getInt(1));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return d;
	}

	
	public static Integer getDocumentWorkStatus(String url,
		int analysis_id, int doc_id) {
		Integer d=null;
		try (Connection con =  DriverManager.getConnection(url)){
			try (Statement st = con.createStatement()) {
				String query ="SELECT status_id FROM NLP_DOC_HISTORY WHERE "+
						" analysis_id="+analysis_id+" and document_id="+doc_id;
				try (ResultSet resultset = (ResultSet) st.executeQuery(query)){
					if ( !resultset.next() ){
						return null;
					}
					d = new Integer(resultset.getInt(1));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return d;
	}



	/**
	 * Get a string in the format dd-MM-yyyy representing yesterday
	 * as per the system calendar
	 * @return
	 */

	public static String getDaysAgoDateString(int days_ago){
		DateFormat dateformat = new SimpleDateFormat("dd-MM-yyyy");
		Calendar cal = Calendar.getInstance();
		int days = -1 * days_ago;
		cal.add(Calendar.DATE, days); //1 day ago
		java.util.Date oneday = cal.getTime();
		return dateformat.format(oneday);		
	}



	public static void fillNLP_Clobs(String source, String url, Object reportid,
			java.sql.Date dn_update_datetime, String type, String subtype,
			String mrn, Integer version, int generated_key, NLP_Clobs pprop)
					throws NumberFormatException {
		pprop.setURL(url);
		if(mrn!=null) pprop.setMRN(Integer.parseInt(mrn.toString()));
		pprop.setReportID( (new Integer(generated_key)));
		pprop.setDateOfService(dn_update_datetime.toString());
		pprop.setDocumentTypeAbbreviation(type);
		pprop.setDocumentSubType(subtype);
		pprop.setDocumentVersion(version);
		pprop.setSource(source);
		pprop.setSourceID(reportid.toString());
	}


	public static  int insertMedicsDocument(String source, Object reportid,

			String blobtype, java.sql.Date dn_update_datetime, String type,
			String subtype, String mrn, Integer version, Connection medcon,
			int analysis_id, int docset_id)
					throws SQLException, CollectionException {
		int generated_key;
		try {
			generated_key = insertDocument(medcon,reportid.toString(),mrn,
					dn_update_datetime,source,type,subtype,blobtype,version);

			LegacyMedicsTools.insertDocumentHistory(medcon,generated_key,analysis_id,1);
			LegacyMedicsTools.insertDocumentDocSetMap(medcon,docset_id,generated_key,mrn);

			if(mrn==null) {
				throw new CollectionException("Null MRN in "+reportid.toString(), null);
			} else if(mrn.toString().indexOf("00000000000")!=-1) {
				throw new CollectionException("Fake patient in "+reportid.toString(), null);
			} else { /** Valid MRN **/ }
		} catch (CollectionException e1) {
			System.out.println("CollectionException with medics insertions:"+e1.getMessage());
			if(medcon!=null) medcon.close();
			throw e1;
		}
		return generated_key;
	}

	private static int insertDocument(Connection conn, String source_id,
			String mrn,java.sql.Date dn_update_datetime, String source,
			String type, String subtype, String mimetype, Integer version)
					throws SQLException {
		int documentIdentifier = -1;
		if(mimetype==null) mimetype="text";
		String insertTableSQL = "INSERT INTO NLP_DOCS"
				+"(NC_SOURCE_ID,NC_PT_MRN,NC_DOS,NC_SOURCE,NC_TYPE,NC_SUBTYPE,"+
				"NC_VERSION) VALUES (?,?,?,?,?,?,?) ";
		try (PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, new String[]{"NC_REPORTID"})){
			preparedStatement.setString(1, source_id );
			preparedStatement.setString(2, mrn );
			preparedStatement.setDate(3, dn_update_datetime );
			preparedStatement.setString(4, source);
			preparedStatement.setString(5, type );
			preparedStatement.setString(6, subtype );
			preparedStatement.setInt(7, version );
			//preparedStatement.setTimestamp(8, getCurrentTimestamp());
			preparedStatement.executeUpdate();
			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()){
				if (generatedKeys.next()) {
					documentIdentifier = generatedKeys.getInt(1);
				} else {
					throw new SQLException("Creating NLP_DOCS, no generated key obtained.");
				}
			}
		} catch (SQLIntegrityConstraintViolationException sql_integrity) {
			//Trying to insert duplicate document
			try (PreparedStatement pst = conn.prepareStatement
					("SELECT NC_REPORTID FROM NLP_DOCS WHERE "+
							"NC_SOURCE=? AND NC_SOURCE_ID=? AND NC_VERSION=?")){

				String integrity_message =
						"Integrity Violated for document source id:"+source_id+
						" from source "+source+" with version "+version;

				System.out.println(integrity_message);
				pst.setString(1,source);
				pst.setString(2,source_id);
				pst.setInt(3,version);
				try (ResultSet resultset = (ResultSet) pst.executeQuery()){
					resultset.next();
					documentIdentifier = resultset.getInt(1);
				}
				return documentIdentifier;
			} catch (SQLException e) {
				String notfound="Could not find previously inserted document:"+source_id;
				System.out.println(notfound);
				e.printStackTrace();
				throw (e);
			}
		} catch (SQLException sqle) {
			System.out.println("Failed query was:"+insertTableSQL);
			System.out.println("With values: source_id:"+source_id+" mrn:"+mrn+
					" dos:"+dn_update_datetime+" type:"+type+" subtype:"+subtype+
					" version:"+version);
			throw new SQLException(sqle);
		} 
		return documentIdentifier;
	}


}
