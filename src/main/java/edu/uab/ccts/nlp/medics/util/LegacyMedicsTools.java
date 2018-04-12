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
import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.tika.metadata.Metadata;

import org.apache.poi.hwpf.extractor.WordExtractor;

import org.apache.uima.collection.CollectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Was CollectionReaderTools with all simple CRUD or CRUD-like operations
 * on medics extracted with the Oracle dependency removed.
 * JPA should eventually replace most of this
 * Does NOT figure out what Metamap port to use
 * Does NOT insert a MedicsAnalysis due to tangled logic
 * Does NOT include analysisAlreadyRun due to software version issues
 * Does NOT include insertMedicsAnalysis
 * Does NOT include insertNovelAnalysis
 * @author josborne
 *
 */
public class LegacyMedicsTools {
	
    static final Logger logger = LoggerFactory.getLogger(LegacyMedicsTools.class);


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
				MedicsTools mt = new MedicsTools();
				s = mt.tikaFetchDoc(wordBlob, null, metadata);								
			}
			catch(Exception te) {
				logger.error("Tika failed to convert (Word) blob.."+te.getMessage());
				te.printStackTrace();
				throw te;
			}			
		}
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
	public static int InsertDocSet(Connection conn, String doc_set_description,
			String mrn , String startdate, String enddate
			) throws SQLException, ParseException {
		int docSetID = -1;

		String insertTableSQL = "INSERT INTO NLP_DOCSET "
				+ "( DESCRIPTION,MRN,START_DATE,END_DATE ) VALUES (?,?,?,?)  ";
		PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, new String[]{"DOCSET_ID"});
		preparedStatement.setString(1, doc_set_description );
		if(mrn==null) mrn="";
		preparedStatement.setString(2, mrn );
		preparedStatement.setDate(3, convertString2SqlDate(startdate) );
		java.sql.Date edate = null;
		if(enddate==null || enddate.isEmpty()) edate = getTodaysDate();
		else edate= convertString2SqlDate(enddate);
		preparedStatement.setDate(4, edate);
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
	
	
	public static java.sql.Date convertString2SqlDate(String date) throws ParseException{
		java.sql.Date sqldate = null;
		DateFormat dateformat = new SimpleDateFormat("dd-MMM-yyyy");
		java.util.Date udate = dateformat.parse(date);
		sqldate = new java.sql.Date(udate.getTime());
		return sqldate;
	}
	
	
	public static java.sql.Date getTodaysDate() throws ParseException{
		java.sql.Date sqldate = null;
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 0); //Today
		java.util.Date today = cal.getTime();
		sqldate = new java.sql.Date(today.getTime());
		return sqldate;
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


	/**
	 * @param medicsurl
	 * @param analysisType
	 * @param mrn
	 * @param comparisonDate
	 * @param comparisonDate2
	 * @param daybackoffset
	 * @param cflo_rec_type
	 * @param icda_rec_type
	 * @param analysis_description
	 * @param docset_description
	 * @return
	 * @throws Exception
	 */
	public static Hashtable<String,String> insertMedicsAnalysis(String medicsurl,
			int analysisType, String mrn, String comparisonDate, String comparisonDate2,
			int daybackoffset, String cflo_rec_type, String icda_rec_type, 
			String analysis_description, String docset_description,
			String softwarename, String softwareversion, int thread_count,
			int pipeline_type) throws Exception {
		Hashtable<String,String> result = new Hashtable<String,String>();
	
		String datenow = getDaysAgoDateString(0);
		result.put("analysis_start_date", datenow);
		String docsetdesc = "Unknown document set";
		String analysisdesc = "Unknown analysis";
		String pipelinestring = getQueueNameByLegacyId(medicsurl,analysisType);
		if(softwarename==null)softwarename="Unknown";
		if(softwareversion==null)softwareversion=MedicsConstants.DEFAULT_ANALYSIS_VERSION;
	
	
		// Generic docset description
		if(icda_rec_type!=null && icda_rec_type.isEmpty()==false){
			docsetdesc = "ICDA ("+icda_rec_type+") "; 
			if(cflo_rec_type!=null && cflo_rec_type.isEmpty()==false){
				docsetdesc += "Careflow ("+cflo_rec_type+") "; 
			}
		} else {
			if(cflo_rec_type!=null && cflo_rec_type.isEmpty()==false){
				docsetdesc = "Careflow ("+cflo_rec_type+") "; 
			}
		} 
		
	
		if(analysisType==MedicsConstants.PATH_REPORT_ANALYSIS_ID) {
			docsetdesc = "ICDA PTH Notes signed from ";
			analysisdesc = "Cancer Pipeline on ICDA PTH Notes signed on ";
			pipelinestring = "CancerDetectionPipeline";
			if(comparisonDate==null || comparisonDate.isEmpty()) {
				docsetdesc+=getDaysAgoDateString(daybackoffset);
				analysisdesc+=getDaysAgoDateString(daybackoffset)+
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
		} else if(analysisType == MedicsConstants.SEMEVAL_2014_TASK7_ANALYSIS){
			pipelinestring = "SemEvalDetectionPipeline";
			docsetdesc = " SemEval Task 7 2014 Docs";
			analysisdesc = pipelinestring+" on "+datenow;
		} else if(analysisType == MedicsConstants.SHARECLEF_2014_POST_COORDINATION_ANALYSIS){
			pipelinestring = "SemEvalPostCoordinationCUIlessPipeline";
			docsetdesc = "SemEval Task7 CUIless Concepts";
		} else if(analysisType == MedicsConstants.MELANOMA_DETECTION_ANALYSIS_ID){
			pipelinestring = "MultipleMyelomaDetectionPipeline";
		} else if (analysisType == MedicsConstants.MELANOMA_EXTRACTION_ANALYSIS_ID){
			pipelinestring = "MultipleMyelomaExtractionPipeline";
		} else if (analysisType == MedicsConstants.OSTEOPENIA_DETECTION_ANALYSIS_ID){
			pipelinestring = "OsteopeniaDetectionPipeline";
		} else if (analysisType == MedicsConstants.BONE_LESION_DETECTION_ANALYSIS_ID){
			pipelinestring = "BoneLesionLucencyDetectionPipeline";
		} else if (analysisType == MedicsConstants.UPDATE_HASHCODE_ANALYSIS){
			pipelinestring = "UpdateHashCodePipeline";
		} else if (analysisType == MedicsConstants.WORD2VEC_MODEL_CREATION_ANALYSIS){
			pipelinestring = "Word2VecModelCreationPipeline";
		} else if (analysisType == MedicsConstants.EEG_SEIZURE_ANALYSIS){
			pipelinestring = "PdfIcuEegSeizureDetectionPipeline";
		} else {
			if(!(mrn==null || mrn.isEmpty())) {
				docsetdesc += " (MRN "+mrn+")";
			}
			if(analysis_description!=null) analysisdesc = analysis_description;
			if(docset_description!=null) docsetdesc = docset_description;
		}
	
		if(analysisType!=MedicsConstants.PATH_REPORT_ANALYSIS_ID){
			docsetdesc += " up to "+datenow;
			analysisdesc = pipelinestring+" on "+datenow;
		}
		try (Connection conn =  DriverManager.getConnection(medicsurl)){
			int docset_id = InsertDocSet(conn,docsetdesc,
					mrn,comparisonDate,comparisonDate2);
			int analysis_id= InsertAnalysis(conn,
					pipelinestring,softwarename,docset_id,
					//ConfigurationSingleton.PTH_METAMAP_SERVER_ALLOCATION,
					thread_count,
					analysisdesc,
					MedicsConstants.ANALYSIS_INITATED_STATUS,softwareversion,
					pipeline_type);
	
			String sanalysisid=Integer.toString(analysis_id);
			//Fill up our hash
			result.put("analysis_id",sanalysisid);
			result.put("docset_id",Integer.toString(docset_id));
			result.put("analysis_type",Integer.toString(analysisType));
		} catch (Exception e) {
			e.printStackTrace();
			throw(e);
		}
		return result;
	}


	/**
	 * @param conn
	 * @param analysis_type
	 * @param analysis_software
	 * @return
	 * @throws SQLException
	 */
	public static int InsertAnalysis(Connection conn,
			String analysis_type, String analysis_software,
			int dataset, int caspoolsize, String p_description, int p_status,
			String software_version,int pipeline_type) throws SQLException {
		int analysisID = -1;
	
		if(analysis_type==null) analysis_type="CancerDetectionPipeline";
		if(analysis_software==null) analysis_software="UIET";
	
		String insertTableSQL = "INSERT INTO NLP_ANALYSIS"
				+ "(  ANALYSIS_TYPE, ANALYSIS_SOFTWARE, "+
				"ANALYSIS_DATASET, ANALYSIS_START_DATE , ANALYSIS_DESCRIPTION, ANALYSIS_STATUS, "+
				"ANALYSIS_SOFTWARE_VERSION, MACHINE, THREAD_COUNT, ANALYSIS_PIPELINE_ID "+
				") VALUES (?,?,?,SYSDATE,?,?,?,?,?,?)  ";
		PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL, new String[]{"ANALYSIS_ID"});
		preparedStatement.setString(1, analysis_type );
		preparedStatement.setString(2, analysis_software );
		preparedStatement.setInt(3, dataset);
		preparedStatement.setString(4, p_description);
		preparedStatement.setInt(5, p_status);
		if(software_version==null) preparedStatement.setString(6, MedicsConstants.DEFAULT_ANALYSIS_VERSION);
		else preparedStatement.setString(6, software_version);
		try {
			preparedStatement.setString(7, InetAddress.getLocalHost().getHostName() );
		} catch (UnknownHostException e) {
			preparedStatement.setString(7, "InetAddress.getLocalHost().getHostName " + e.getMessage());
		}
		preparedStatement.setInt(8, caspoolsize);
		if(pipeline_type!=0) preparedStatement.setInt(9, pipeline_type);
		else preparedStatement.setNull(9,java.sql.Types.NUMERIC);
	
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
	
	
	private static String getQueueNameByLegacyId(String connectionString,
			int legacy_id) {
		String queue="None";
			try (Connection con =  DriverManager.getConnection(connectionString)){
			try (Statement st = con.createStatement()) {
				String query ="SELECT as_queue_name FROM NLP_ANALYSIS_PIPELINE WHERE "+
						" legacy_analysis_type_id="+legacy_id;
				try (ResultSet resultset = (ResultSet) st.executeQuery(query)){
					if ( !resultset.next() ){
						return null;
					}
					queue = new String(resultset.getString(1));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return queue;
	
		
	}


	public static int[] insertClientAnalysis(String medicsurl, int analysis_id_code, 
			String mrn, String comparisonDate, String comparisonDate2,int daybackoffset, 
			String cflo_rec_type, String icda_rec_type, String analysis_description,
			String docset_description,String softname, String softversion,
			int thread_count, int pipeline_type) throws Exception {
	
		Hashtable<String,String> result = null;
		int[] return_info = new int[2];
		try {
			result = insertMedicsAnalysis(medicsurl,analysis_id_code,mrn,comparisonDate,
					comparisonDate2,daybackoffset,cflo_rec_type,icda_rec_type,analysis_description,
					docset_description,softname,softversion,thread_count,
					pipeline_type);
		} catch (Exception e) {
			e.printStackTrace();
			throw(e);
		}
		return_info[0] = Integer.parseInt(result.get("docset_id"));
		return_info[1] = Integer.parseInt(result.get("analysis_id"));
		return return_info;
	}


}
