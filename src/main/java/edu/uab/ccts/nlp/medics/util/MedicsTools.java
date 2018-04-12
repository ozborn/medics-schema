package edu.uab.ccts.nlp.medics.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Clob;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.text.Normalizer;

public class MedicsTools {

	private final Logger logger  = LoggerFactory.getLogger(MedicsTools.class);

	public MedicsTools(){}

	public String calculateMd5(String text) throws AnalysisEngineProcessException {
		String md5sum ="";
		MessageDigest algorithm;
		try {
			algorithm = MessageDigest.getInstance("MD5");
			algorithm.reset();
			algorithm.update(text.getBytes("UTF-8")); //Close enough to Oracle AL32UTF8
			byte[] md5bytes = algorithm.digest();
			BigInteger bigint = new BigInteger(1,md5bytes);
			String md5nolead = bigint.toString(16);
			md5sum = ("00000000000000000000000000000000"+md5nolead).substring(md5nolead.length());
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new AnalysisEngineProcessException(e);
		}
		return md5sum;
	}


	public String fetchAsciDoc(Clob someclob) throws Exception {
		StringBuffer wholedoc = new StringBuffer((int)someclob.length());
		try(InputStream is = someclob.getAsciiStream()){
			Metadata metadata = new Metadata();
			String s = tikaFetchDoc(null, is, metadata);
			wholedoc = new StringBuffer(s);
		} catch(Exception te) {
			logger.error("Tika exception after Tika asci extraction failure");
			te.printStackTrace();
			throw te;
		} 
		return wholedoc.toString();
	}


	/**
	 * Fetches a String in its original coding with
	 * newlines converted to \n from a Clob
	 * @param ptl
	 * @return
	 * @throws Exception
	 */
	public String fetchDoc(Clob someclob) throws Exception {
		String line;
		StringBuffer wholedoc = new StringBuffer((int)someclob.length());
		try (Reader reader = someclob.getCharacterStream()) {
			BufferedReader br = new BufferedReader(reader);
			while((line = br.readLine()) != null) {
				wholedoc.append(line+"\n");
			}
		} catch (Exception e) {
			throw e;
		}
		return wholedoc.toString();
	}


	public String fetchDoc(Blob pdf) throws Exception {
		String s = null;
		try (InputStream is = pdf.getBinaryStream())
		{
			BodyContentHandler bch = new BodyContentHandler();
			ParseContext pcontext = new ParseContext();
			Metadata md = new Metadata();
			PDFParser parser = new PDFParser();
			try {
				// Slows it down by about 10 percent
				parser.getPDFParserConfig().setSortByPosition(true);
			} catch (NoSuchMethodError nsme) {}
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
	public String tikaFetchDoc(Blob blob, InputStream iis, Metadata metaData) throws IOException,SAXException, TikaException , Exception{
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
	public String detagHTML(String broken_html){
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
	 * Want to convert a UTF String to ASCII and strip control characters
	 * 1) Normalize to handle diacritical marks, etc..
	 * 2) Get only the ASCII characters
	 * 3) Remove ASCII control characters 
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public String convertUTFtoASCII(String input) throws Exception {
		String not_ascii = "[^\\x00-\\x7F]";
		String bestfix = input;
		try {
			String s1 = convertUTF16Whitespace2AsciSpace(input);
			String s2 =  Normalizer.normalize(s1,Normalizer.Form.NFKD);
			bestfix = s2.replaceAll(not_ascii, "");
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
	public String convertUTF16Whitespace2AsciSpace(String input) {
		//FIXME whitespace_chars exists only for documentation
		@SuppressWarnings("unused")
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

}
