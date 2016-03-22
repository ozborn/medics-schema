package edu.uab.ccts.nlp.medics.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;

public class MedicsTools {
	
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

}
