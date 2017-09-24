package edu.uab.ii.nlp.medics.test.unit;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import edu.uab.ccts.nlp.medics.util.LegacyMedicsTools;

public class TestDateConversion {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetDaysAgoDateString() {
		try {
			System.out.println("Todays's date:"+
		    LegacyMedicsTools.getTodaysDate().toString());
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testConvertString2SqlDate() {
		try {
			System.out.println(LegacyMedicsTools.convertString2SqlDate("03-APR-2015"));
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

}
