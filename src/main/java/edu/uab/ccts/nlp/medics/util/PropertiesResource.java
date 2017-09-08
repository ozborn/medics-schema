package edu.uab.ccts.nlp.medics.util;

import java.util.Properties;
import org.apache.uima.resource.SharedResourceObject;

public interface PropertiesResource extends org.apache.uima.resource.SharedResourceObject {
	Properties getProperties();
	void setProperties(Properties p);
}
