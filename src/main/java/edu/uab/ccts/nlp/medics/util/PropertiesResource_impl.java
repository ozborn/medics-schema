package edu.uab.ccts.nlp.medics.util;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

import org.apache.uima.resource.DataResource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.SharedResourceObject;

public class PropertiesResource_impl implements PropertiesResource, SharedResourceObject {
  private Properties prop = new Properties();

  /**
   * Expecting to load property files from jar file, URI should be set to
   * something like: edu/uab/ccts/nlp/uima/collection_reader/CareflowColumnMap.properties
   * @see org.apache.uima.resource.SharedResourceObject#load(DataResource)
   */
  public void load(DataResource aData) throws ResourceInitializationException {
    try (InputStream inStr = ClassLoader.getSystemResourceAsStream(aData.getUri().toString())){
      prop.load(inStr);
    } catch (IOException e) {
      throw new ResourceInitializationException(e);
    } 
  }

  /**
 * Get the loaded properties
   */
  public Properties getProperties(){ return prop; }
}
