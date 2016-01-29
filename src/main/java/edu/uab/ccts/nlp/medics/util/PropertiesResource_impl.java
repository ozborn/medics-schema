package edu.uab.ccts.nlp.medics.util;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uima.resource.DataResource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.SharedResourceObject;

public class PropertiesResource_impl implements PropertiesResource, SharedResourceObject {
  private Properties prop = new Properties();
  private static final Logger log  = LoggerFactory.getLogger(PropertiesResource_impl.class);


  /**
   * Expecting to load property files from jar file, URI should be set to
   * something like: edu/uab/ccts/nlp/database/column_map/CareflowColumnMap.properties
   * @see org.apache.uima.resource.SharedResourceObject#load(DataResource)
   */
  public void load(DataResource aData) throws ResourceInitializationException {
    log.info("URI was:"+aData.getUri());
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
