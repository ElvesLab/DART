/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute 
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software 
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or 
promote products derived from this software without specific prior written 
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is" 
basis, without any representations or warranties of any kind, express or implied 
including, but not limited to, representations or warranties of 
non-infringement, merchantability or fitness for a particular purpose. In no 
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect, 
incidental, special, exemplary, or consequential damages (including, but not 
limited to, procurement of substitute goods or services; loss of use, data, or 
profits; or business interruption) however caused and on any theory of 
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if 
advised of the possibility of such damage.

*******************************************************************************/ 

package rice.p2p.util;

import java.io.*;
import java.util.*;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

/**
 * XMLReader is a utility class used by XMLObjectInputStreamm to perform the actual
 * XML parsing.  This reader is based on the XML Pull-Parsing API, available online
 * at http://www.xmlpull.org.  Any of the provided parser implementations will work 
 * with this reader.
 *
 * @version $Id: XMLReader.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class XMLReader {
  
  /**
   * The actual reader which the parser uses
   */
  protected Reader in;
  
  /**
   * The actual XML parser which we use
   */
  protected XmlPullParser xpp;
  
  /**
   * The cached type of the last event the parser saw
   */
  protected int eventType;
    
  /**
   * Constructor which takes the provided reader and
   * builds a new XML parser to read XML from the reader.
   *
   * @param in The reader to base this XML reader off of
   * @throws IOException If an error occurs
   */
  public XMLReader(Reader in) throws IOException {  
    this.in = new BufferedReader(in);
    
    try {
  /*    XmlPullParserFactory factory = XmlPullParserFactory.newInstance(System.getProperty(XmlPullParserFactory.PROPERTY_NAME), null);
      factory.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true); 
    
      xpp = factory.newPullParser(); */
      xpp = new XMLParser();
      xpp.setInput(this.in);
    } catch (XmlPullParserException e) {
      throw new IOException("XML Exception thrown: " + e);
    } 
  }
  
  /**
   * Method which closes the underlying reader, which will
   * cause future step attempts to throw an IOException.
   *
   * @throws IOException If an error occurs
   */
  public void close() throws IOException {
    in.close(); 
  }
  
  /**
   * Method which writes a sequence of base64 encoded bytes to the output stream
   *
   * @param bytes The bytes to write
   */
  public byte[] readBase64() throws IOException {
    byte[] bytes = new byte[0];
    
    assertStartTag("base64");
    step();
    
    if (isText()) {
      bytes = xpp.getText().getBytes();
      bytes = Base64.decode(bytes, 0, bytes.length);
      step();
    } 
    
    assertEndTag("base64");
    return bytes;
  }
  
  /**
   * Method which reads the header from the reader.  If an
   * invalid header is found, and IOException is thrown.
   *
   * @throws IOException If an error occurs
   */
  public void readHeader() throws IOException {
    assertEvent(xpp.START_DOCUMENT);
    readStartTag("jsx");
  }
  
  /**
   * Asserts that the given event type just happened.
   *
   * @throws IOException If a the assertion failed
   */
  protected void assertEvent(int type) throws IOException {
    if (eventType != type)
      throw new IOException("Expected event " + type + ", got a " + eventType);
  }
  
  /**
   * Asserts that a start tag was just read
   *
   * @throws IOException If a the assertion failed
   */
  public void assertStartTag() throws IOException {
    assertEvent(xpp.START_TAG);
  }
  
  /**
   * Asserts that a end tag was just read
   *
   * @throws IOException If a the assertion failed
   */
  public void assertEndTag() throws IOException {
    assertEvent(xpp.END_TAG);
  }
  
  /**
   * Asserts that the provided start tag was just read
   *
   * @param name The name of the start tag
   * @throws IOException If a the assertion failed
   */
  public void assertStartTag(String name) throws IOException {
    assertStartTag();
    
    if (! xpp.getName().equals(name))
      throw new IOException("Expected start tag '" + name + "', got a '" + xpp.getName()+"'");
  }
  
  /**
   * Asserts that the provided end tag was just read
   *
   * @param name The name of the end tag
   * @throws IOException If a the assertion failed
   */
  public void assertEndTag(String name) throws IOException {
    assertEvent(xpp.END_TAG);
    
    if (! xpp.getName().equals(name))
      throw new IOException("Expected end tag '" + name + "', got a '" + xpp.getName()+"'");
  }
  
  /**
   * Asserts that a start tag will be read next.  Causes a step() 
   * to be called.
   *
   * @throws IOException If a the assertion failed
   */
  public void readStartTag() throws IOException {
    step();
    assertStartTag();
  }
  
  /**
   * Asserts that a end tag will be read next.  Causes a step() 
   * to be called.
   *
   * @throws IOException If a the assertion failed
   */
  public void readEndTag() throws IOException {
    step();
    assertEndTag();    
  }
  
  /**
   * Asserts that the provided start tag will be read next.  Causes a step() 
   * to be called.
   *
   * @throws IOException If a the assertion failed
   */
  public void readStartTag(String name) throws IOException {
    readStartTag();
    assertStartTag(name);
  }
  
  /**
   * Asserts that the provided end tag will be read next.  Causes a step() 
   * to be called.
   *
   * @throws IOException If a the assertion failed
   */
  public void readEndTag(String name) throws IOException {
    readEndTag();
    assertEndTag(name);
  }
  
  /**
   * Asserts that the given attribute exists and is equal to the given value.
   *
   * @throws IOException If a the assertion failed
   */
  public void assertAttribute(String name, String value) throws IOException {
    if (getAttribute(name) == null)
      throw new IOException("Expected attribute " + name + ", found none");
    
    if (! getAttribute(name).equals(value))
      throw new IOException("Expected attribute " + name + " to be '" + value + "', got '" + getAttribute(name)+"'");
  }
  
  /**
   * Advances the parser one step, skipping whitespace
   *
   * @throws IOException If an error occurs
   */
  public void step() throws IOException {
    try { 
      eventType = xpp.next();
      
      if ((eventType == xpp.TEXT) && (xpp.isWhitespace()))
        step();
    } catch (XmlPullParserException e) {
      throw new IOException("XML Exception thrown: " + e);
    }
  }
  
  /**
   * Returns whether or not a start document just happened
   *
   * @return Whether or not a start doucment just happened
   */
  public boolean isStartDocument() {
    return (eventType == xpp.START_DOCUMENT);
  }
  
  /**
   * Returns whether or not a end document just happened
   *
   * @return Whether or not a end doucment just happened
   */
  public boolean isEndDocument() {
    return (eventType == xpp.END_DOCUMENT);
  }
  
  /**
   * Returns whether or not a start tag just happened
   *
   * @return Whether or not a start tag just happened
   */ 
  public boolean isStartTag() {
    return (eventType == xpp.START_TAG);
  }
  
  /**
   * Returns whether or not a end tag just happened
   *
   * @return Whether or not a end tag just happened
   */
  public boolean isEndTag() {
    return (eventType == xpp.END_TAG);
  }
  
  /**
   * Returns whether or not a end tag just happened
   *
   * @return Whether or not a end tag just happened
   */
  public boolean isText() {
    return (eventType == xpp.TEXT);
  }
  
  /**
   * Returns the value of the given attribute, or null if the
   * attribute cannot be found.
   *
   * @return The corresponding value, or null
   * @throws IOException If the current event is not a start tag
   */
  public String getAttribute(String name) throws IOException {
    assertEvent(xpp.START_TAG);
    String result = xpp.getAttributeValue(null, name);
      
    return result;
  }
  
  /**
   * Returns the value of the current start tag
   *
   * @return The start tag value
   * @throws IOException If the current event is not a start tag
   */
  public String getStartTag() throws IOException {
    try {
      assertEvent(xpp.START_TAG);
      return xpp.getName();
    } catch (IOException e) {
      throw new IOException("getStartTag called, caused " + e);
    }
  }
  
  /**
   * Returns the value of the current end tag
   *
   * @return The end tag value
   * @throws IOException If the current event is not a end tag
   */
  public String getEndTag() throws IOException {
    try {
      assertEvent(xpp.END_TAG);
      return xpp.getName();
    } catch (IOException e) {
      throw new IOException("getEndTag called, caused " + e);
    }
  }
  
}
