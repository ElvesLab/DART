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
import java.lang.reflect.*;
import java.security.*;
import java.util.*;
import sun.reflect.*;

/**
 * XMLObjectInputStreamm is an extension of ObjectInputStreamm which provides
 * for deserialization for objects which have been converted to XML via a
 * XMLObjectOutputStream.  This class supports all of the features of 
 * the ObjectInputStreamm, including serialization support for any Java 
 * object graph implementing the Serializable interface, support for 
 * Externalizable classes, custom deserialization via the readObject() method, 
 * class evolution via the readObjectNoData() method, alternate field
 * reading mechanisms via the readFields() method, support for the 
 * readUnshared method, and dynamic object replacement via the readResolve() 
 * method.
 *
 * The format of the XML data to be read in should conform to the JSX XML 
 * Schema, available online at http://www.jsx.org/jsx.xsd.  This class is 
 * designed to be able to read objects serialized using JSX, however, this
 * has not been fully tested and bugs may be encountered.
 *
 * @version $Id: XMLObjectInputStream.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class XMLObjectInputStream extends ObjectInputStream {
  
  /**
   * The hashmap of readResolve methods, mapping Class->Method
   */
  protected static SoftHashMap READ_RESOLVES = new SoftHashMap();
  
  /**
   * The hashmap of readObject methods, mapping Class->Method
   */
  protected static SoftHashMap READ_OBJECTS = new SoftHashMap();
  
  /**
   * A cache of constructors, mapping classes to serialization constructors
   */
  protected static SoftHashMap CONSTRUCTORS = new SoftHashMap();
  
  /**
   * The underlying reader, which parses the XML
   */
  protected XMLReader reader;
  
  /**
   * The hashtable of references, which is updated each time a new object is 
   * read off of the stream.
   */
  protected Hashtable references;
   
  /**
   * The stack of objects which are currently being read off of the stream
   */
  protected Stack currentObjects;
  
  /**
   * The stack of class types which are being read off of the stream
   */
  protected Stack currentClasses;
  
  /**
   * The ReflectionFactory, which allows for prividged construction of
   * objects
   */
  protected ReflectionFactory reflFactory = (ReflectionFactory) AccessController.doPrivileged(new sun.reflect.ReflectionFactory.GetReflectionFactoryAction());

  /**
   * The list of validation objects waiting for the entire object graph to be read in
   */
  protected ValidationList vlist;
  
  /**
   * The depth at which we are currently at in the object tree
   */
  protected int depth;
  
  /**
   * Constructor which reads data from the given input stream in order
   * deserialize objects.  This constructor also reads the header from the 
   * stream, and throws an IOException if the correct header is not seen.
   *
   * @param in The input stream to read data from
   * @throws IOException If the stream header is corrupt
   */
  public XMLObjectInputStream(InputStream in) throws IOException {
    super();
    
    this.reader = new XMLReader(new InputStreamReader(in));
    this.currentObjects = new Stack();
    this.currentClasses = new Stack();
    this.references = new Hashtable();
    this.vlist = new ValidationList();
    this.depth = 0;
        
    readStreamHeader();
  }
  
  
  // ----- ObjectInputStreamm Overriding Methods -----
  
  /**
   * Method which reads the XML header off of the stream.  Usually, this is the
   * <?xml version="1.0"?> tag, but it may include processing instructions.
   *
   * @throws IOException If the stream header is corrupt
   */
  protected void readStreamHeader() throws IOException {
    reader.readHeader();
  }
  
  /**
   * Method which closes the underlying input stream for reading.  Any subsequent 
   * reads will throw an IOException.
   *
   * @throws IOException If an error occurs
   */
  public void close() throws IOException {
    reader.close();
  } 
  
  /**
   * Method which resets the input stream, which removes the binding of all previously
   * stored references.
   *
   * @throws IOException If an error occurs
   */
  public void reset() throws IOException {
    references = new Hashtable(); 
    vlist.clear();
  }
  
  /**
   * Method which reads a byte from the underlying output stream.  Simply calls
   * readByte()
   *
   * @throws IOException If an error occurs
   */
  public int read() throws IOException {
    return readByte();
  }
  
  /**
   * Method which reads a array of bytes from the underlying output stream.  Simply calls
   * b[x] = readByte() on each of the array elements.
   *
   * @throws IOException If an error occurs
   */
  public int read(byte[] b, int offset, int length) throws IOException {
    reader.readStartTag("base64");
    byte[] bytes = reader.readBase64();
    
    int written = (length < bytes.length ? length : bytes.length);
    System.arraycopy(bytes, 0, b, offset, written);
    
    return written;
  }
  
  /**
   * Method which reads a byte from the underlying output stream.  Simply calls
   * read()
   *
   * @throws IOException If an error occurs
   */
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }
  
  /**
   * Method which reads a byte from the underlying output stream.  Simply calls
   * read()
   *
   * @throws IOException If an error occurs
   */
  public void readFully(byte[] b, int offset, int length) throws IOException {
    read(b, offset, length);
  }
  
  /**
   * Method which reads an unsigned byte from the underlying output stream.  Simply calls
   * readByte()
   *
   * @throws IOException If an error occurs
   */
  public int readUnsignedByte() throws IOException {
    return readByte();
  }
  
  /**
   * Method which reads an unsigned short from the underlying output stream.  Simply calls
   * readShort()
   *
   * @throws IOException If an error occurs
   */
  public int readUnsignedShort() throws IOException {
    return readShort();
  }
  
  /**
   * Method which reads an int from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public int readInt() throws IOException {
    reader.readStartTag("primitive");
    return readIntHelper();
  }
  
  /**
   * Method which reads a boolean from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public boolean readBoolean() throws IOException {
    reader.readStartTag("primitive");
    return readBooleanHelper();
  }
  
  /**
   * Method which reads a byte from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */  
  public byte readByte() throws IOException {
    reader.readStartTag("primitive");
    return readByteHelper();
  }
  
  /**
   * Method which reads a char from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public char readChar() throws IOException {
    reader.readStartTag("primitive");
    return readCharHelper();
  }
  
  /**
   * Method which reads a double from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public double readDouble() throws IOException {
    reader.readStartTag("primitive");
    return readDoubleHelper();
  }
  
  /**
   * Method which reads a float from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public float readFloat() throws IOException {
    reader.readStartTag("primitive");
    return readFloatHelper();
  }
  
  /**
   * Method which reads a long from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public long readLong() throws IOException {
    reader.readStartTag("primitive");
    return readLongHelper();
  }
  
  /**
   * Method which reads a short from the stream and returns the result.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public short readShort() throws IOException {
    reader.readStartTag("primitive");
    return readShortHelper();
  }
  
  /**
   * Method which reads a UTF-encoded String from the stream and returns the
   * result.  This method simply calls readObject(), as all strings are
   * UTF encoded in XML.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   */
  public String readUTF() throws IOException {
    try {
      return (String) readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("ReadUTF caused " + e);
    }
  }
  
  /**
   * Method which is called by ObjectInputStreamm.readObject(), and reads
   * the next object from the stream, and returns the result.  If the object
   * (or any of it's descendents) has a readResolve() method, that object is
   * returned.
   *
   * @return The value from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If a class in the stream cannot be found
   */
  protected Object readObjectOverride() throws IOException, ClassNotFoundException {
    reader.readStartTag();
    
    Object result = readObjectHelper();
    
    if (depth == 0) {
      vlist.doCallbacks();
    }
    
    return result;
  }
  
  /**
   * Method which reads the next object from the stream and does not record a 
   * reference to the object.  This guarantees that in future references to this
   * object in the stream will throw an IOException.
   *
   * @return The next object from the stream, without recording a reference
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If a class in the stream cannot be found
   */
  public Object readUnshared() throws IOException, ClassNotFoundException {
    reader.readStartTag();
    
    Object result = readUnsharedHelper(false);
    
    if (depth == 0) {
      vlist.doCallbacks();
    }
    
    return result;
  }
  
  /**
   * Method which can be called by objects if they have a readObject() method.  
   * This method initiates the default field deserialization mechanism, and
   * sets all of the object's fields from the stream.  If this method is called,
   * the readFields() method CANNOT be called by the same object.  However, one
   * of these two methods MUST be called in the context of a readObject().
   *
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If a class in the stream cannot be found
   * @throws NotActiveException If a object is not currently being read
   */
  public void defaultReadObject() throws IOException, ClassNotFoundException {
    if (currentObjects.peek() != null)
      readFields(currentObjects.peek(), (Class) currentClasses.peek());
    else
      throw new NotActiveException("defaultReadObject called with empty stack!");
  }
  
  /**
   * Method which can be called by objects if they have a readObject() method.  
   * This method initiates the default field deserialization mechanism, and reads
   * all of the default fields in the stream.  It does NOT set the field values of
   * the object, however, but instead returns the result to the Object as a GetField
   * object.  It is explicitly the responsibility of the object to set its fields
   * appropriately.  If this method is called, the defaultReadObject() method CANNOT 
   * be called by the same object.  However, one of these two methods MUST be called
   * in the context of a readObject().
   *
   * @return A GetField, representing all of this object's fields
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If a class in the stream cannot be found
   * @throws NotActiveException If a object is not currently being read
   */
  public ObjectInputStream.GetField readFields() throws IOException, ClassNotFoundException {
    if (currentObjects.peek() != null)
      return readGetFields();
    else
      throw new NotActiveException("readFields called with empty stack!");
  }
  
  /**
   * Register an object to be validated before the graph is returned.  While
   * similar to resolveObject these validations are called after the entire
   * graph has been reconstituted.  Typically, a readObject method will
   * register the object with the stream so that when all of the objects are
   * restored a final set of validations can be performed.
   *
   * @param  obj the object to receive the validation callback.
   * @param  prio controls the order of callbacks
   * @throws  NotActiveException The stream is not currently reading objects
   *     so it is invalid to register a callback.
   * @throws  InvalidObjectException The validation object is null.
   */
  public void registerValidation(ObjectInputValidation obj, int prio) throws NotActiveException, InvalidObjectException {
    if (currentObjects.peek() == null) 
      throw new NotActiveException("registerValidation called with empty stack!");
    
    vlist.register(obj, prio);
  }
  
  // ----- Internal helper methods -----
 
  
  /**
   * This method returns the readResolve() method of a given class, if such a method 
   * exists.  This method searches the class's heirarchy for a readResolve() method
   * which is assessible by the given class.  If no such method is found, null is returned.
   *
   * @param cl The class to find the readResolve() of
   * @return The method, or null if none was found
   */
  private static Method getReadResolve(Class cl) {
    synchronized (READ_RESOLVES) {
      if (READ_RESOLVES.containsKey(cl)) 
        return (Method) READ_RESOLVES.get(cl);
            
      Method meth = null;
      Class defCl = cl;
      while (defCl != null) {
        try {
          meth = defCl.getDeclaredMethod("readResolve", new Class[0]);
          break;
        } catch (NoSuchMethodException ex) {
          defCl = defCl.getSuperclass();
        }
      }
      
      if (meth == null) {
        READ_RESOLVES.put(cl, null);
        return null;
      }
      
      meth.setAccessible(true);
      int mods = meth.getModifiers();
      if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0) {
        READ_RESOLVES.put(cl, null);
        return null;
      } else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0) {
        READ_RESOLVES.put(cl, meth);
        return meth;
      } else if ((mods & Modifier.PRIVATE) != 0) {
        if (cl == defCl) {
          READ_RESOLVES.put(cl, meth);
          return meth;
        } else {
          READ_RESOLVES.put(cl, null);
          return null;
        }
      } else {
        READ_RESOLVES.put(cl, meth);
        return meth;
      }
    }
  }
  
  /**
   * This method returns the readResolve() method of a given class, if such a method 
   * exists.  This method searches the class's heirarchy for a readResolve() method
   * which is assessible by the given class.  If no such method is found, null is returned.
   *
   * @param cl The class to find the readResolve() of
   * @return The method, or null if none was found
   */
  private static Method getReadObject(Class cl) {
    synchronized (READ_OBJECTS) {
      if (READ_OBJECTS.containsKey(cl)) 
        return (Method) READ_OBJECTS.get(cl);
      
      try {
        Method method = cl.getDeclaredMethod("readObject", new Class[] {ObjectInputStream.class});
        method.setAccessible(true);
      
        READ_OBJECTS.put(cl, method);
        return method;
      } catch (NoSuchMethodException e) {
        READ_OBJECTS.put(cl, null);
        return null;
      }
    }
  }
  
  /**
   * Method which returns the Serializable constructor for the provided class.  This
   * Constructor is the no-arg Constructor for the first non-Serializable class
   * in the class's superclass heirarchy.  This method does not cache the result, but
   * does set the constructor to be accessible, regardless of the Java security
   * protection.
   *
   * @param c The class to fetch the constructor for
   * @throws IOException If an error occurs
   * @throws NoSuchMethodException If the first non-Serializable class does not have a no-arg 
   *   Constructor
   */
  protected Constructor getSerializableConstructor(Class c) throws IOException, NoSuchMethodException {
    Class initCl = c;
    
    while (Serializable.class.isAssignableFrom(initCl))
      initCl = initCl.getSuperclass();

    Constructor cons = initCl.getDeclaredConstructor(new Class[0]);
    cons = reflFactory.newConstructorForSerialization(c, cons);
    cons.setAccessible(true);
    return cons;
  }

  /**
   * Method which returns a new instance of the provided class.  It does this by 
   * getting the class's serializable constructor (described in getSerializableConstructor())
   * and using it to create a new object.  Thus, all of the Serializable superclasses
   * of the object must be initialized from the stream.
   *
   * @param c The class to create a new instance of
   * @throws IOException If an error occurs
   */
  protected Object newInstance(Class c) throws IOException {
    try {
      Constructor cons = (Constructor) CONSTRUCTORS.get(c);
    
      if (cons == null) {
        if (Externalizable.class.isAssignableFrom(c)) {
          cons = c.getDeclaredConstructor(new Class[0]);
        } else {
          cons = getSerializableConstructor(c);
        }
          
        cons.setAccessible(true);
        CONSTRUCTORS.put(c, cons);
      }

      return cons.newInstance(new Object[0]);
    } catch (InstantiationException e) {
      throw new IOException("Could not instanciate new class " + c);
    } catch (IllegalAccessException e) {
      throw new IOException("Could not instanciate new class " + c);
    } catch (InvocationTargetException e) {
      throw new IOException("Could not instanciate new class " + c);
    } catch (NoSuchMethodException e) {
      throw new IOException("Could not instanciate new class " + c);
    }
  }
  
  /**
   * Method which returns the class object for class names written to the
   * stream.  If the name represents a primitive, the the X.TYPE class is
   * returned, otherwise, Class.forName() is used.
   *
   * @param name The name of the class to return
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected Class getClass(String name) throws ClassNotFoundException {
    if (name.equals("int"))
      return Integer.TYPE;
    else if (name.equals("boolean"))
      return Boolean.TYPE;
    else if (name.equals("byte"))
      return Byte.TYPE;
    else if (name.equals("char"))
      return Character.TYPE;
    else if (name.equals("double"))
      return Double.TYPE;
    else if (name.equals("float"))
      return Float.TYPE;
    else if (name.equals("long"))
      return Long.TYPE;
    else if (name.equals("short"))
      return Short.TYPE;
    else
      return Class.forName(name);
  }
  
  
  // ----- ObjectInputStreamm Referencing Methods -----
  
  /**
   * Method which adds a reference in the hashtable of references.
   * Multiple calls to this method will replace prior objects.
   *
   * @param reference The reference name to use
   * @param o The object to reference
   */
  protected void putReference(String reference, Object o) {
    references.put(reference, o);
  }
  
  /**
   * Method which returns a previously stored reference.  If the
   * reference cannot be found, null is returned.
   *
   * @param reference The reference to use
   * @return The referenced object, or null if none can be found
   */
  protected Object getReference(String reference) {
    return references.get(reference);
  }
  
  // ----- Internal readX methods -----

  /**
   * Method which reads a primitive value from the stream and 
   * returns the String representation to the callee for processing.
   * This method assumes that the XML reader is currently on a 
   * primitive tag, and does consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The String representation of the primitive
   * @throws IOException If an error occurs
   */
  protected String readPrimitive(String type) throws IOException {
    reader.assertStartTag("primitive");
    reader.assertAttribute("type", type);
    String value = reader.getAttribute("value");
    reader.readEndTag("primitive");
    
    return value;
  }
  
  /**
   * Method which reads an int from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The int read from the stream
   * @throws IOException If an error occurs
   */
  protected int readIntHelper() throws IOException {
    return Integer.parseInt(readPrimitive("int"));
  }
  
  /**
   * Method which reads an boolean from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The boolean read from the stream
   * @throws IOException If an error occurs
   */
  protected boolean readBooleanHelper() throws IOException {
    return readPrimitive("boolean").equals("true");
  }
  
  /**
   * Method which reads an byte from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The byte read from the stream
   * @throws IOException If an error occurs
   */
  protected byte readByteHelper() throws IOException {
    return Byte.parseByte(readPrimitive("byte"));
  }
  
  /**
   * Method which reads an char from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The char read from the stream
   * @throws IOException If an error occurs
   */
  protected char readCharHelper() throws IOException {
    return readPrimitive("char").charAt(0);
  }
  
  /**
   * Method which reads an double from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The double read from the stream
   * @throws IOException If an error occurs
   */
  protected double readDoubleHelper() throws IOException {
    return Double.parseDouble(readPrimitive("double"));
  }
  
  /**
   * Method which reads an float from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The float read from the stream
   * @throws IOException If an error occurs
   */
  protected float readFloatHelper() throws IOException {
    return Float.parseFloat(readPrimitive("float"));
  }
  
  /**
   * Method which reads an long from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The long read from the stream
   * @throws IOException If an error occurs
   */
  protected long readLongHelper() throws IOException {
    return Long.parseLong(readPrimitive("long"));
  }
  
  /**
   * Method which reads an short from the stream.  This method assumes 
   * that the XML reader is currently on a primitive tag, and does 
   * consume the end primitive tag.
   *
   * @param type The type of primitive which should be next
   * @return The short read from the stream
   * @throws IOException If an error occurs
   */
  protected short readShortHelper() throws IOException {
    return Short.parseShort(readPrimitive("short"));
  }  
    
  /**
   * Method which reads an object from the stream.  This method assumes 
   * that the XML reader is currently on a start tag, and does 
   * consume the corresponding end tag.  If the next object represents a
   * reference or null, then the appropriate helper is called.  Otherwise, 
   * readObjectUnshared() is called to process the object.
   *
   * @return The object read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected Object readObjectHelper() throws IOException, ClassNotFoundException {
    reader.assertStartTag();
    depth++;
    Object result = null;
    
    if (reader.getStartTag().equals("reference")) {
      result = readReference();
    } else if (reader.getStartTag().equals("null")) {
      result = readNull();
    } else {
      result = readUnsharedHelper(true); 
    } 
    
    depth--;
    return result;
  }
  
  /**
   * Method which reads an object from the stream.  This method assumes 
   * that the XML reader is currently on a start tag, and does 
   * consume the corresponding end tag.  This method also assumes that
   * the type of object which is being read is *sharable*, even if it is
   * not going to be shared.  Thus, this method can read objects of type
   * String, Array, or Serializable.
   *
   * @param shared Whether or not to record a reference to this object
   * @return The object read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected Object readUnsharedHelper(boolean shared) throws IOException, ClassNotFoundException {
    reader.assertStartTag();
    
    if (reader.getStartTag().equals("string")) {
      return readString(shared);
    } else if (reader.getStartTag().equals("array")) {
      return readArray(shared);
    } else if (reader.getStartTag().equals("object")) {
      return readOrdinaryObject(shared);
    } else {
      throw new IOException("Unknown element name " + reader.getStartTag());
    }
  }  
  
  /**
   * Method which reads a reference off of the stream, and looks the reference up
   * in the references table.  If not corresponding object is found, an IOException
   * is thrown.  This method expected that a reference tag has just been read, and
   * it does consume the end reference tag.
   *
   * @throws IOException If an error occurs or if a reference is not found
   * @throws ClassNotFoundException If the class cannot be found
   */  
  protected Object readReference() throws IOException, ClassNotFoundException {
    reader.assertStartTag("reference");
    
    Object result = getReference(reader.getAttribute("idref"));
    
    if (result == null)
      throw new IOException("Invalid reference " + reader.getAttribute("idref") + " found.");
    
    reader.readEndTag("reference");
    
    return result;
  }
  
  /**
   * Method which reads a null item off of the stream. This method expected that a
   * null tag has just been read, and it does consume the end null tag.
   *
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */  
  protected Object readNull() throws IOException, ClassNotFoundException {
    reader.assertStartTag("null");
    reader.readEndTag("null");
    
    return null;
  }
  
  /**
   * Method which reads a string item off of the stream. This method expects that
   * a start string tag has just been read, an it does consume the end start tag. 
   * If this string is to be shared, a reference is added to the references tag.
   *
   * @param shared Whether or not to add this string to the references table
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */  
  protected Object readString(boolean shared) throws IOException, ClassNotFoundException {
    reader.assertStartTag("string");
    
    String result = new String(reader.getAttribute("value"));
    
    if (shared && (reader.getAttribute("id") != null)) 
      putReference(reader.getAttribute("id"), result);
    
    reader.readEndTag("string");
    
    return result;
  }
  
  /**
   * Method which reads an ordinary object from the stream (not a String or Array).
   * This method assumes that the XML reader is currently on a start tag, and does 
   * consume the corresponding end tag.  This method first reads the class type, 
   * and if it is Serializable, it constructs a new instance.  Then, if the object
   * is shared and the id field is not null, it records a reference to the object.
   * If the object is Externalizable, the readExternal() method is called and the
   * object is returned.  Otherwise, each of the superclasses are read, from highest
   * to lowest, using the readClass() method.  Lastly, if the object defines a
   * readResolve() method, it is called and the result is recorded and returned.
   *
   * @param shared Whether or not to record a reference to this object
   * @return The object read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   * @throws NotSerializableException If the class to be deserialized is not Serializable
   */
  protected Object readOrdinaryObject(boolean shared) throws IOException, ClassNotFoundException {
    reader.assertStartTag("object");
    
    Class c = Class.forName(reader.getAttribute("class"));
    String id = reader.getAttribute("id");

    if (! Serializable.class.isAssignableFrom(c))
      throw new NotSerializableException(c.getName());
        
    Object o = newInstance(c);

    if (shared && (id != null)) 
      putReference(id, o);
      
    if (Externalizable.class.isAssignableFrom(c)) {
      ((Externalizable) o).readExternal(this);
      reader.step();
    } else {
      reader.step();
    
      while (! reader.isEndTag()) {
        readClass(o);
        reader.step();
      }
    }
    
    reader.assertEndTag("object");
    
    Method method = getReadResolve(c);
      
    try {
      if (method != null) {
        o = method.invoke(o, new Object[0]);
      
        if (shared && (id != null)) 
          putReference(id, o);
      }
    } catch (IllegalAccessException e) {
      throw new IOException("ReadResolve caused " + e);
    } catch (InvocationTargetException e) {
      throw new IOException("ReadResolve caused " + e);
    }
    
    return o;
  } 
  
  /**
   * Method which reads the information for one class for a given object from the 
   * stream. This method assumes that the XML reader is currently on a start declaredClass tag, 
   * and does consume the corresponding end declaredClass tag.  This method first reads the class type, 
   * and if it defines a readObject() method, it is called.  Otherwise, the fields are
   * read in a default manner.  Any extra data not read in the stream is read and 
   * ignored.
   *
   * @param o The object we are reading the the class for
   * @return The object read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected Object readClass(Object o) throws IOException, ClassNotFoundException {
    reader.assertStartTag("declaredClass");

    Class c = Class.forName(reader.getAttribute("class"));
    
    Method method = getReadObject(c);
    
    if (method != null) {
      try {
        currentObjects.push(o);
        currentClasses.push(c);
        method.invoke(o, new Object[] {this});
        currentObjects.pop();
        currentClasses.pop();
        readUnreadOptionalData();
      } catch (InvocationTargetException e) {
        System.out.println(e.getTargetException().getMessage());
        e.getTargetException().printStackTrace();
        throw new IOException("InvocationTargetException thrown! " + e.getTargetException());
      } catch (IllegalAccessException e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
        throw new IOException("IllegalAccessException thrown! " + e);
      }
    } else {
      readFields(o, c);
      readUnreadOptionalData();
    } 
    
    reader.assertEndTag("declaredClass");
    
    return o;
  }
  
  /**
   * Method which reads any extra data from the stream which was not read by the object.  
   * This method simply loops until it hits the appropriate end declaredClass tag, and 
   * then returns.  Thus, any data read by this method is ignored.  However, the objects
   * here are deserialized as the stream may reference them in the future.
   *
   * @param o The object we are reading the the class for
   * @return The object read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected void readUnreadOptionalData() throws IOException, ClassNotFoundException {
    reader.step();
    
    while (! reader.isEndTag()) {
      if (reader.getStartTag().equals("primitive")) {
        reader.readEndTag("primitive");
      } else if (reader.getStartTag().equals("base64")) {
        reader.readEndTag("base64");
      } else {
        readObjectHelper();
      }

      reader.step();
    }
  }
  
  /**
   * Method which reads all of the field data from the stream, as readFields() does, 
   * but instead of assigning the fields to the object, it returns them as a GetField
   * object.  In this manner, the object itself can perform field inititalization.  
   *
   * @return The fields read from the stream
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected GetField readGetFields() throws IOException, ClassNotFoundException {
    reader.readStartTag("default");
    
    GetField g = new GetField();
    reader.step();
    
    while (! reader.isEndTag()) {
      readGetField(g);
      reader.step();
    }
    
    reader.assertEndTag("default"); 
    
    return g;
  }
  
  /**
   * Method which read a single field from the stream and places it in the provided
   * GetField object.  This method assumes that the XML reader is on a start tag, and
   * that the item read has a field attribute.
   *
   * @param g The GetField object into which the field should be put
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected void readGetField(GetField g) throws IOException, ClassNotFoundException {
    reader.assertStartTag();
    
    String name = reader.getAttribute("field");
    
    if (name == null)
      throw new IOException("Could not read field " + reader.getStartTag() + ", as field attribute was null!");
    
    if (reader.getStartTag().equals("primitive")) {
      readPrimitiveGetField(g);
    } else {
      g.put(name, readObjectHelper());
    } 
  }
  
  /**
   * Method which reads a primitive field from the stream, and places it in the 
   * provided GetField object.  This method assumes that the XML parser has just
   * read a primitive start tag, and that the tag has a field attribute.  This method
   * does consume that end primitive tag.
   *
   * @param g The GetField object to put the primitive in
   * @throws IOException If an error occurs
   */
  protected void readPrimitiveGetField(GetField g) throws IOException, ClassNotFoundException {
    reader.assertStartTag("primitive");
    String name = reader.getAttribute("field");
    
    if (name == null)
      throw new IOException("Could not read primitive field " + reader.getAttribute("type") + ", as field attribute was null!");
    
    Class c = getClass(reader.getAttribute("type"));
    
    if (c.equals(Integer.TYPE)) {
      g.put(name, readIntHelper());
    } else if (c.equals(Boolean.TYPE)) {
      g.put(name, readBooleanHelper());
    } else if (c.equals(Byte.TYPE)) {
      g.put(name, readByteHelper());
    } else if (c.equals(Character.TYPE)) {
      g.put(name, readCharHelper());
    } else if (c.equals(Double.TYPE)) {
      g.put(name, readDoubleHelper());
    } else if (c.equals(Float.TYPE)) {
      g.put(name, readFloatHelper());
    } else if (c.equals(Long.TYPE)) {
      g.put(name, readLongHelper());
    } else if (c.equals(Short.TYPE)) {
      g.put(name, readShortHelper());
    } else {
      throw new IllegalArgumentException("Field " + name + " is not primitive!");
    }
  }
  
  /**
   * Method which initiates the default field reading mechanism for the given
   * object's class.  This method reads the start default tag, all of the fields,
   * and then reads the end default tag.  It reads all of the fields using the 
   * readField() method.
   *
   * @param o The object which is currently being read in
   * @param c The class to read the fields for
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected void readFields(Object o, Class c) throws IOException, ClassNotFoundException {
    reader.readStartTag("default");
    reader.step();
    
    while (! reader.isEndTag()) {
      readField(o, c);
      reader.step();
    }
    
    reader.assertEndTag("default"); 
  }
  
  /**
   * Method which reads a single field from the stream, and assignes it to
   * the provided object.  If the field cannot be found in the given class, 
   * it is deserialized and then ignored, in case it is referenced later in 
   * the stream.
   *
   * @param o The object which is currently being read in
   * @param c The class to read the fields for
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected void readField(Object o, Class c) throws IOException, ClassNotFoundException {
    reader.assertStartTag();
    String field = reader.getAttribute("field");
        
    try {
      Field f = c.getDeclaredField(reader.getAttribute("field"));
      f.setAccessible(true);

      int mask = Modifier.STATIC;
      if ((f.getModifiers() & mask) != 0)
        throw new NoSuchFieldException("Field read was static!");

      if (reader.getStartTag().equals("primitive")) {
        readPrimitiveField(o, f);
      } else {
        f.set(o, readObjectHelper());
      }
    } catch (NoSuchFieldException e) {
      if (reader.getStartTag().equals("primitive")) {
        reader.readEndTag("primitive");
      } else {
        readObjectHelper();
      }    
    } catch (IllegalAccessException e) {
      // skip IAE, which is very likely caused by setting a final field on < JVM 1.5
    } catch (IllegalArgumentException e) {
      System.err.println("COULD NOT SET FIELD " + field + " OF " + o.getClass().getName() + " - SKIPPING.  THIS SHOULD NOT HAPPEN!" + e);
      e.printStackTrace();
    }
  }
  
  /**
   * Method which reads a primitive field from the stream, and places it in the 
   * given object.  This method assumes that the XML parser has just read a primitive 
   * start tag, and that the tag has a field attribute.  This method does consume 
   * that end primitive tag.
   *
   * @param o The object to put the primitive in
   * @param f The field representing the primitive about to be read
   * @throws IOException If an error occurs
   */  
  protected void readPrimitiveField(Object o, Field f) throws IOException, IllegalAccessException {
    reader.assertStartTag("primitive");
    
    if (f.getType().equals(Integer.TYPE)) {
      f.setInt(o, readIntHelper());
    } else if (f.getType().equals(Boolean.TYPE)) {
      f.setBoolean(o, readBooleanHelper());
    } else if (f.getType().equals(Byte.TYPE)) {
      f.setByte(o, readByteHelper());
    } else if (f.getType().equals(Character.TYPE)) {
      f.setChar(o, readCharHelper());
    } else if (f.getType().equals(Double.TYPE)) {
      f.setDouble(o, readDoubleHelper());
    } else if (f.getType().equals(Float.TYPE)) {
      f.setFloat(o, readFloatHelper());
    } else if (f.getType().equals(Long.TYPE)) {
      f.setLong(o, readLongHelper());
    } else if (f.getType().equals(Short.TYPE)) {
      f.setInt(o, readShortHelper());
    } else {
      throw new IllegalArgumentException("Field " + f + " is not primitive!");
    }
  }
  
  /**
   * Method which reads an array off of the stream.  This method expects that a 
   * start array tag has just been read, and does consume the end array tag.  This
   * method constructs an array of the provided base type, and then recursively
   * reads in all of the objects for the array.
   *  
   * @param shared Whether or not to add this array to the references table
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */  
  protected Object readArray(boolean shared) throws IOException, ClassNotFoundException {
    reader.assertStartTag("array");
    
    Class c = getClass(reader.getAttribute("base"));
    
    int length = Integer.valueOf(reader.getAttribute("length")).intValue();
    int dim = Integer.valueOf(reader.getAttribute("dim")).intValue();
    Object result = Array.newInstance(c, length);
    
    if (shared && (reader.getAttribute("id") != null)) 
      putReference(reader.getAttribute("id"), result);
    
    for (int i=0; i<length; i++) {
      if (c.equals(Integer.TYPE))
        Array.setInt(result, i, readInt());
      else if (c.equals(Boolean.TYPE))
        Array.setBoolean(result, i, readBoolean());
      else if (c.equals(Byte.TYPE))
        Array.setByte(result, i, readByte());
      else if (c.equals(Character.TYPE))
        Array.setChar(result, i, readChar());
      else if (c.equals(Double.TYPE))
        Array.setDouble(result, i, readDouble());
      else if (c.equals(Float.TYPE))
        Array.setFloat(result, i, readFloat());
      else if (c.equals(Long.TYPE))
        Array.setLong(result, i, readLong());
      else if (c.equals(Short.TYPE))
        Array.setShort(result, i, readShort());
      else 
        Array.set(result, i, readObject());
    }
    
    reader.readEndTag("array");
    
    return result;
  }

  
  // ----- Implementation of GetField -----
  
  /**
   * This class is an implementation of GetField which is compatible with 
   * the XMLObjectInputStreamm.  It works in the same manner as the 
   * ObjectInputStreamm.GetField.
   */
  public class GetField extends ObjectInputStream.GetField {
    
    protected HashMap primitives = new HashMap();
    
    protected HashMap objects = new HashMap();
    
    public boolean defaulted(String name) {
      return (! (primitives.containsKey(name) || objects.containsKey(name)));
    }
    
    public boolean get(String name, boolean value) {
      if (primitives.get(name) == null)
        return value;
          
      return ((Boolean) primitives.get(name)).booleanValue();
    }
    
    public byte get(String name, byte value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Byte) primitives.get(name)).byteValue();
    }
    
    public char get(String name, char value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Character) primitives.get(name)).charValue();
    }
    
    public double get(String name, double value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Double) primitives.get(name)).doubleValue();
    }
    
    public float get(String name, float value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Float) primitives.get(name)).floatValue();
    }
    
    public int get(String name, int value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Integer) primitives.get(name)).intValue();
    }
    
    public long get(String name, long value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Long) primitives.get(name)).longValue();
    }
    
    public short get(String name, short value) {
      if (primitives.get(name) == null)
        return value;
      
      return ((Short) primitives.get(name)).shortValue();
    }
    
    public Object get(String name, Object value) {
      if (objects.get(name) == null)
        return value;
      
      return objects.get(name);
    }
    
    protected void put(String name, boolean value) {
      primitives.put(name, new Boolean(value));
    }
    
    protected void put(String name, byte value) {
      primitives.put(name, new Byte(value));
    }
    
    protected void put(String name, char value) {
      primitives.put(name, new Character(value));
    }
    
    protected void put(String name, double value) {
      primitives.put(name, new Double(value));
    }
    
    protected void put(String name, float value) {
      primitives.put(name, new Float(value));
    }
    
    protected void put(String name, int value) {
      primitives.put(name, new Integer(value));
    }
    
    protected void put(String name, long value) {
      primitives.put(name, new Long(value));
    }
    
    protected void put(String name, short value) {
      primitives.put(name, new Short(value));
    }
    
    protected void put(String name, Object value) {
      objects.put(name, value);
    }
    
    public ObjectStreamClass getObjectStreamClass() {
      throw new UnsupportedOperationException("CANNOT GET THE OBJECT STREAM CLASS!");
    }
  }
  
  // ----- VALIDATION LIST IMPLEMENTATION ----
  
  /**
   * Prioritized list of callbacks to be performed once object graph has been
   * completely deserialized.
   */
  private static class ValidationList {
    
    private static class Callback {
      final ObjectInputValidation obj;
      final int priority;
      Callback next;
      
      Callback(ObjectInputValidation obj, int priority, Callback next) {
        this.obj = obj;
        this.priority = priority;
        this.next = next;
      }
    }
    
    /** linked list of callbacks */
    private Callback list;
    
    /**
     * Creates new (empty) ValidationList.
     */
    ValidationList() {
    }
    
    /**
     * Registers callback.  Throws InvalidObjectException if callback
     * object is null.
     */
    void register(ObjectInputValidation obj, int priority) throws InvalidObjectException {
      if (obj == null) {
        throw new InvalidObjectException("null callback");
      }
      
      Callback prev = null, cur = list;
      while (cur != null && priority < cur.priority) {
        prev = cur;
        cur = cur.next;
      }
      if (prev != null) {
        prev.next = new Callback(obj, priority, cur);
      } else {
        list = new Callback(obj, priority, list);
      }
    }
    
    /**
     * Invokes all registered callbacks and clears the callback list.
     * Callbacks with higher priorities are called first; those with equal
     * priorities may be called in any order.  If any of the callbacks
     * throws an InvalidObjectException, the callback process is terminated
     * and the exception propagated upwards.
     */
    void doCallbacks() throws InvalidObjectException {
      try {
        while (list != null) {
          list.obj.validateObject();
          list = list.next;
        }
      } catch (InvalidObjectException ex) {
        list = null;
        throw ex;
      }
    }
    
    /**
     * Resets the callback list to its initial (empty) state.
     */
    public void clear() {
      list = null;
    }
  }
}
