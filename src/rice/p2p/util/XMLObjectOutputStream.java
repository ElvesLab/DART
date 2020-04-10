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
import java.util.*;

/**
 * XMLObjectOutputStream is an extension of ObjectOutputStream which provides
 * for serialization for arbitrary Java objects, in the same manner as the
 * ObjectOutputStream class.  This class supports all of the features of 
 * the ObjectOutputStream, including serialization support for any Java 
 * object graph implementing the Serializable interface, maintenance of
 * references in the object graph, support for Externalizable classes, 
 * custom serialization via the writeObject() method, alternate field
 * writing mechanisms via the putFields() and writeFields() methods, support 
 * for the writeUnshared() method, custom serializable field specification via
 * the serialPersistentFields field, and dynamic object replacement via the 
 * writeReplace() method.
 *
 * The format of the XML data written does conform to the JSX XML 
 * Schema, available online at http://www.jsx.org/jsx.xsd.  This class is 
 * designed to be able to write objects which can then be deserialized using 
 * JSX, however, this has not been fully tested and bugs may be encountered.
 *
 * @version $Id: XMLObjectOutputStream.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class XMLObjectOutputStream extends ObjectOutputStream {
  
  /**
   * A cache of the writeReplace() methods, mapping class->writeReplace()
   */
  protected static SoftHashMap WRITE_REPLACES = new SoftHashMap();
  
  /**
   * A cache of the writeObject() methods, mapping class->writeObject()
   */
  protected static SoftHashMap WRITE_OBJECTS = new SoftHashMap();
  
  /**
   * A cache of the persistentFields, mapping class->Field[]
   */
  protected static SoftHashMap PERSISTENT_FIELDS = new SoftHashMap();
  
  /**
   * The underlying XML writing engine
   */
  protected XMLWriter writer;
  
  /**
   * The collection of references stored in the stream so far, 
   * maps Integer(hash) -> reference name.
   */
  protected Hashtable references;
  
  /**
   * A counter used to generate unique references
   */
  protected int next = 0;

  /**
   * The stack of objects which are currently being written to the stream
   */
  protected Stack currentObjects;
  
  /**
    * The stack of classes which are currently being written to the stream
   */
  protected Stack currentClasses;
  
  /**
   * The stack of putFields which are currently being written to the stream
   */
  protected Stack currentPutFields;

  private String debugstr;
  
  /**
   * Constructor which writes data from the given output stream in order
   * serialize objects.  This constructor also writes the header to the 
   * stream, and throws an IOException if an error occurs while writing
   * the header.
   *
   * @param out The output stream to write data to
   * @throws IOException If the an error occurs
   */
  public XMLObjectOutputStream(OutputStream out) throws IOException {
    super();
    try {
      this.writer = new XMLWriter(out);
      int hash = 0;
      if (writer != null) hash = writer.hashCode();
      this.debugstr = "writer after new, in try: "+writer+" "+hash+"\n";
    } catch (NoClassDefFoundError ncdfe) {
      System.err.println("ERROR: Make sure to add xmlpull.jar to the classpath");
      throw ncdfe; 
    }
    int hash = 0;
    if (writer != null) hash = writer.hashCode();
    this.debugstr += "writer after try: "+writer+" "+hash+"\n";
    this.references = new Hashtable();
    this.currentObjects = new Stack();
    this.currentClasses = new Stack();
    this.currentPutFields = new Stack();
    hash = 0;
    if (writer != null) hash = writer.hashCode();
    this.debugstr += "writer before writeStreamHeader: "+writer+" "+hash+"\n";
    writeStreamHeader();
  }
  
  // ----- ObjectOutputStream Overriding Methods -----
  
  /**
   * Method which writes the XML header to the stream.  Usually, this is the
   * <?xml version="1.0"?> tag, but it may include processing instructions.
   *
   * @throws IOException If an error occurs
   */
  protected void writeStreamHeader() throws IOException {
    if (writer == null) {
      System.out.println("FLUGLE writer is null in writeStreamHeader()...");
      System.out.println(debugstr);
    }
    try {
      writer.writeHeader();
      writer.start("jsx");
      writer.attribute("major", 1);
      writer.attribute("minor", 1);
      writer.attribute("format", "JSX.DataReader");
    } catch (NullPointerException npe) {
      System.out.println("FLUGLE writer NPE'd in writeStreamHeader()...");
      int hash = 0;
      if (writer != null) hash = writer.hashCode();
      this.debugstr += "writer in NPE handler: "+writer+" "+hash+"\n";
      System.out.println(debugstr);
      throw npe;
    }
  }  
  
  /**
   * Method which flushes all buffered data to the output stream.  
   *
   * @throws IOException if an error occurs
   */
  public void flush() throws IOException {
    writer.flush();
  }
  
  /**
   * Method which closes the underlying output stream for writing.  Any subsequent 
   * writes will throw an IOException.
   */
  public void close() throws IOException {
    writer.end("jsx");
    writer.close();
  }
  
  /**
   * Method which resets the output stream, which removes the binding of all previously
   * stored references.  If the same object is written before and after a reset(), it
   * will be written twice to the stream.
   *
   * @throws IOException If an error occurs, or an object is currently being written
   */
  public void reset() throws IOException {
    if (currentObjects.peek() == null) {
      references = new Hashtable(); 
      writeReset();
    } else {
      throw new IOException("Reset called during active write!");
    }
  }
  
  /**
   * Method which writes a byte to the underlying output stream.  Simply calls
   * writeByte(b)
   *
   * @throws IOException If an error occurs
   */
  public void write(byte b) throws IOException {
    writeByte(b);
  }
  
  /**
   * Method which writes a array of bytes to the underlying output stream.  Simply calls
   * writeByte(b[x]) on each of the array elements.
   *
   * @throws IOException If an error occurs
   */
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }
  
  /**
   * Method which writes a array of bytes to the underlying output stream.  Simply calls
   * writeByte(b[x]) on each of the array elements.
   *
   * @throws IOException If an error occurs
   */
  public void write(byte[] b, int offset, int length) throws IOException {
    writer.start("base64");
    writer.attribute("length", length);
    writer.writeBase64(b, offset, length);
    writer.end("base64");
  }
  
  /**
   * Method which writes an int to the stream.
   *
   * @param i The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeInt(int i) throws IOException {
    writePrimitive(i, null);
  }
  
  /**
   * Method which writes an boolean to the stream.
   *
   * @param b The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeBoolean(boolean b) throws IOException {
    writePrimitive(b, null);
  }
  
  /**
   * Method which writes an byte to the stream.
   *
   * @param i The value to write to the stream, casted to a byte
   * @throws IOException If an error occurs
   */
  public void writeByte(int i) throws IOException {
    writeByte((byte) i);
  }
  
  /**
   * Method which writes an byte to the stream.
   *
   * @param b The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeByte(byte b) throws IOException {
    writePrimitive(b, null);
  }
  
  /**
   * Method which writes an char to the stream.
   *
   * @param i The value to write to the stream, casted to an int
   * @throws IOException If an error occurs
   */
  public void writeChar(int i) throws IOException {
    writeChar((char) i);
  }
  
  /**
   * Method which writes an char to the stream.
   *
   * @param c The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeChar(char c) throws IOException {
    writePrimitive(c, null);
  }
  
  /**
   * Method which writes an double to the stream.
   *
   * @param d The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeDouble(double d) throws IOException {
    writePrimitive(d, null);
  }
  
  /**
   * Method which writes an float to the stream.
   *
   * @param f The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeFloat(float f) throws IOException {
    writePrimitive(f, null);
  }
  
  /**
   * Method which writes an long to the stream.
   *
   * @param l The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeLong(long l) throws IOException {
    writePrimitive(l, null);
  }
  
  /**
   * Method which writes an short to the stream.
   *
   * @param i The value to write to the stream, casted to a short
   * @throws IOException If an error occurs
   */
  public void writeShort(int i) throws IOException {
    writeShort((short) i);
  }
  
  /**
   * Method which writes an short to the stream.
   *
   * @param s The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeShort(short s) throws IOException {
    writePrimitive(s, null);
  } 
  
  /**
   * Method which writes a UTF-encoded String to the stream. This method 
   * simply calls writeObject(), as all strings are UTF encoded in XML.
   *
   * @throws IOException If an error occurs
   */
  public void writeUTF(String s) throws IOException {
    writeObject(s);
  }
  
  /**
   * Method which writes a String as a sequence of chars to the stream. This
   * method is equivalent to calling s.toCharArray() and calling writeChar()
   * on each element.  Using this method is *NOT* recommended, as it is an
   * increadible waste of space.
   *
   * @throws IOException If an error occurs
   */
  public void writeChars(String s) throws IOException {
    char[] chars = s.toCharArray();
    
    for (int i=0; i<chars.length; i++) 
      writeChar(chars[i]);
  }
  
  /**
    * Method which writes a String as a sequence of chars to the stream. This
   * method is equivalent to calling s.toCharArray() and calling writeChar()
   * on each element.  Using this method is *NOT* recommended, as it is an
   * increadible waste of space.
   *
   * @throws IOException If an error occurs
   */
  public void writeBytes(String s) throws IOException {
    byte[] bytes = s.getBytes();
    
    for (int i=0; i<bytes.length; i++) 
      writeByte(bytes[i]);
  }
  
  /**
   * Method which is called by ObjectOutputStream.writeObject(), and writes
   * the given object to the stream.  If the object (or any of it's descendents) 
   * has a writeReplace() method, that object is written instead of the original 
   * object.
   *
   * @param o The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeObjectOverride(Object o) throws IOException {
    writeObject(o, null);
  }
  
  /**
   * Method which writes the given object to the stream and does not record a 
   * reference to the object.  This guarantees that in future writes of this
   * object will not reference this copy of the object.
   *
   * @param o The value to write to the stream
   * @throws IOException If an error occurs
   */
  public void writeUnshared(Object o) throws IOException {
    writeObjectUnshared(o, null, false);
  }
  
  /**
   * Method which can be called by objects if they have a writeObject() method.  
   * This method initiates the default field serialization mechanism, and
   * writes all of the object's fields to the stream.  If this method is called,
   * the putFields() method CANNOT be called by the same object.  However, one
   * of these two methods MUST be called in the context of a writeObject().
   *
   * @throws IOException If an error occurs
   * @throws NotActiveException If a object is not currently being written
   */
  public void defaultWriteObject() throws IOException {
    if (currentObjects.peek() != null)
      writeFields(currentObjects.peek(), (Class) currentClasses.peek());
    else
      throw new NotActiveException();
  }
  
  /**
   * Method which can be called by objects if they have a writeObject() method.  
   * This method returns an object which the callee can then use to specify
   * exactly which fields and values should be written to the steam, and can
   * specify fields which do not exist as a member of the object. If this method is 
   * called, the defaultWriteObject() method CANNOT be called by the same object.  
   * However, one of these two methods MUST be called in the context of a writeObject().
   *
   * @return A PutField, a container for fields to be written
   * @throws IOException If an error occurs
   * @throws NotActiveException If a object is not currently being read
   */
  public ObjectOutputStream.PutField putFields() throws IOException {
    if (currentPutFields.peek() != null)
      return (PutField) currentPutFields.peek();
    else
      throw new NotActiveException();
  }
  
  /**
   * Method which writes the current state of the PutField object to the stream as
   * this object's fields. If this method is called, the defaultWriteObject() method 
   * CANNOT be called by the same object.  However, one of these two methods MUST be 
   * called in the context of a writeObject().
   *
   * @throws IOException If an error occurs
   * @throws NotActiveException If a object is not currently being read
   */
  public void writeFields() throws IOException {
    if (currentObjects.peek() != null)
      writePutFields((PutField) putFields());
    else
      throw new NotActiveException();
  }
  
  // ----- Internal Helper Methods -----
  
  /**
   * This method returns the writeReplce() method of a given class, if such a method 
   * exists.  This method searches the class's heirarchy for a writeReplace() method
   * which is assessible by the given class.  If no such method is found, null is returned.
   *
   * @param cl The class to find the writeReplace() of
   * @return The method, or null if none was found
   */
  private Method getWriteReplace(Class cl) {
    if (WRITE_REPLACES.containsKey(cl))
      return (Method) WRITE_REPLACES.get(cl);
    
    Method meth = null;
    Class defCl = cl;
    while (defCl != null) {
      try {
        meth = defCl.getDeclaredMethod("writeReplace", new Class[0]);
        break;
      } catch (NoSuchMethodException ex) {
        defCl = defCl.getSuperclass();
      }
    }
    
    if (meth == null) {
      WRITE_REPLACES.put(cl, meth);
      return null;
    }
    
    meth.setAccessible(true);
    int mods = meth.getModifiers();
    if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0) {
    } else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0) {
      WRITE_REPLACES.put(cl, meth);
      return meth;
    } else if ((mods & Modifier.PRIVATE) != 0) {
      if (cl == defCl) {
        WRITE_REPLACES.put(cl, meth);
        return meth;
      }
    } else {
      WRITE_REPLACES.put(cl, meth);
      return meth;
    }
      
    WRITE_REPLACES.put(cl, null);
    return null;
  }
  
  /**
    * This method returns the readResolve() method of a given class, if such a method 
   * exists.  This method searches the class's heirarchy for a readResolve() method
   * which is assessible by the given class.  If no such method is found, null is returned.
   *
   * @param cl The class to find the readResolve() of
   * @return The method, or null if none was found
   */
  private static Method getWriteObject(Class cl) {
    synchronized (WRITE_OBJECTS) {
      if (WRITE_OBJECTS.containsKey(cl)) 
        return (Method) WRITE_OBJECTS.get(cl);
      
      try {
        Method method = cl.getDeclaredMethod("writeObject", new Class[] {ObjectOutputStream.class});
        method.setAccessible(true);
        
        WRITE_OBJECTS.put(cl, method);
        return method;
      } catch (NoSuchMethodException e) {
        WRITE_OBJECTS.put(cl, null);
        return null;
      }
    }
  }
  
  /**
   * Method which returns the serializable fields of the provided class.  If
   * the class has a serialPersistentFields field, then that is used to determine
   * which fields are serializable.  Otherwise, all declared non-static and non-transient
   * fields are returned.
   *
   * @param c The class to return the fields for
   */
  protected Field[] getPersistentFields(Class cl) {
    synchronized (PERSISTENT_FIELDS) {
      if (PERSISTENT_FIELDS.containsKey(cl))
        return (Field[]) PERSISTENT_FIELDS.get(cl);
          
      Field[] fields = getSerialPersistentFields(cl);
    
      if (fields == null) {
        fields = cl.getDeclaredFields();
      }
    
      PERSISTENT_FIELDS.put(cl, fields);
      return fields;
    }
  }
  
  /**
   * Method which returns the serializablePersistenFields field of the provided class.  
   * If no such field exists, then null is returned. 
   *
   * @param c The class to return the fields for
   */
  protected Field[] getSerialPersistentFields(Class c) {    
    try {
      Field f = c.getDeclaredField("serialPersistentFields");
      int mask = Modifier.PRIVATE | Modifier.STATIC | Modifier.FINAL;
      if ((f.getModifiers() & mask) != mask) {
        return null;
      }
      
      f.setAccessible(true);
      ObjectStreamField[] serialPersistentFields = (ObjectStreamField[]) f.get(null);
      
      Field[] fields = new Field[serialPersistentFields.length];
      
      for (int i = 0; i < serialPersistentFields.length; i++) {
        ObjectStreamField spf = serialPersistentFields[i];
        
        Field thisf = c.getDeclaredField(spf.getName());
        if (! ((thisf.getType() == spf.getType()) && ((thisf.getModifiers() & Modifier.STATIC) == 0))) {
          return null;
        }
          
        fields[i] = thisf;
      }
      
      return fields;
    } catch (NoSuchFieldException ex) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
  }
  
  /**
   * Method which returns the component type of the given array
   * class.  If the class is not of type array, the class itself
   * is returned
   *
   * @return The component type of the array, or the class itself if not an array
   */
  protected Class getComponentType(Class array) {
    if (array.isArray())
      return getComponentType(array.getComponentType());
    
    return array;
  }
  
  /**
   * Method which returns the dimension of the given array class.  This
   * is determines recursively by using the getCompoenetType() method
   * on the class.
   *
   * @return The dimension of the corresponding array class
   */
  protected int getDimension(Class array) {
    if (array.isArray())
      return 1 + getDimension(array.getComponentType());
    
    return 0;
  }
  
  /**
   * Method which returns an array of classes representing the class 
   * hierarchy of the provided class, exempting the Object class.  
   *
   * @param c The class to return the heirarchy for
   * @return The heierarchy of the provided class
   */
  protected Class[] getSuperClasses(Class c) {
    Vector v = new Vector();
    
    while (true) {
      if (c.getSuperclass().equals((new Object()).getClass())) 
        break;
      c = c.getSuperclass();
      
      v.addElement(c);
    }
    
    return (Class[]) v.toArray(new Class[0]);
  }  
  
  // ----- ObjectOutputStream Referencing Methods -----
  
  /**
   * Method which determines a unique hash value for each object.  This implementation
   * uses the System.identityHashCode() method, which returns the memory address.
   *
   * @return An integer, representing a unqiue hash value
   */
  protected int hash(Object o) {
    return System.identityHashCode(o) & 0x7FFFFFFF;
  }
  
  /**
   * Method which adds a reference in the hashtable of references.
   * Multiple calls to this method will replace prior objects.
   *
   * @param o The object to reference
   * @param reference The reference name to use
   */
  protected void putReference(Object o, String reference) {
    Reference ref = new Reference(o);
    
    if (references.get(ref) == null) {
      references.put(ref, reference);
    } else {
      System.out.println("SERIOUS ERROR: Attempt to re-store reference: " + 
                         "EXISTING: " + ((Reference) references.get(ref)).object + " " + references.get(ref).hashCode() + 
                         "NEW: " + o + " " + o.hashCode());
    }
  }
  
  /**
   * Method which returns a previously stored reference.  If the
   * reference cannot be found, null is returned.
   *
   * @param o The object to look up
   * @return The reference name, or null if none is found
   */
  protected String getReference(Object o) {
    return (String) references.get(new Reference(o));
  }
  
  /**
   * Method which assigns a new unique reference. 
   *
   * @return A new unique reference
   */
  protected String assignReference() {
    return "i" + (next++);
  }
  
  // ----- ObjectOutputStream WriteX Methods -----
  
  /**
   * Method which writes a reset command to the stream.
   *
   * @throws IOException If an error occurs
   */
  protected void writeReset() throws IOException {
    writer.start("reset");
    writer.end("reset");
  }
  
  /**
   * Method which writes an object to the stream as the given field name
   * If the next object represents a reference or null, then the appropriate 
   * helper is called.  Otherwise, writeObjectUnshared() is called to process the object.
   *
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @throws IOException If an error occurs
   * @throws ClassNotFoundException If the class cannot be found
   */
  protected void writeObject(Object o, String field) throws IOException {
    if (o == null) {
      writeNull(field);
    } else if (getReference(o) != null) {
      writeReference(o, field);
    } else {      
      writeObjectUnshared(o, field, true);
    }
  }
  
  /**
   * Method which writes an object to the stream.  This method assumes that
   * the type of object which is being read is *sharable*, even if it is
   * not going to be shared.  Thus, this method can write objects of type
   * String, Array, or Serializable.  This method first calls the object's
   * writeReplace() method, if such a method exists, and if a different
   * object is returned, that object is written instead.
   *   
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @param shared Whether or not to record a reference to this object
   * @throws IOException If an error occurs
   */
  protected void writeObjectUnshared(Object o, String field, boolean shared) throws IOException {
    Method replace = getWriteReplace(o.getClass());
      
    if (replace != null) {
      try {
        o = replace.invoke(o, new Object[0]);
        
        if (o == null) {
          writeNull(field);
          return;
        } else if (getReference(o) != null) {
          writeReference(o, field);
          return; 
        }
      } catch (IllegalAccessException e) {
        throw new IOException("IllegalAccessException thrown! " + e);
      } catch (InvocationTargetException e) {
        throw new IOException("InvocationTargetException thrown! " + e.getTargetException());
      }
    } 
    
    if (shared) 
      putReference(o, assignReference());
      
    if (o instanceof String) {
      writeString((String) o, field, shared);
    } else if (o.getClass().isArray()) {
      writeArray(o, field, shared);
    } else {
      writeOrdinaryObject(o, field, shared);
    }
  }
  
  /**
   * Method which writes a null item to the stream as the provided field.
   *
   * @param field The field name to write the object as
   * @throws IOException If an error occurs
   */  
  protected void writeNull(String field) throws IOException {
    writer.start("null");
    
    if (field != null)
      writer.attribute("field", field);
    
    writer.end("null");
  }
  
  /**
   * Method which writes a string to the stream as the provided field.
   *
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @param shared Whether or not to record a reference to this object
   * @throws IOException If an error occurs
   */  
  protected void writeString(String s, String field, boolean shared) throws IOException {
    writer.start("string");
    
    if (field != null)
      writer.attribute("field", field);
    
    if (shared && getReference(s) != null)
      writer.attribute("id", getReference(s));
    
    writer.attribute("value", s);
    writer.end("string");
  }
  
  /**
   * Method which writes an array to the stream.  This method writes the array 
   * header, and then recursively writes all of the objects in the array to the 
   * stream.  If a non-serializable object is found, the object is replaced with null.
   *  
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @param shared Whether or not to record a reference to this object
   * @throws IOException If an error occurs
   */  
  protected void writeArray(Object o, String field, boolean shared) throws IOException {    
    writer.start("array");
    
    if (field != null)
      writer.attribute("field", field);
    
    if (shared && getReference(o) != null)
      writer.attribute("id", getReference(o));
    
    writer.attribute("base", o.getClass().getComponentType().getName());
    writer.attribute("dim" , getDimension(o.getClass()));
    writer.attribute("length", Array.getLength(o));
    
    if (o.getClass().getComponentType().isPrimitive()) {
      Class c = o.getClass().getComponentType();

      for (int i=0; i<Array.getLength(o); i++) {
        if (c.equals(Integer.TYPE)) {
          writePrimitive(Array.getInt(o, i), null);
        } else if (c.equals(Boolean.TYPE)) {
          writePrimitive(Array.getBoolean(o, i), null);
        } else if (c.equals(Byte.TYPE)) {
          writePrimitive(Array.getByte(o, i), null);
        } else if (c.equals(Character.TYPE)) {
          writePrimitive(Array.getChar(o, i), null);
        } else if (c.equals(Double.TYPE)) {
          writePrimitive(Array.getDouble(o, i), null);
        } else if (c.equals(Float.TYPE)) {
          writePrimitive(Array.getFloat(o, i), null);
        } else if (c.equals(Long.TYPE)) {
          writePrimitive(Array.getLong(o, i), null);
        } else if (c == Short.TYPE) {
          writePrimitive(Array.getShort(o, i), null);
        } else {
          throw new IllegalArgumentException("Class " + c + " is not primitive!");
        }
      } 
    } else {
      for (int i=0; i<Array.getLength(o); i++) {
        if (Array.get(o, i) instanceof Serializable)
          writeObject(Array.get(o, i), null);
        else
          writeNull(null);
      }
    }
    
    writer.end("array");
  }
  
  /**
   * Method which writes a reference to the stream, determined from the references table.  This
   * method throws an IOException if a reference is not found to the provided object.
   *
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @throws IOException If an error occurs
   */  
  protected void writeReference(Object o, String field) throws IOException {
    writer.start("reference");
    
    if (field != null)
      writer.attribute("field", field);
    
    writer.attribute("idref", getReference(o));
    writer.end("reference");
  }  
  
  /**
   * Method which writes an ordinary object to the stream (not a String or Array).
   * This method first writes the class type, and a list of the classes' supertypes.
   * If the object is Externalizable, the writeExternal() method is called and the
   * method returns.  Otherwise, each of the superclasses are written, from highest
   * to lowest, using the writeClass() method.  If the object is not serializable,
   * a NotSerializableException is thrown.
   *  
   * @param o The object to write to the stream
   * @param field The field name to write the object as
   * @param shared Whether or not to record a reference to this object
   * @throws IOException If an error occurs
   * @throws NotSerializableException If the object is not serializable
   */  
  protected void writeOrdinaryObject(Object o, String field, boolean shared) throws IOException {
    if (! Serializable.class.isAssignableFrom(o.getClass())) {
      if (field == null)
        throw new NotSerializableException(o.getClass().getName() + " " + field + " " + shared);
      else
        return;
    }
    
    writer.start("object");
    
    if (shared && getReference(o) != null)
      writer.attribute("id", getReference(o));
    
    if (field != null)
      writer.attribute("field", field);
    
    writer.attribute("class", o.getClass().getName());
    
    StringBuffer classes = new StringBuffer();
    Class[] supers = getSuperClasses(o.getClass());
    
    for (int i=0; i<supers.length; i++) {
      classes.append(supers[i].getName());
      classes.append(",");
    }
    
    writer.attribute("superclasses", classes.toString());
    
    if (o instanceof Externalizable) {
      ((Externalizable) o).writeExternal(this);
    } else {
      for (int i=supers.length-1; i>=0; i--) {
        if (Serializable.class.isAssignableFrom(supers[i]))
          writeClass(o, supers[i]);
      }
      
      writeClass(o, o.getClass());
    }
    
    writer.end("object");
  }
  
  /**
   * Method which writes the information for one class for a given object to the 
   * stream. This method first writes the class type, and if it defines a writeObject()
   * method, it is called.  Otherwise, the fields are written in a default manner.  Any 
   * extra data not read in the stream is read and ignored.
   *
   * @param o The object to write to the stream
   * @param c The class of the object to write
   * @throws IOException If an error occurs
   */
  protected void writeClass(Object o, Class c) throws IOException {
    writer.start("declaredClass");
    writer.attribute("class", c.getName());
    
    Method method = getWriteObject(c);
    
    if (method != null) {
      try {
        currentObjects.push(o);
        currentClasses.push(c);
        currentPutFields.push(new PutField());
        method.invoke(o, new Object[] {this});
        currentObjects.pop();
        currentClasses.pop();
        currentPutFields.pop();
      } catch (IllegalAccessException e) {
        throw new IOException("IllegalAccessException thrown! " + e);
      } catch (InvocationTargetException e) {
        throw new IOException("InvocationTargetException thrown! " + e.getTargetException());
      }
    } else {
      writeFields(o, c);
    } 
    
    writer.end("declaredClass"); 
  }
  
  /**
   * Method which initiates the default field writing mechanism for the given
   * object's class.  This method only writes the non-static and non-transient
   * fields to the stream - all others are ignored.  It reads all of the fields 
   * using the writeField() method.
   *
   * @param o The object to write to the stream
   * @param c The class of the object to write
   * @throws IOException If an error occurs
   */
  protected void writeFields(Object o, Class c) throws IOException {
    writer.start("default");
    
    Field[] fields = getPersistentFields(c);
    
    for (int i=0; i<fields.length; i++) {
      fields[i].setAccessible(true);
      
      if (! (Modifier.isStatic(fields[i].getModifiers()) || 
             Modifier.isTransient(fields[i].getModifiers()))) {
        if (fields[i].getType().isPrimitive()) {
          writePrimitiveField(o, fields[i]);
        } else {
          try {
            writeObject(fields[i].get(o), fields[i].getName());
          } catch (IllegalAccessException e) {
            throw new IOException("IllegalAccessException thrown " + e);
          }
        }
      }
    }
    
    writer.end("default");
  }
  
  /**
   * Method which writes out the data from the given PutField class as the data 
   * for the given class.  Each item in the PutField is written as the type it
   * was inserted as.  Thus, fields can be written which do not actually exist 
   * in the class.
   *
   * @param p The fields to write to  the stream
   * @throws IOException If an error occurs
   */
  protected void writePutFields(PutField p) throws IOException {
    writer.start("default");
    
    String[] primitives = p.getPrimitives();
    
    for (int i=0; i<primitives.length; i++) {
      String name = primitives[i];
      Object primitive = p.getPrimitive(name);
      
      if (primitive.getClass().equals(Integer.class)) {
        writePrimitive(p.getInt(name), name); 
      } else if (primitive.getClass().equals(Boolean.class)) {
        writePrimitive(p.getBoolean(name), name); 
      } else if (primitive.getClass().equals(Byte.class)) {
        writePrimitive(p.getByte(name), name); 
      } else if (primitive.getClass().equals(Character.class)) {
        writePrimitive(p.getChar(name), name); 
      } else if (primitive.getClass().equals(Double.class)) {
        writePrimitive(p.getDouble(name), name); 
      } else if (primitive.getClass().equals(Float.class)) {
        writePrimitive(p.getFloat(name), name); 
      } else if (primitive.getClass().equals(Long.class)) {
        writePrimitive(p.getLong(name), name); 
      } else if (primitive.getClass().equals(Short.class)) {
        writePrimitive(p.getShort(name), name); 
      } else {
        throw new IllegalArgumentException("Field " + name + " is not primitive!");
      }
    }
    
    String[] objects = p.getObjects();
    
    for (int i=0; i<objects.length; i++) {
      writeObject(p.getObject(objects[i]), objects[i]);
    }
    
    writer.end("default");
  }
  
  /**
   * Method which writes a primitive field to the stream.  This method determiens
   * the type of field that needs to be written, and calls f.getX(o), wiritng it 
   * to the stream.
   *
   * @param o The object get the primitive from
   * @param f The field representing the primitive about to be written
   * @throws IOException If an error occurs
   */    
  protected void writePrimitiveField(Object o, Field f) throws IOException {
    try {
      if (f.getType().equals(Integer.TYPE)) {
        writePrimitive(f.getInt(o), f.getName());
      } else if (f.getType().equals(Boolean.TYPE)) {
        writePrimitive(f.getBoolean(o), f.getName());
      } else if (f.getType().equals(Byte.TYPE)) {
        writePrimitive(f.getByte(o), f.getName());
      } else if (f.getType().equals(Character.TYPE)) {
        writePrimitive(f.getChar(o), f.getName());
      } else if (f.getType().equals(Double.TYPE)) {
        writePrimitive(f.getDouble(o), f.getName());
      } else if (f.getType().equals(Float.TYPE)) {
        writePrimitive(f.getFloat(o), f.getName());
      } else if (f.getType().equals(Long.TYPE)) {
        writePrimitive(f.getLong(o), f.getName());
      } else if (f.getType().equals(Short.TYPE)) {
        writePrimitive(f.getShort(o), f.getName());
      } else {
        throw new IllegalArgumentException("Field " + f + " is not primitive!");
      }
    } catch (IllegalAccessException e) {
      throw new IOException("IllegalAccessException thrown " + e);
    }
  }
  
  /**
   * Method which writes a int to the stream, as the given field name.
   *
   * @param i The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(int i, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "int");
    writer.attribute("value", i);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a boolean to the stream, as the given field name.
   *
   * @param b The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(boolean b, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "boolean");
    writer.attribute("value", b);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a byte to the stream, as the given field name.
   *
   * @param b The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(byte b, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "byte");
    writer.attribute("value", b);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a char to the stream, as the given field name.
   *
   * @param c The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(char c, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "char");
    writer.attribute("value", c);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a double to the stream, as the given field name.
   *
   * @param d The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(double d, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "double");
    writer.attribute("value", d);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a float to the stream, as the given field name.
   *
   * @param f The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(float f, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "float");
    writer.attribute("value", f);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a long to the stream, as the given field name.
   *
   * @param l The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(long l, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "long");
    writer.attribute("value", l);
    
    writer.end("primitive");
  }  
  
  /**
   * Method which writes a short to the stream, as the given field name.
   *
   * @param s The value to write to the stream
   * @param field The field name to use
   * @throws IOException If an error occurs
   */
  protected void writePrimitive(short s, String field) throws IOException {
    writer.start("primitive");
    
    if (field != null)
      writer.attribute("field", field);
    writer.attribute("type", "short");
    writer.attribute("value", s);
    
    writer.end("primitive");
  }
  
  
  // ----- Implementation of PutField -----
  
  /**
   * This class is an implementation of PutField which is compatible with 
   * the XMLObjectOutputStream.  It works in the same manner as the 
   * ObjectInputStream.PutField.
   */  
  public class PutField extends ObjectOutputStream.PutField {

    protected Hashtable primitives = new Hashtable();
    
    protected Hashtable objects = new Hashtable();
    
    protected String[] getPrimitives() {
      return (String[]) primitives.keySet().toArray(new String[0]);
    }
    
    protected String[] getObjects() {
      return (String[]) objects.keySet().toArray(new String[0]);
    }
    
    public void put(String name, boolean value) {
      primitives.put(name, new Boolean(value));
    }
    
    public void put(String name, byte value) {
      primitives.put(name, new Byte(value));
    }
    
    public void put(String name, char value) {
      primitives.put(name, new Character(value));
    }
    
    public void put(String name, double value) {
      primitives.put(name, new Double(value));
    }

    public void put(String name, float value) {
      primitives.put(name, new Float(value));
    }
    
    public void put(String name, int value) {
      primitives.put(name, new Integer(value));
    }
    
    public void put(String name, long value) {
      primitives.put(name, new Long(value));
    }
    
    public void put(String name, short value) {
      primitives.put(name, new Short(value));
    }
    
    public void put(String name, Object value) {
      objects.put(name, value);
    }
    
    private Object getPrimitive(String name) {
      return primitives.get(name);
    }
    
    protected Object getObject(String name) {
      return objects.get(name);
    }
    
    protected boolean getBoolean(String name) {
      return ((Boolean) getPrimitive(name)).booleanValue();
    }
    
    protected byte getByte(String name) {
      return ((Byte) getPrimitive(name)).byteValue();
    }
    
    protected char getChar(String name) {
      return ((Character) getPrimitive(name)).charValue();
    }
    
    protected double getDouble(String name) {
      return ((Double) getPrimitive(name)).doubleValue();
    }
    
    protected float getFloat(String name) {
      return ((Float) getPrimitive(name)).floatValue();
    }
    
    protected int getInt(String name) {
      return ((Integer) getPrimitive(name)).intValue();
    }
    
    protected long getLong(String name) {
      return ((Long) getPrimitive(name)).longValue();
    }
    
    protected short getShort(String name) {
      return ((Short) getPrimitive(name)).shortValue();
    } 
    
    /**
     * only exists to satisfy deprecated method in superclass
     * @deprecated
     */
    public void write(ObjectOutput output) throws IOException {
      XMLObjectOutputStream xoos = (XMLObjectOutputStream) output;
      xoos.writeFields();
    }
  }
  
  // Internal class for maintaining references
  
  private class Reference {
   
    private Object object;
    
    public Reference(Object object) {
      this.object = object;
    }
    
    public int hashCode() {
      return hash(object);
    }
    
    public boolean equals(Object o) {
      return (((Reference) o).object == object); 
    }
  }
}
