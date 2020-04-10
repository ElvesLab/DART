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

package rice.p2p.util.testing;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.zip.*;
import rice.p2p.util.*;

@SuppressWarnings("unchecked")
public class XMLObjectStreamUnit {
  
  protected XMLObjectOutputStream xoos;
  
  protected XMLObjectInputStream xois;
  
  protected ByteArrayOutputStream baos;
  
  protected ByteArrayInputStream bais;
  
  public XMLObjectStreamUnit() throws IOException {
    reset();
  }
  
  protected void reset() throws IOException {
    baos = new ByteArrayOutputStream();
 //   xoos = new XMLObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(baos)));
    xoos = new XMLObjectOutputStream(new BufferedOutputStream(baos));
    bais = null;
    xois = null;
  }
  
  protected void flip() throws IOException {
    xoos.close();
    bais = new ByteArrayInputStream(baos.toByteArray());
 //   xois = new XMLObjectInputStreamm(new GZIPInputStream(bais));
    xois = new XMLObjectInputStream(bais);
  }
  
  protected void testInt(int i) {
    try {
      xoos.writeInt(i);
      flip();
      int j = xois.readInt();
      reset();
      
      if (i != j)
        throw new IOException("int " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testBoolean(boolean b) {
    try {
      xoos.writeBoolean(b);
      flip();
      boolean c = xois.readBoolean();
      String xml = new String(baos.toByteArray());
      reset();
      
      if (b != c)
        throw new IOException("boolean " + c + " was not equal to " + b + ". XML: " + xml);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testByte(byte i) {
    try {
      xoos.writeByte(i);
      flip();
      byte j = xois.readByte();
      reset();
      
      if (i != j)
        throw new IOException("byte " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testChar(char i) {
    try {
      xoos.writeChar(i);
      flip();
      char j = xois.readChar();
      reset();
      
      if (i != j)
        throw new IOException("char " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testDouble(double i) {
    try {
      xoos.writeDouble(i);
      flip();
      double j = xois.readDouble();
      reset();
      
      if (i != j)
        throw new IOException("double " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testFloat(float i) {
    try {
      xoos.writeFloat(i);
      flip();
      float j = xois.readFloat();
      reset();
      
      if (i != j)
        throw new IOException("float " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testLong(long i) {
    try {
      xoos.writeLong(i);
      flip();
      long j = xois.readLong();
      reset();
      
      if (i != j)
        throw new IOException("long " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testShort(short i) {
    try {
      xoos.writeShort(i);
      flip();
      short j = xois.readShort();
      reset();
      
      if (i != j)
        throw new IOException("short " + j + " was not equal to " + i);
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testMultiplePrimitives() {
    try {
      int i = -1029;
      double d = 2939.22;
      long l = 1929029389303L;
      
      xoos.writeInt(i);
      xoos.writeDouble(d);
      xoos.writeLong(l);
      flip();
      if ((xois.readInt() != i) || (xois.readDouble() != d) || (xois.readLong() != l))
        throw new IOException("Multiple primitives were not read correctly!");
      
      reset();      
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }

  protected boolean compare(Object o1, Object o2) {
    if (o1.equals(o2))
      return true;
    
//    System.out.println("comparing: "+o1.getClass()+": "+o1+" "+o2.getClass()+" "+o2);
    
    if (o1.getClass().isArray() && o2.getClass().isArray() &&
        o1.getClass().getComponentType().equals(o2.getClass().getComponentType())) {
      if (Array.getLength(o1) != Array.getLength(o2))
        return false;
      if (o2.getClass().getComponentType().isPrimitive()) {
        Class c = o2.getClass().getComponentType();
        if (c.equals(Integer.TYPE)) {
          return Arrays.equals((int[])o1,(int[])o2);
        } else if (c.equals(Boolean.TYPE)) {
          return Arrays.equals((boolean[])o1,(boolean[])o2);
        } else if (c.equals(Byte.TYPE)) {
          return Arrays.equals((byte[])o1,(byte[])o2);
        } else if (c.equals(Character.TYPE)) {
          return Arrays.equals((char[])o1,(char[])o2);
        } else if (c.equals(Double.TYPE)) {
          return Arrays.equals((double[])o1,(double[])o2);
        } else if (c.equals(Float.TYPE)) {
          return Arrays.equals((float[])o1,(float[])o2);
        } else if (c.equals(Long.TYPE)) {
          return Arrays.equals((long[])o1,(long[])o2);
        } else if (c == Short.TYPE) {
          return Arrays.equals((short[])o1,(short[])o2);
        } else {
          throw new IllegalArgumentException("Class " + c + " is not primitive!");
        }
      } else {
        for (int i = 0; i < Array.getLength(o1); i++)
          if (!compare(Array.get(o1,i),Array.get(o2,i)))
            return false;
        return true;
      }
    }
    
    return false;
  }
  
  protected void test(Object o) {
    try {
      long start = System.currentTimeMillis();
      xoos.writeObject(o);
      flip();
      long mid = System.currentTimeMillis();
      Object o2 = xois.readObject();
      long end = System.currentTimeMillis();

      System.out.println("WRITE: " + (mid - start) + " READ: " + (end - mid));
      
      if (o == o2)
        throw new IOException("Returned object is identicial to first!");
      
      if (!compare(o,o2)) {
        System.out.println("XML IS: " + (new String(baos.toByteArray())));
        throw new IOException("Object " + o2 + " was not equal to " + o);
      }

      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testHashtable() {
    try {
      Hashtable table = new Hashtable();
      Integer i = new Integer(1020);
      table.put("monkey", i);
      table.put("cat", i);
      table.put(i, i);
      
      xoos.writeObject(table);
      flip();
      Hashtable table2 = (Hashtable) xois.readObject();
      
      if (table == table2)
        throw new IOException("Returned table is identicial to first!");
      
      if (! (table.equals(table2)))
        throw new IOException("Object " + table2 + " was not equal to " + table);
      
      if (! (table2.get("monkey") == table2.get("cat")))
        throw new IOException("Object " + table2.get("monkey") + " was not equal to " + table2.get("cat"));

      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testMultipleObjects() {
    try {
      xoos.writeObject(new Integer(28));
      xoos.writeObject(new Vector());
      flip();
      
      if (! (xois.readObject().equals(new Integer(28)) || xois.readObject().equals(new Vector())))
        throw new IOException("Objects are not equal!");
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testUnserializableObject() {
    try {
      try {
        xoos.writeObject(new Object());
      } catch (NotSerializableException e) {
        reset();
        return;
      }
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testByteCustomSerializer() {
    try {
      TestByteSerialization object = new TestByteSerialization();
      xoos.writeObject(object);
      flip();
      System.out.println("XML IS: " + (new String(baos.toByteArray())));
      
      TestByteSerialization object2 = (TestByteSerialization) xois.readObject();

      if (object2 == null)
        throw new IOException("Object was null!");
      
      if (object2.bytes() == null)
        throw new IOException("Object bytes was null!");
      
      if (object2.bytes().length != 5)
        throw new IOException("Object did not have correct length " + object2.bytes().length);
      
      for (int i=0; i<4; i++) 
        if (object.bytes()[i] != object2.bytes()[i])
          throw new IOException("Object did not have correct byte!"); 

      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testCustomSerializer() {
    try {
      Object object = new Serializable() {
        private int hashCode = 83;
        
        public int hashCode() {
          return hashCode;
        }
        
        private void readObject(ObjectInputStream oos) throws IOException, ClassNotFoundException {
          hashCode = 100;
        }
        
        private void writeObject(ObjectOutputStream oos) throws IOException {
        }
      };
      
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      
      if (object2.hashCode() != 100)
        throw new IOException("Object did not have correct hashCode " + object2.hashCode());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testBrokenCustomSerializer() {
    try {
      try {
        Object object = new Serializable() {
          private void writeObject(ObjectOutputStream oos) throws IOException {
          }
        };
        
        xoos.writeObject(object);
        flip();
        xois.readObject();
      } catch (IOException e) {
        reset();
        
        return;
      } catch (ClassNotFoundException e) {
        System.out.println("test produced exception " + e);
        e.printStackTrace();
      }
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } 
  }
  
  protected void testUnshared() {
    try {
      Object object = new Integer(3);
      
      xoos.writeObject(object);
      xoos.writeUnshared(object);
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      Object object3 = xois.readObject();
      Object object4 = xois.readObject();
      
      if ((object2 == object3) || (object3 == object4) || (object2 != object4))
        throw new IOException("Object did not have correct equality " + System.identityHashCode(object2)
                              + " " + System.identityHashCode(object3)
                              + " " + System.identityHashCode(object4));
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testExternal() {
    try {
      TestExternalizable object = new TestExternalizable();
      
      xoos.writeObject(object);
      flip();
      TestExternalizable object2 = (TestExternalizable) xois.readObject();
      
      if (object2.getNum() != 299)
        throw new IOException("Object did not have correct num " + object2.getNum());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testSubExternal() {
    try {
      TestSubExternalizable object = new TestSubExternalizable(null);
      
      xoos.writeObject(object);
      flip();
      TestSubExternalizable object2 = (TestSubExternalizable) xois.readObject();
      
      if (object2.getNum() != 1000)
        throw new IOException("Object did not have correct num " + object2.getNum());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testPutFields() {
    try {
      TestPutFields object = new TestPutFields();
      
      xoos.writeObject(object);
      flip();
      TestPutFields object2 = (TestPutFields) xois.readObject();
      
      if ((object2.getNum() != 10001) || (object2.getNum2() != 99))
        throw new IOException("Object did not have correct num " + object2.getNum() + " " + object2.getNum2());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testUnreadData() {
    try {
      TestUnreadData object = new TestUnreadData();
      
      xoos.writeObject(object);
      flip();
      TestUnreadData object2 = (TestUnreadData) xois.readObject();
      
      if (object2.getNum() != object.getNum())
        throw new IOException("Object did not have correct num " + object2.getNum() + " " + object.getNum());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testWriteReplace() {
    try {
      TestReplace object = new TestReplace();
      
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      
      if (! (object2 instanceof TestReplace2))
        throw new IOException("Object did not have correct class " + object2.getClass());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testReadResolve() {
    try {
      TestResolve object = new TestResolve();
      
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      
      if (! (object2 instanceof TestResolve2))
        throw new IOException("Object did not have correct class " + object2.getClass());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testInheritedWriteReplace() {
    try {
      TestReplace3 object = new TestReplace3();
      
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      
      if (! (object2 instanceof TestReplace2))
        throw new IOException("Object did not have correct class " + object2.getClass());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testInheritedReadResolve() {
    try {
      TestResolve3 object = new TestResolve3();
      
      xoos.writeObject(object);
      flip();
      Object object2 = xois.readObject();
      
      if (! (object2 instanceof TestResolve2))
        throw new IOException("Object did not have correct class " + object2.getClass());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  protected void testSerialPersistentFields() {
    try {
      TestSerialPersistentFields object = new TestSerialPersistentFields();
      
      xoos.writeObject(object);
      flip();
      TestSerialPersistentFields object2 = (TestSerialPersistentFields) xois.readObject();
      
      if (! (object2.getNum1().equals(new Integer(1)) && (object2.getNum2() == null)))
        throw new IOException("Object did not have correct nums " + object2.getNum1() + " " + object2.getNum2());
      
      reset();
    } catch (IOException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("test produced exception " + e);
      e.printStackTrace();
    }
  }
  
  public void start() {
    testInt(20930);
    testInt(0);
    testInt(-29384);
    testBoolean(true);
    testBoolean(false);
    testByte((byte) 0);
    testByte((byte) 10);
    testByte((byte) 255);
    testChar('A');
    testChar('B');
    testChar('z');
    testChar('1');
    testChar('.');
    testChar('&');
    testChar('<');
    testChar('>');
    testChar(' ');
    testChar('"');
    testChar('\'');
    testDouble((double) 0);
    testDouble((double) -1029.2);
    testDouble((double) 17);
    testDouble((double) 182.29938);
    testFloat((float) 0);
    testFloat((float) 29.239);
    testFloat((float) 11.1029);
    testFloat((float) -1902.1);
    testLong(1920L);
    testLong(0L);
    testLong(192983783739892L);
    testLong(-1299L);
    testLong(-19282738339299L);
    testShort((short) 28);
    testShort((short) 1829);
    testShort((short) 0);
    testMultiplePrimitives();

    test(new Integer(5)); 
    test(new Long(2837L));
    test(new Vector());
    test("monkey");
    test("");
    test("blah blah balh\n blah blah ablh");
    test("blah blah balh\n\t\r\n\r\t\r blah blah ablh");
    test("<monkey>");
    test("<>&;'\"");
    
    testHashtable();
    testMultipleObjects();
    testUnserializableObject();
    testByteCustomSerializer();
    testCustomSerializer();
    testBrokenCustomSerializer();
    testSerialPersistentFields();
    
    testUnshared();
    testExternal();
    testSubExternal();
    
    testPutFields();
    testUnreadData();
    testWriteReplace();
    testReadResolve();
    testInheritedWriteReplace();
    testInheritedReadResolve();
    test(new byte[7847]);
    test(new byte[4][6]);
  }
  
  public static void main(String[] args) throws IOException {
    XMLObjectStreamUnit test = new XMLObjectStreamUnit();
    test.start();
  }
  
  public static class TestByteSerialization implements Serializable {
    private transient byte[] bytes = new byte[] {23, 19, 49, 0};
    
    public byte[] bytes() {
      return bytes;
    }
    
    private void readObject(ObjectInputStream oos) throws IOException, ClassNotFoundException {
      bytes = new byte[5];
      oos.read(bytes, 0, 5);
    }
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.write(bytes);
    }
    
  }
  
  public static class TestExternalizable implements Externalizable {
    
    protected int num = 199;
    
    public int getNum() {
      return num;
    }
    
    public void writeExternal(ObjectOutput o) throws IOException {
      o.writeInt(num + 100);
      o.writeInt(2000);
    }
    
    public void readExternal(ObjectInput i) throws IOException, ClassNotFoundException {
      num = i.readInt();
      i.readInt();
    }
  }
  
  public static class TestSubExternalizable extends TestExternalizable {
        
    public TestSubExternalizable(Object o) {
    }
    
    private TestSubExternalizable() {
    }
    
    public void writeExternal(ObjectOutput o) throws IOException {
      super.writeExternal(o);
      o.writeInt(1000);
    }
    
    public void readExternal(ObjectInput i) throws IOException, ClassNotFoundException {
      super.readExternal(i);
      num = i.readInt();
    }
    
    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      throw new IllegalArgumentException("READ OBJECT SHOULD NOT BE CALLED!");
    }
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
      throw new IllegalArgumentException("WRITE OBJECT SHOULD NOT BE CALLED!");
    }
  }
  
  public static class TestPutFields implements Serializable {
    
    int num = 0;
    
    Integer num2 = null;
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
      ObjectOutputStream.PutField pf = oos.putFields();
      pf.put("num", 10001);
      pf.put("num2", new Integer(99));
      pf.put("blah", 100);
      oos.writeFields();
    }
    
    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField gf = ois.readFields();
      num = gf.get("num", 0);
      num2 = (Integer) gf.get("num2", new Integer(0));
      gf.get("blah", 0);
      if (! gf.defaulted("monkey"))
        throw new IOException("Field monkey was not defaulted!");
      if (gf.defaulted("num"))
        throw new IOException("Field num was defaulted!");
      if (gf.defaulted("num2"))
        throw new IOException("Field num was defaulted!");
    }
    
    public int getNum() {
      return num;
    }
    
    public int getNum2() {
      return num2.intValue();
    }
    
  }
  
  public static class TestUnreadData implements Serializable {
    
    int num = 293;
        
    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.defaultWriteObject();
      oos.writeInt(10);
      oos.writeObject("niondco");
      oos.writeObject(new Vector());
    }
    
    public int getNum() {
      return num;
    }
  }
  
  public static class TestReplace implements Serializable {
    
    protected Object writeReplace() {
      return new TestReplace2();
    }

  }
  
  public static class TestReplace2 extends TestReplace {
  }
  
  public static class TestReplace3 extends TestReplace {
  }
  
  public static class TestResolve implements Serializable {
   
    protected Object readResolve() {
      return new TestResolve2();
    }
  }
  
  public static class TestResolve2 extends TestResolve {
  }
  
  public static class TestResolve3 extends TestResolve {
  }
  
  public static class TestSerialPersistentFields implements Serializable {
    
    private Integer num1 = new Integer(1);
    
    private Integer num2 = new Integer(2);
    
    private static final ObjectStreamField[] serialPersistentFields = {new ObjectStreamField("num1", Integer.class)};
    
    public Integer getNum1() {
      return num1;
    }
    
    public Integer getNum2() {
      return num2;
    }
  }

}
