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
package rice.p2p.glacier;

import java.io.*;
import java.util.Arrays;

import rice.environment.Environment;
import rice.environment.logging.*;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.past.PastContent;
import rice.p2p.past.rawserialization.*;
import rice.p2p.util.rawserialization.*;

/**
 * DESCRIBE THE CLASS
 *
 * @version $Id: ErasureCodec.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author ahae
 */
public class ErasureCodec {

  protected int numFragments;
  protected int numSurvivors;

  final static int Lfield = 10;
  final static int MultField = (1 << Lfield) - 1;

  static int[] ExpToFieldElt;
  static int[] FieldEltToExp;
  static boolean isEltInitialized = false;

  protected Environment environment;
  protected Logger logger;
  
  /**
   * Constructor for ErasureCodec.
   *
   * @param _numFragments DESCRIBE THE PARAMETER
   * @param _numSurvivors DESCRIBE THE PARAMETER
   */
  public ErasureCodec(int _numFragments, int _numSurvivors, Environment env) {
    environment = env;
    logger = environment.getLogManager().getLogger(getClass(), null);
    numFragments = _numFragments;
    numSurvivors = _numSurvivors;

    if (!isEltInitialized)
      initElt();
  }

  public void dump(byte[] data) {
    String hex = "0123456789ABCDEF";
    String s = "";
    for (int i=0; i<data.length; i++) {
      int d = data[i];
      if (d<0)
        d+= 256;
      int hi = (d>>4);
      int lo = (d&15);
        
      s+=hex.charAt(hi)+""+hex.charAt(lo);
      if (((i%16)==15) || (i==(data.length-1)))
        s+="\n";
      else
        s+=" ";
    }
    if (logger.level <= Logger.INFO) logger.log(
        s);
  }
  public Fragment[] encodeObject(PastContent obj, boolean[] generateFragment) {
    return encodeObject(obj instanceof RawPastContent ? (RawPastContent)obj : new JavaSerializedPastContent(obj), generateFragment);
  }

  /**
   * Serializes the object.
   * 
   * @param obj
   * @param generateFragment
   * @return
   */
  public Fragment[] encodeObject(RawPastContent obj, boolean[] generateFragment) {
    if (logger.level <= Logger.FINER) logger.log( 
        "Serialize object: " + obj);

    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      sob.writeShort(obj.getType());
      obj.serialize(sob);
      return encode(sob.getBytes(), sob.getWritten(), generateFragment); 
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log( 
          "Cannot serialize object: "+ioe);
      return null;
    }
  }

  
//  public Fragment[] encodeObject(PastContent obj, boolean[] generateFragment) {
//    byte bytes[];
//
//    try {
//      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
//      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
//
//      objectStream.writeObject(obj);
//      objectStream.flush();
//
//      bytes = byteStream.toByteArray();
//    } catch (IOException ioe) {
//      if (logger.level <= Logger.WARNING) logger.logException(
//          "encodeObject: ", ioe);
//      return null;
//    }
//
//    return encode(bytes, generateFragment);
//  }

  /**
   * Input: buffer of size <numFragments*Lfield>; first <numSurvivors*Lfield> words contain message, rest is zeroes
   * Output: buffer contains fragments
   */
  protected void encodeChunk(int[] buffer) {
    for (int row = 0; row < (numFragments - numSurvivors); row++) {
      for (int col = 0; col < numSurvivors; col++) {
        int exponent = (MultField - FieldEltToExp[(row ^ col) ^ (1 << (Lfield - 1))]) % MultField;
        for (int row_bit = 0; row_bit < Lfield; row_bit ++) 
          for (int col_bit = 0; col_bit < Lfield; col_bit ++) 
            if ((ExpToFieldElt[exponent + row_bit] & (1 << col_bit)) != 0) 
              buffer[numSurvivors * Lfield + row * Lfield + row_bit] ^= buffer[col_bit + col * Lfield];
      }
    }
  }

  /**
   * Generates multiple fragments from the incoming bytes
   * @param bytes
   * @param length
   * @param generateFragment
   * @return
   */
  public Fragment[] encode(byte[] bytes, int length, boolean[] generateFragment) {
    int wantFragments = 0;
    for (int i=0; i<generateFragment.length; i++)
      if (generateFragment[i])
        wantFragments ++;
        
    // System.outt.println(Systemm.currentTimeMillis()+" XXX before encode("+bytes.length+" bytes, "+wantFragments+" fragments) free="+Runtime.getRuntime().freeMemory()+" total="+Runtime.getRuntime().totalMemory());
    
    int numWords = (length + 3) / 4;
    int wordsPerGroup = (numSurvivors * Lfield);
    int numGroups = (numWords + (wordsPerGroup - 1)) / wordsPerGroup;
    int wordsPerFragment = numGroups * Lfield;
    int buffer[] = new int[numFragments * Lfield];
    Fragment frag[] = new Fragment[numFragments];

    for (int i = 0; i < numFragments; i++) {
      if (generateFragment[i])
        frag[i] = new Fragment(wordsPerFragment * 4);
      else
        frag[i] = null;
    }

    //System.outt.println(bytes.length+" bytes => "+numFragments+" fragments with "+wordsPerFragment+" words ("+numGroups+" groups)");
    
    for (int g=0; g<numGroups; g++) {
      int offset = g * wordsPerGroup * 4;
      int wordsHere = Math.min((length - offset + 3)/4, wordsPerGroup);

      Arrays.fill(buffer, 0);
      for (int i=0; i<wordsHere; i++) {
        byte b0 = (offset+4*i+0 < length) ? bytes[offset+4*i+0] : 0;
        byte b1 = (offset+4*i+1 < length) ? bytes[offset+4*i+1] : 0;
        byte b2 = (offset+4*i+2 < length) ? bytes[offset+4*i+2] : 0;
        byte b3 = (offset+4*i+3 < length) ? bytes[offset+4*i+3] : 0;
        buffer[i] = (b0<<24) | ((b1<<16)&0xFF0000) | ((b2<<8)&0xFF00) | (b3&0xFF);
      }
      
      encodeChunk(buffer);
      
      for (int i=0; i<numFragments; i++) {
        if (generateFragment[i]) {
          for (int j=0; j<Lfield; j++) {
            frag[i].payload[4*(g*Lfield + j) + 0] = (byte) ((buffer[i * Lfield + j]      ) & 0xFF);
            frag[i].payload[4*(g*Lfield + j) + 1] = (byte) ((buffer[i * Lfield + j] >>  8) & 0xFF);
            frag[i].payload[4*(g*Lfield + j) + 2] = (byte) ((buffer[i * Lfield + j] >> 16) & 0xFF);
            frag[i].payload[4*(g*Lfield + j) + 3] = (byte) ((buffer[i * Lfield + j] >> 24) & 0xFF);
          }
        }
      }
    }

    // System.out.println(Systemm.currentTimeMillis()+" XXX after encode("+bytes.length+" bytes, "+wantFragments+" fragments) free="+Runtime.getRuntime().freeMemory()+" total="+Runtime.getRuntime().totalMemory());

    return frag;
  }

  protected void decodeChunk(int[] buffer, int nExtra, int[] RowInd, boolean[] haveFragment, long[][] InvMat, int[] ColInd) {

    // *** Second last step ***
  
    int M[] = new int[(numFragments - numSurvivors) * Lfield];
    for (int i = 0; i < nExtra; i++)
      for (int j = 0; j < Lfield; j++)
        M[i * Lfield + j] = buffer[(RowInd[i] + numSurvivors) * Lfield + j];

    for (int row = 0; row < nExtra; row++) {
      for (int col = 0; col < numSurvivors; col++) {
        if (haveFragment[col]) {
          int exponent = (MultField - FieldEltToExp[RowInd[row] ^ col ^ (1 << (Lfield - 1))]) % MultField;
          for (int row_bit = 0; row_bit < Lfield; row_bit++) 
            for (int col_bit = 0; col_bit < Lfield; col_bit++) 
              if ((ExpToFieldElt[exponent + row_bit] & (1 << col_bit)) != 0)
                M[row_bit + row * Lfield] ^= buffer[col_bit + col * Lfield];
        }
      }
    }

    // *** Last step ***

    for (int row = 0; row < nExtra; row++) {
      for (int col = 0; col < nExtra; col++) {
        int exponent = (int) InvMat[row][col];
        for (int row_bit = 0; row_bit < Lfield; row_bit++)
          for (int col_bit = 0; col_bit < Lfield; col_bit++) 
            if ((ExpToFieldElt[exponent + row_bit] & (1 << col_bit)) != 0) 
              buffer[row_bit + ColInd[row] * Lfield] ^= M[col_bit + col * Lfield];
      }
    }
  }

  public PastContent decode(Fragment frag[], Endpoint endpoint, PastContentDeserializer pcd) {

    Fragment firstFrag = null;
    for (int i=0; i<frag.length; i++)
      if (frag[i] != null)
        firstFrag = frag[i];

    // System.out.println(Systemm.currentTimeMillis()+" XXX before decode("+firstFrag.getPayload().length+" bytes per fragment) free="+Runtime.getRuntime().freeMemory()+" total="+Runtime.getRuntime().totalMemory());

    if (frag.length != numFragments)
      return null;
  
    int firstFragment = -1;
    for (int i=0; (i<numFragments) && (firstFragment == -1); i++)
      if (frag[i] != null)
        firstFragment = i;
  
    int wordsPerFragment = (frag[firstFragment].payload.length + 3) / 4;
    int numGroups = wordsPerFragment / Lfield;

    boolean haveFragment[] = new boolean[numFragments];
    Arrays.fill(haveFragment, false);

    for (int i = 0; i < numFragments; i++)
      if (frag[i] != null)
        haveFragment[i] = true;

    int ColInd[] = new int[numSurvivors];
    int RowInd[] = new int[numFragments - numSurvivors];

    int nMissing = 0;

    int nExtra = 0;
    for (int i = 0; i < numSurvivors; i++) 
      if (!haveFragment[i]) 
        ColInd[nMissing++] = i;

    for (int i = 0; i < (numFragments - numSurvivors); i++) 
      if (haveFragment[numSurvivors + i])
        RowInd[nExtra++] = i;

    if (nMissing > nExtra)
      return null;

    if (nMissing < nExtra)
      nExtra = nMissing;


    int[] C, D, E, F;
    C = new int[numFragments - numSurvivors];
    Arrays.fill(C, 0);
    D = new int[numFragments - numSurvivors];
    Arrays.fill(D, 0);
    E = new int[numFragments - numSurvivors];
    Arrays.fill(E, 0);
    F = new int[numFragments - numSurvivors];
    Arrays.fill(F, 0);

    for (int row = 0; row < nExtra; row++) {
      for (int col = 0; col < nExtra; col++) {
        if (col != row) {
          C[row] += FieldEltToExp[RowInd[row] ^ RowInd[col]];
          D[row] += FieldEltToExp[ColInd[row] ^ ColInd[col]];
        }

        E[row] += FieldEltToExp[RowInd[row] ^ ColInd[col] ^ (1 << (Lfield - 1))];
        F[col] += FieldEltToExp[RowInd[row] ^ ColInd[col] ^ (1 << (Lfield - 1))];
      }
    }

    long InvMat[][] = new long[nExtra][nExtra];

    for (int row = 0; row < nExtra; row++) {
      for (int col = 0; col < nExtra; col++) {
        InvMat[row][col] = E[col] + F[row] - C[col] - D[row] -
          FieldEltToExp[RowInd[col] ^ ColInd[row] ^ (1 << (Lfield - 1))];
        if (InvMat[row][col] >= 0) {
          InvMat[row][col] = InvMat[row][col] % MultField;
        } else {
          InvMat[row][col] = (MultField - (-InvMat[row][col] % MultField)) % MultField;
        }
      }
    }

    // *** Second last step ***

    byte[] bytes = new byte[numSurvivors * wordsPerFragment * 4];
    int[] buffer = new int[numFragments * Lfield];
    for (int g=0; g<numGroups; g++) {
      Arrays.fill(buffer, 0);
      for (int i=0; i<numFragments; i++) {
        if (haveFragment[i]) {
          for (int j=0; j < Lfield; j++) {
            buffer[i*Lfield + j] = 
              ((frag[i].payload[4*(g*Lfield + j) + 0]      ) & 0x0000FF) +
              ((frag[i].payload[4*(g*Lfield + j) + 1] <<  8) & 0x00FF00) +
              ((frag[i].payload[4*(g*Lfield + j) + 2] << 16) & 0xFF0000) +
               (frag[i].payload[4*(g*Lfield + j) + 3] << 24);
          }
        }
      }
      
      decodeChunk(buffer, nExtra, RowInd, haveFragment, InvMat, ColInd);
      
      for (int i=0; i<(numSurvivors*Lfield); i++) {
        bytes[4*(g*(numSurvivors*Lfield) + i) + 0] = (byte)  (buffer[i] >> 24);
        bytes[4*(g*(numSurvivors*Lfield) + i) + 1] = (byte) ((buffer[i] >> 16) & 0xFF);
        bytes[4*(g*(numSurvivors*Lfield) + i) + 2] = (byte) ((buffer[i] >>  8) & 0xFF);
        bytes[4*(g*(numSurvivors*Lfield) + i) + 3] = (byte) ((buffer[i]      ) & 0xFF);
      }
    }

    try {
      return deserialize(bytes, endpoint, pcd);
//      SimpleInputBuffer sib = new SimpleInputBuffer(bytes);
//      short type = sib.readShort();
//      return pcd.deserializePastContent(sib, endpoint, type);
      
//      ByteArrayInputStream byteinput = new ByteArrayInputStream(bytes);
//      ObjectInputStream objectInput = new ObjectInputStream(byteinput);
//      // System.out.println(Systemm.currentTimeMillis()+" XXX after decode("+firstFrag.getPayload().length+" bytes per fragment) free="+Runtime.getRuntime().freeMemory()+" total="+Runtime.getRuntime().totalMemory());
//      return (Serializable) objectInput.readObject();
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException(
          "", ioe);
    } catch (ClassNotFoundException cnfe) {
      if (logger.level <= Logger.WARNING) logger.logException(
          "", cnfe);
    } catch (IllegalStateException ise) {
      if (logger.level <= Logger.WARNING) logger.logException(
          "", ise);
    }

    return null;
  }
  
  protected PastContent deserialize(byte[] bytes, Endpoint endpoint, PastContentDeserializer pcd) throws IOException, ClassNotFoundException {
//  ByteArrayInputStream byteinput = new ByteArrayInputStream(bytes);
//  ObjectInputStream objectInput = new ObjectInputStream(byteinput);
//  // System.out.println(Systemm.currentTimeMillis()+" XXX after decode("+firstFrag.getPayload().length+" bytes per fragment) free="+Runtime.getRuntime().freeMemory()+" total="+Runtime.getRuntime().totalMemory());
//  return (Serializable) objectInput.readObject();
    
    SimpleInputBuffer sib = new SimpleInputBuffer(bytes);
    short type = sib.readShort();
    return pcd.deserializePastContent(sib, endpoint, type);    
  }

  protected void initElt() {
    final int polymask[] = {
      0x0000, 0x0003, 0x0007, 0x000B, 0x0013, 0x0025, 0x0043, 0x0083,
      0x011D, 0x0211, 0x0409, 0x0805, 0x1053, 0x201B, 0x402B, 0x8003
      };

    ExpToFieldElt = new int[MultField + Lfield];

    ExpToFieldElt[0] = 1;
    for (int i = 1; i < MultField + Lfield - 1; i++) {
      ExpToFieldElt[i] = ExpToFieldElt[i - 1] << 1;
      if ((ExpToFieldElt[i] & (1 << Lfield)) != 0) {
        ExpToFieldElt[i] ^= polymask[Lfield];
      }
    }

    /*
     *  This is the inverter for the previous field. Note that 0
     *  is not invertible!
     */
    FieldEltToExp = new int[MultField + 1];

    FieldEltToExp[0] = -1;
    for (int i = 0; i < MultField; i++) {
      FieldEltToExp[ExpToFieldElt[i]] = i;
    }
  }

//  public static void main(String args[]) {
//    Environment env = new Environment();
//    RandomSource rng = env.getRandomSource();
//    ErasureCodec codec = new ErasureCodec(48, 5, env);
//    Serializable s = new String("Habe Mut, Dich Deines eigenen Verstandes zu bedienen! Aufklaerung ist der Ausgang aus Deiner selbstverschuldeten Unmuendigkeit!");
//
//    System.out.println("Encoding...");
//    
//    boolean generateFragment[] = new boolean[48];
//    Arrays.fill(generateFragment, true);
//    
//    Fragment frag[] = codec.encodeObject(s, generateFragment);
//
//    Fragment frag2[] = new Fragment[48];
//    frag2[9] = frag[9];
//    frag2[11] = frag[11];
//    frag2[12] = frag[12];
//    frag2[17] = frag[17];
//    frag2[33] = frag[33];
//
//    System.out.println("Decoding...");
//    Object d = codec.decode(frag2);
//
//    System.out.println(d);
//    
//    for (int i=0; i<100; i++) {
//      int[] theObject = new int[rng.nextInt(100000)];
//      for (int j=0; j<theObject.length; j++)
//        theObject[j] = rng.nextInt(2000000000);
//      
//      System.out.println("#"+i+": "+(theObject.length*4)+" bytes");
//        
//      int fid[] = new int[5];
//      for (int j=0; j<5; j++) {
//        boolean again = true;
//        while (again) {
//          fid[j] = rng.nextInt(48);
//          again = false;
//          for (int k=0; k<j; k++)
//            if (fid[k] == fid[j])
//              again = true;
//        }
//      }
//      
//      generateFragment = new boolean[48];
//      for (int j=0; j<48; j++)
//        generateFragment[j] = false;
//      for (int j=0; j<5; j++)
//        generateFragment[fid[j]] = true;
//      
//      Fragment xf[] = codec.encodeObject(theObject, generateFragment);
//      
//      Fragment yf[] = new Fragment[48];
//      for (int j=0; j<5; j++)
//        yf[fid[j]] = xf[fid[j]];
//      
//      int[] decodedObject = (int[]) codec.decode(yf);
//
//      for (int j=0; j<theObject.length; j++) {
//        if (decodedObject[j] != theObject[j]) {
//          System.err.println("FAIL: Run #"+i+" offset "+j+" decoded="+decodedObject[j]+" original="+theObject[j]);
//          System.exit(1);
//        }
//      }
//    }
//    
//    System.out.println("PASS");
//  }
}
