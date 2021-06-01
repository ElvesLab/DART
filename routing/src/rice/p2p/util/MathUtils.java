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
import java.math.*;
import java.util.*;

import rice.environment.random.RandomSource;

/**
 * This class contains a large number of static methods for performing
 * math operations.
 *
 * @version $Id: MathUtils.java 4549 2008-10-16 18:21:19Z jeffh $
 * @author amislove
 */
public class MathUtils {
  
  /**
  * The array used for conversion to hexadecimal
   */
  public final static char[] HEX_ARRAY = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  
  /**
   * Make the constructor private so no MathUtils is ever created.
   */
  private MathUtils() {
  }
  
  /**
   * Utility which does *proper* modding, where the result is guaranteed to be
   * positive.
   *
   * @param a The modee
   * @param b The value to mod by
   * @return The result
   */
  public static int mod(int a, int b) {
    return ((a % b) + b) % b;
  }
  
  /**
   * Utility method which xors two given byte arrays, of equal length, and returns the results
   *
   * @param a The first array
   * @param b The second array
   * @return A new byte array, xored
   */
  public static byte[] xor(byte[] a, byte[] b) {
    byte[] result = new byte[a.length];
    
    for (int i=0; i<result.length; i++) 
      result[i] = (byte) (a[i] ^ b[i]);
    
    return result;
  }

  /**
   * Method which returns a specified number of random bytes
   *
   * @param len The number of random bytes to generate
   */
  public static byte[] randomBytes(int len, RandomSource random) {
    byte[] result = new byte[len];
    random.nextBytes(result);
    
    return result;
  }
  
  /**
   * Method which returns a random int
   *
   * @param len The number of random bytes to generate
   */
  public static int randomInt(RandomSource random) {
    return random.nextInt();
  }
  
  /**
   * Utility method which converts a byte[] to a hexadecimal string of characters, in lower case
   *
   * @param text The array to convert
   * @return A string representation
   */
  public static String toHex(byte[] text) {
    StringBuffer buffer = new StringBuffer();
    
    for (int i=0; i<text.length; i++) {
      buffer.append(HEX_ARRAY[0x0f & (text[i] >> 4)]);
      buffer.append(HEX_ARRAY[0x0f & text[i]]);
    }
    
    return buffer.toString(); 
  }
  
  /**
   * Utility method which converts a hex string to a byte[]
   *
   * @param text The text to convert
   * @return The bytes
   */
  public static byte[] fromHex(String text) {
    byte[] result = new byte[text.length() / 2];
    
    for (int i=0; i<result.length; i++) 
      result[i] = (byte) ((byte) ((byte) 0xf0 & ((getByte(text.charAt(2*i))) << 4)) | getByte(text.charAt(2*i + 1)));
    
    return result;
  }
  
  /**
   * Utility method which converts a byte[] to a base64 string of characters, in lower case
   *
   * @param text The array to convert
   * @return A string representation
   */
  public static String toBase64(byte[] text) {
    return Base64.encodeBytes(text);
  }
  
  /**
   * Utility method which converts a base64 string to a byte[]
   *
   * @param text The text to convert
   * @return The bytes
   */
  public static byte[] fromBase64(String text) {
    return Base64.decode(text);
  }
  
  /**
   * A simple and fast hash function for hashing an arbitrary length
   * byte array into an int.  Not for crypto.  Not to be trusted.  
   * 
   * From Wikipedia: http://en.wikipedia.org/wiki/Hash_table
   * 
   * @param b
   * @return A hash of b
   */
  public static int simpleHash(byte[] b) {
    int hash = 0;
      
    for (int i = 0; i < b.length; i++) {
      hash += b[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
  }

  /**  
   * Utility method for converting a char to a byte
   *
   * @param c The char
   * @return The byte
   */  
  protected static byte getByte(char c) {
    if ((c >= '0') && (c <= '9'))
      return (byte) (c - '0');
    else if ((c >= 'A') && (c <= 'F'))
      return (byte) (10 + (byte) (c - 'A'));
    else if ((c >= 'a') && (c <= 'f'))
      return (byte) (10 + (byte) (c - 'a'));
    else
      throw new RuntimeException("Could not decode hex character '" + c + "'");
  }
  
  /**
   * Utility method for converting a int into a byte[]
   *
   * @param input The log to convert
   * @return a byte[] representation
   */
  public static byte[] intToByteArray(int input) {
    byte[] output = new byte[4];
    intToByteArray(input, output, 0);
    return output;
  }
  
  /**
   * Utility method for converting a int into a byte[]
   *
   * @param input The log to convert
   * @return a byte[] representation
   */
  public static void intToByteArray(int input, byte[] output, int offset) {
    output[offset + 0] = (byte) (0xFF & (input >> 24));
    output[offset + 1] = (byte) (0xFF & (input >> 16));
    output[offset + 2] = (byte) (0xFF & (input >> 8));
    output[offset + 3] = (byte) (0xFF & input);
  }
  
  /**
   * Utility method for converting a byte[] into a int
   *
   * @param input The byte[] to convert
   * @return a int representation
   */
  public static int byteArrayToInt(byte[] input) {
    return byteArrayToInt(input,0);
  }
  
  public static int byteArrayToInt(byte[] input, int offset) {
    input = correctLength(input, offset+4);

    int result;
    result  = (input[offset+0] & 0xFF) << 24;
    result |= (input[offset+1] & 0xFF) << 16;
    result |= (input[offset+2] & 0xFF) << 8;
    result |= (input[offset+3] & 0xFF);

    return result;
  }

  public static short byteArrayToShort(byte[] input) {
    return byteArrayToShort(input,0);
  }
  
  public static short byteArrayToShort(byte[] input, int offset) {
    input = correctLength(input, offset+2);

    short result;
    result  = (short)((input[offset+0] & 0xFF) << 8);
    result |= (short)((input[offset+1] & 0xFF));

    return result;
  }


  /**
   * Utility method for converting a long into a byte[]
   *
   * @param input The log to convert
   * @return a byte[] representation
   */
  public static byte[] longToByteArray(long input) {
    byte[] output = new byte[8];
    longToByteArray(input, output, 0);
    return output;
  }
  
  /**
   * Utility method for converting a long into a byte[]
   *
   * @param input The log to convert
   * @return a byte[] representation
   */
  public static void longToByteArray(long input, byte[] output, int offset) {    
    output[offset + 0] = (byte) (0xFF & (input >> 56));
    output[offset + 1] = (byte) (0xFF & (input >> 48));
    output[offset + 2] = (byte) (0xFF & (input >> 40));
    output[offset + 3] = (byte) (0xFF & (input >> 32));
    output[offset + 4] = (byte) (0xFF & (input >> 24));
    output[offset + 5] = (byte) (0xFF & (input >> 16));
    output[offset + 6] = (byte) (0xFF & (input >> 8));
    output[offset + 7] = (byte) (0xFF & input);
  }

  /**
   * Utility method for converting a byte[] into a long
   *
   * @param input The byte[] to convert
   * @return a long representation
   */
  public static long byteArrayToLong(byte[] input) {
    return byteArrayToLong(input, 0);
  }
  public static long byteArrayToLong(byte[] input, int offset) {

    input = correctLength(input, offset+8);
 
    long result;
    result  = ((long)(input[offset+0] & 0xFF)) << 56;
    result |= ((long)(input[offset+1] & 0xFF)) << 48;
    result |= ((long)(input[offset+2] & 0xFF)) << 40;
    result |= ((long)(input[offset+3] & 0xFF)) << 32;
    result |= ((long)(input[offset+4] & 0xFF)) << 24;
    result |= ((long)(input[offset+5] & 0xFF)) << 16;
    result |= ((long)(input[offset+6] & 0xFF)) << 8;
    result |= ((long)(input[offset+7] & 0xFF));

    return result;
  }

  /**
   * Utility method for ensuring the array is of the proper length.  THis
   * method enforces the length by appending 0's or returning a subset of
   * the input array.
   *
   * @param data The input array
   * @param length The length the array should be
   * @return A correct-length array
   */
  private static byte[] correctLength(byte[] data, int length) {
    return correctLength(data, 0, length);
  }
  
  private static byte[] correctLength(byte[] data, int offset, int length) {
    if (data.length >= length)
      return data;
    
    byte[] result = new byte[length];

    for (int i=0; (i<data.length) && (i<result.length); i++) 
      result[i] = data[i];

    return result;
  }
  
  public static int uByteToInt(byte b) {
    return (int) b & 0xFF;
  }
  
  public static int uShortToInt(short b) {
    return (int) b & 0xFFFF;
  }
  
  public static byte intToUByte(int i) {
    if ((i > 255) || (i < 0)) throw new RuntimeException("can't convert "+i+" to an unsigned byte");
    return (byte)i;
  }  
}
