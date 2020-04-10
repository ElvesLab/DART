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

import java.security.*;
import java.security.cert.*;
import java.security.interfaces.*;
import java.security.spec.*;
import java.util.*;
import java.util.zip.*;
import javax.crypto.*;
import javax.crypto.spec.*;

/**
 * This class contains a large number of static methods for performing
 * security-related primitives, such as encrypt, decrypt, etc...
 *
 * @version $Id: SecurityUtils.java 4167 2008-03-28 15:26:37Z jstewart $
 * @author amislove
 */
public class SecurityUtils {

  // ----- STATIC CONFIGURATION FIELDS -----

  /**
   * The name of the asymmetric cipher to use.
   */
  public final static String ASYMMETRIC_ALGORITHM = "RSA/ECB/OAEPPadding";
  public final static String DEPRECATED_ASYMMETRIC_ALGORITHM = "RSA";

  /**
   * The name of the symmetric cipher to use.
   */
  public final static String SYMMETRIC_ALGORITHM = "DES";

  /**
   * The name of the asymmetric generator to use.
   */
  public final static String ASYMMETRIC_GENERATOR = "RSA";

  /**
   * The name of the symmetric cipher to use.
   */
  public final static String SYMMETRIC_GENERATOR = "DES";

  /**
   * The name of the signature algorithm to use.
   */
  public final static String SIGNATURE_ALGORITHM = "SHA1withRSA";

  /**
   * The length of the symmetric keys
   */
  public final static int SYMMETRIC_KEY_LENGTH = 56;

  /**
   * The length of the symmetric keys
   */
  public final static int SYMMETRIC_IV_LENGTH = 64;
  
  /**
   * The name of the hash function.
   */
  public final static String HASH_ALGORITHM = "SHA1";
  
  /**
   * The name of the hmac function. 
   */
  public final static String HMAC_ALGORITHM = "MD5";
  
  /**
   * The name of the apop function. 
   */
  public final static String APOP_ALGORITHM = "MD5";
  
  /**
   * The length of hmac keys
   */
  public final static int HMAC_KEY_LENGTH = 64;
  
  /**
   * The ipad of hmac keys, as defined in RFC 2195
   */
  public final static byte HMAC_IPAD_BYTE = 0x36;
  
  /**
   * The opad of hmac keys, as defined in RFC 2195
   */
  public final static byte HMAC_OPAD_BYTE = 0x5C;
  
  /**
   * The ipad byte array for use in hmac
   */
  public final static byte[] HMAC_IPAD = new byte[HMAC_KEY_LENGTH];
  
  /**
   * The opad byte array for use in hmac
   */
  public final static byte[] HMAC_OPAD = new byte[HMAC_KEY_LENGTH];
  
  /**
   * Initialize the ipad/opad buffers
   */
  static {
    Arrays.fill(HMAC_IPAD, HMAC_IPAD_BYTE);
    Arrays.fill(HMAC_OPAD, HMAC_OPAD_BYTE);
  }

  // ----- STATIC CIPHER OBJECTS -----

  /**
   * The message digest used for doing hashing
   */
  private static MessageDigest hash;
  
  /**
   * The message digest used for doing apop
   */
  private static MessageDigest apop;
  
  /**
   * The message digest used for doing hmacing
   */
  private static MessageDigest hmac1;
  
  /**
   * The message digest used for doing hmacing
   */
  private static MessageDigest hmac2;
  
  /**
   * The cipher used to encrypt/decrypt data using DES
   */
  private static Cipher cipherSymmetric;

  /**
   * The cipher used to encrypt/decrypt data using RSA
   */
  private static Cipher cipherAsymmetric;
  private static Cipher deprecatedCipherAsymmetric;

  /**
   * The generator used to generate DES keys
   */
  private static KeyGenerator generatorSymmetric;

  /**
   * The generator used to generate RSA keys
   */
  private static KeyPairGenerator generatorAsymmetric;

  /**
   * The generator used to decode RSA keys
   */
  private static KeyFactory factoryAsymmetric;

  /**
   * The signature used for verification and signing data.
   */
  private static Signature signature;

  /**
   * The RNG for generating DES IVs
   */
  private static Random random;

  // ----- STATIC BLOCK TO INITIALIZE THE KEY GENERATORS -----

  static {
    // Add a provider for RSA encryption
    Security.insertProviderAt(new org.bouncycastle.jce.provider.BouncyCastleProvider(), 2);
    
    try {
      random = new Random();
      cipherSymmetric = Cipher.getInstance(SYMMETRIC_ALGORITHM);
      cipherAsymmetric = Cipher.getInstance(ASYMMETRIC_ALGORITHM, "BC");
      deprecatedCipherAsymmetric = Cipher.getInstance(DEPRECATED_ASYMMETRIC_ALGORITHM);
      generatorSymmetric = KeyGenerator.getInstance(SYMMETRIC_GENERATOR);
      generatorAsymmetric = KeyPairGenerator.getInstance(ASYMMETRIC_GENERATOR);
      factoryAsymmetric = KeyFactory.getInstance(ASYMMETRIC_GENERATOR);
      signature = Signature.getInstance(SIGNATURE_ALGORITHM);
      hash = MessageDigest.getInstance(HASH_ALGORITHM);
      apop = MessageDigest.getInstance(APOP_ALGORITHM);
      hmac1 = MessageDigest.getInstance(HMAC_ALGORITHM);
      hmac2 = MessageDigest.getInstance(HMAC_ALGORITHM);
      
      generatorSymmetric.init(SYMMETRIC_KEY_LENGTH);
    } catch (NoSuchAlgorithmException e) {
      throw new SecurityException("NoSuchAlgorithmException on construction: " + e);
    } catch (NoSuchPaddingException e) {
      throw new SecurityException("NoSuchPaddingException on construction: " + e);
    } catch (NoSuchProviderException e) {
      throw new SecurityException("NoSuchProviderException on construction: " + e);
    }
  }

  /**
   * Make the constructor private so no SecurityUtils is ever created.
   */
  private SecurityUtils() {
  }

  /**
   * Utility method for serializing an object to a byte[].
   *
   * @param o The object to serialize
   * @return The byte[] of the object
   * @exception IOException If serialization does not happen properly
   */
  public static byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(baos)));

    oos.writeObject(o);
    oos.flush();
    oos.close();
    
    return baos.toByteArray();
  }

  /**
   * Utility method for deserializing an object from a byte[]
   *
   * @param data The data to deserialize
   * @return The object
   * @exception IOException If deserialization does not happen properly
   * @exception ClassNotFoundException If the deserialized class is not found
   */
  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(bais)));

    return ois.readObject();
  }

  /**
   * Utility method for determining the hash of a byte[] using a secure hashing
   * algorithm.
   *
   * @param input The input
   * @return The hash value
   * @exception SecurityException If the hashing does not happen properly
   */
  public static byte[] hash(byte[] input) throws SecurityException {
    synchronized (hash) {
      return hash.digest(input);
    }
  }
  
  /**
   * Utility method for determining the apop of a challenge and password using a secure hashing
   * algorithm.
   *
   * @param password The password
   * @param challenge The challengr
   * @return The hash value
   * @exception SecurityException If the hashing does not happen properly
   */
  public static byte[] apop(byte[] challenge, byte[] password) throws SecurityException {
    synchronized (apop) {
      apop.update(challenge);
      apop.update(password);
      return apop.digest();
    }
  }
  
  /**
   * Utility method for determining the hmac of a byte[] and key using a secure hashing
   * algorithm.
   *
   * @param text The text
   * @param key The key
   * @return The hmac value
   * @exception SecurityException If the hmacing does not happen properly
   */
  public static byte[] hmac(byte[] key, byte[] text) throws SecurityException {
    synchronized (hmac1) {
      byte[] realKey = new byte[HMAC_KEY_LENGTH];
      System.arraycopy(key, 0, realKey, 0, (key.length < realKey.length ? key.length : realKey.length));
    
      hmac1.update(MathUtils.xor(realKey, HMAC_IPAD));
      hmac1.update(text);
      hmac2.update(MathUtils.xor(realKey, HMAC_OPAD));
      hmac2.update(hmac1.digest());
      return hmac2.digest();
    }
  }
  
  /**
   * Utility method for encrypting a block of data with symmetric encryption.
   *
   * @param data The data
   * @param key The key
   * @return The ciphertext
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key) throws SecurityException {
    return encryptSymmetric(data, key, new byte[SYMMETRIC_IV_LENGTH]);
  }
 
  /**
   * Utility method for encrypting a block of data with symmetric encryption.
   *
   * @param data The data
   * @param key The key
   * @param iv The initialization vector
   * @return The ciphertext
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key, byte[] iv) throws SecurityException {
    return encryptSymmetric(data, key, 0, data.length, iv);
  }

  /**
   * Utility method for encrypting a block of data with symmetric encryption.
   *
   * @param data The data
   * @param key The key
   * @param offset The offset into the data
   * @param length The length of data to write
   * @param iv The initialization vector
   * @return The ciphertext
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key, int offset, int length) throws SecurityException {
    return encryptSymmetric(data, key, offset, length, new byte[SYMMETRIC_IV_LENGTH]);
  }
  
  /**
   * Utility method for encrypting a block of data with symmetric encryption.
   *
   * @param data The data
   * @param key The key
   * @param offset The offset into the data
   * @param length The length of data to write
   * @param iv The initialization vector
   * @return The ciphertext
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptSymmetric(byte[] data, byte[] key, int offset, int length, byte[] iv) throws SecurityException {
    try {
      iv = correctLength(iv, SYMMETRIC_IV_LENGTH/8);
      IvParameterSpec ivSpec = new IvParameterSpec(iv);
      SecretKeySpec secretKey = new SecretKeySpec(key, SYMMETRIC_ALGORITHM);

      synchronized (cipherSymmetric) {
        cipherSymmetric.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);

        return cipherSymmetric.doFinal(data, offset, length);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException (" + iv.length + "," + key.length + ") encrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException encrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException encrypting object: " + e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new SecurityException("InvalidAlgorithmParameterException encrypting object: " + e);    
    }
  }

  /**
   * Utility method for decrypting some data with symmetric encryption.
   *
   * @param data The data to decrypt
   * @param key The key
   * @return The decrypted data
   * @exception SecurityException If the decryption does not happen properly
   */
  public static byte[] decryptSymmetric(byte[] data, byte[] key) throws SecurityException {
    return decryptSymmetric(data, key, new byte[SYMMETRIC_IV_LENGTH]);
  }
  
 /**
   * Utility method for decrypting some data with symmetric encryption.
   *
   * @param data The data to decrypt
   * @param key The key
   * @param iv The initialization vector
   * @return The decrypted data
   * @exception SecurityException If the decryption does not happen properly
   */
  public static byte[] decryptSymmetric(byte[] data, byte[] key, byte[] iv) throws SecurityException {
    try {
      iv = correctLength(iv, SYMMETRIC_IV_LENGTH/8);
      IvParameterSpec ivSpec = new IvParameterSpec(iv);
      SecretKeySpec secretKey = new SecretKeySpec(key, SYMMETRIC_ALGORITHM);

      synchronized (cipherSymmetric) {
        cipherSymmetric.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);
        
        return cipherSymmetric.doFinal(data);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException decrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException decrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException decrypting object: " + e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new SecurityException("InvalidAlgorithmParameterException decrypting object: " + e);    
    } 
  }

  /**
   * Utility method for signing a block of data with the a private key
   *
   * @param data The data
   * @param key The key to use to sign
   * @return The signature
   * @exception SecurityException If the signing does not happen properly
   */
  public static byte[] sign(byte[] data, PrivateKey key) throws SecurityException {
    try {
      synchronized (signature) {
        signature.initSign(key);
        signature.update(hash(data));

        return signature.sign();
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException signing object: " + e);
    } catch (SignatureException e) {
      throw new SecurityException("SignatureException signing object: " + e);
    }
  }

  /**
   * Utility method for verifying a signature
   *
   * @param data The data to verify
   * @param sig The proposed signature
   * @param key The key to verify against
   * @return Whether or not the sig matches.
   * @exception SecurityException If the verification does not happen properly
   */
  public static boolean verify(byte[] data, byte[] sig, PublicKey key) throws SecurityException {
    try {
      synchronized (signature) {
        signature.initVerify(key);
        signature.update(hash(data));

        return signature.verify(sig);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException verifying object: " + e);
    } catch (SignatureException e) {
      throw new SecurityException("SignatureException verifying object: " + e);
    }
  }

  /**
   * Encrypts the given byte[] using the provided public key. TO DO: Check
   * length of input
   *
   * @param data The data to encrypt
   * @param key The key to encrypt with
   * @return The encrypted data
   * @exception SecurityException If the encryption does not happen properly
   */
  public static byte[] encryptAsymmetric(byte[] data, PublicKey key) throws SecurityException {
    try {
      synchronized (cipherAsymmetric) {
        cipherAsymmetric.init(Cipher.ENCRYPT_MODE, key);

        return cipherAsymmetric.doFinal(data);
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException encrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException encrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException encrypting object: " + e);
    }
  }

  /**
   * Decrypts the given byte[] using the provided private key. TO DO: Check
   * length of input
   *
   * @param data The data to decrypt
   * @param key The private key to use
   * @return The decrypted data
   * @exception SecurityException If the decryption does not happen properly
   */
  public static byte[] decryptAsymmetric(byte[] data, PrivateKey key) throws SecurityException {
    try {
      try {
        // First try normal way of decrypting
        synchronized (cipherAsymmetric) {
          cipherAsymmetric.init(Cipher.DECRYPT_MODE, key);

          return cipherAsymmetric.doFinal(data);
        }
      } catch (BadPaddingException e) {
        // Trying deprecated way of decrypting
        synchronized (deprecatedCipherAsymmetric) {
          deprecatedCipherAsymmetric.init(Cipher.DECRYPT_MODE, key);
          
          return deprecatedCipherAsymmetric.doFinal(data);
        }
      }
    } catch (InvalidKeyException e) {
      throw new SecurityException("InvalidKeyException decrypting object: " + e);
    } catch (IllegalBlockSizeException e) {
      throw new SecurityException("IllegalBlockSizeException decrypting object: " + e);
    } catch (BadPaddingException e) {
      throw new SecurityException("BadPaddingException decrypting object: " + e);
    }
  }

  /**
   * Utility method which will generate a non-weak DES key for applications to
   * use.
   *
   * @return A new, random DES key
   */
  public static byte[] generateKeySymmetric() {
    synchronized (generatorSymmetric) {
      return generatorSymmetric.generateKey().getEncoded();
    }
  }

  /**
   * Utility method which will encode a public key
   *
   * @param key The key to encode
   * @return An encoded public key
   */
  public static byte[] encodePublicKey(PublicKey key) {
    RSAPublicKey rkey = (RSAPublicKey) key;

    byte[] modulus = rkey.getModulus().toByteArray();
    byte[] exponent = rkey.getPublicExponent().toByteArray();
    byte[] length = MathUtils.intToByteArray(modulus.length);

    byte[] result = new byte[length.length + modulus.length + exponent.length];
    System.arraycopy(length, 0, result, 0, length.length);
    System.arraycopy(modulus, 0, result, length.length, modulus.length);
    System.arraycopy(exponent, 0, result, length.length + modulus.length, exponent.length);

    return result;
  }

  /**
   * Utility method which will decode a previously encoded public key
   *
   * @param data The previously encoded key
   * @return The key
   */
  public static PublicKey decodePublicKey(byte[] data) throws SecurityException {
    byte[] len = new byte[4];
    System.arraycopy(data, 0, len, 0, len.length);
    int length = MathUtils.byteArrayToInt(len);

    byte[] modulus = new byte[length];
    System.arraycopy(data, len.length, modulus, 0, length);
    byte[] exponent = new byte[data.length - length - 4];
    System.arraycopy(data, len.length + length, exponent, 0, exponent.length);

    RSAPublicKeySpec rpks = new RSAPublicKeySpec(new BigInteger(modulus), new BigInteger(exponent));
    
    try {
      synchronized (factoryAsymmetric) {
        return factoryAsymmetric.generatePublic(rpks);
      }
    } catch (InvalidKeySpecException e) {
      throw new SecurityException("InvalidKeySpecException while decoding key: " + e);
    }
  }

  /**
   * Utility method which will encode a private key
   *
   * @param key The key to encode
   * @return An encoded public key
   */
  public static byte[] encodePrivateKey(PrivateKey key) {
    RSAPrivateKey rkey = (RSAPrivateKey) key;

    byte[] modulus = rkey.getModulus().toByteArray();
    byte[] exponent = rkey.getPrivateExponent().toByteArray();
    byte[] length = MathUtils.intToByteArray(modulus.length);

    byte[] result = new byte[length.length + modulus.length + exponent.length];
    System.arraycopy(length, 0, result, 0, length.length);
    System.arraycopy(modulus, 0, result, length.length, modulus.length);
    System.arraycopy(exponent, 0, result, length.length + modulus.length, exponent.length);

    return result;
  }

  /**
   * Utility method which will decode a previously encoded private key
   *
   * @param data The previously encoded key
   * @return The key
   */
  public static PrivateKey decodePrivateKey(byte[] data) throws SecurityException {
    byte[] len = new byte[4];
    System.arraycopy(data, 0, len, 0, len.length);
    int length = MathUtils.byteArrayToInt(len);

    byte[] modulus = new byte[length];
    System.arraycopy(data, len.length, modulus, 0, length);
    byte[] exponent = new byte[data.length - length - 4];
    System.arraycopy(data, len.length + length, exponent, 0, exponent.length);

    RSAPrivateKeySpec rpks = new RSAPrivateKeySpec(new BigInteger(modulus), new BigInteger(exponent));
    
    try {
      synchronized (factoryAsymmetric) {
        return factoryAsymmetric.generatePrivate(rpks);
      }
    } catch (InvalidKeySpecException e) {
      throw new SecurityException("InvalidKeySpecException while decoding key: " + e);
    }
  }

  /**
   * Utility method which will generate a random intialization vector for
   * applications to use.
   *
   * @return A new, random DES initialization vector
   */
  public static byte[] generateIVSymmetric() {
    byte[] result = new byte[SYMMETRIC_IV_LENGTH];
    random.nextBytes(result);
    
    return result;
  }

  /**
   * Utility method which will generate a non-weak DES key for applications to
   * use.
   *
   * @return A new, random DES key
   */
  public static KeyPair generateKeyAsymmetric() {
    synchronized (generatorAsymmetric) {
      return generatorAsymmetric.generateKeyPair();
    }
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
    byte[] result = new byte[length];

    for (int i=0; (i<data.length) && (i<result.length); i++) {
      result[i] = data[i];
    }

    return result;
  }
}
