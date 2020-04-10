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

import rice.p2p.util.*;
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

public class SecurityUtilsUnit {

  /**
   * Tests the security service.
   *
   * @param argv The command line arguments
   * @exception NoSuchAlgorithmException If the encryption does not happen
   *      properly
   * @exception IOException If the encryption does not happen properly
   * @exception ClassNotFoundException If the encryption does not happen
   *      properly
   */
  public static void main(String[] argv) throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
    System.out.println("SecurityUtils Test Suite");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Initializing Tests");
    System.out.print("    Generating key pairs\t\t\t\t");

    KeyPair pair = SecurityUtils.generateKeyAsymmetric();
    KeyPair pair2 = SecurityUtils.generateKeyAsymmetric();
    System.out.println("[ DONE ]");

    System.out.print("    Building cipher\t\t\t\t\t");

    System.out.println("[ DONE ]");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Running Tests");

    System.out.print("    Testing serialization\t\t\t\t");
    String testString = "test";
    byte[] testStringByte = SecurityUtils.serialize(testString);
    String testStringOutput = (String) SecurityUtils.deserialize(testStringByte);

    if (testStringOutput.equals(testString)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Output:\t" + testStringOutput);
    }

    System.out.print("    Testing hashing\t\t\t\t\t");
    byte[] testStringHash = SecurityUtils.hash(testStringByte);

    if ((testStringHash != null) && (testStringHash.length == 20)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Output:\t" + testStringHash);

      if (testStringHash != null) {
        System.out.println("    Length:\t" + testStringHash.length);
      }
    }

    System.out.print("    Testing symmetric encryption\t\t\t");

    byte[] key = SecurityUtils.generateKeySymmetric();
    byte[] testStringCipherText = SecurityUtils.encryptSymmetric(testStringByte, key);
    byte[] testStringPlainText = SecurityUtils.decryptSymmetric(testStringCipherText, key);

    if (Arrays.equals(testStringByte, testStringPlainText)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Cipher Len:\t" + testStringCipherText.length);
      System.out.println("    Output Len:\t" + testStringPlainText.length);
    }

    System.out.print("    Testing signing and verification (phase 1)\t\t");

    byte[] testStringSig = SecurityUtils.sign(testStringByte, pair.getPrivate());

    if (SecurityUtils.verify(testStringByte, testStringSig, pair.getPublic())) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Sig Len:\t" + testStringSig.length);
    }

    System.out.print("    Testing signing and verification (phase 2)\t\t");

    testStringSig[0]++;

    if (! SecurityUtils.verify(testStringByte, testStringSig, pair.getPublic())) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Sig Len:\t" + testStringSig.length);
    }

    System.out.print("    Testing asymmetric encryption\t\t\t");

    byte[] testStringEncrypted = SecurityUtils.encryptAsymmetric(testStringByte, pair.getPublic());
    byte[] testStringDecrypted = SecurityUtils.decryptAsymmetric(testStringEncrypted, pair.getPrivate());
    
    if (Arrays.equals(testStringByte, testStringDecrypted)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Enc Len:\t" + testStringEncrypted.length);
      System.out.println("    Dec Len:\t" + testStringDecrypted.length);
    }
    
    System.out.print("    Testing hmac algorithm\t\t\t\t");
    
    String hmacText = "<1896.697170952@postoffice.reston.mci.net>";
    String hmacKey = "tanstaaftanstaaf";
    byte[] hmac = SecurityUtils.hmac(hmacKey.getBytes(), hmacText.getBytes());
    byte[] hmacResult = new byte[] {(byte) 0xb9, (byte) 0x13, (byte) 0xa6, (byte) 0x02, 
      (byte) 0xc7, (byte) 0xed, (byte) 0xa7, (byte) 0xa4, 
      (byte) 0x95, (byte) 0xb4, (byte) 0xe6, (byte) 0xe7, 
      (byte) 0x33, (byte) 0x4d, (byte) 0x38, (byte) 0x90};
    
    if (Arrays.equals(hmac, hmacResult)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + hmacText);
      System.out.println("    Key: \t" + hmacKey);
      System.out.println("    Res Len:\t" + hmac.length);
      System.out.println("    Real Len:\t" + hmacResult.length);
    }
    
    System.out.print("    Testing hmac algorithm again\t\t\t");
    
    String hmacText2 = "<1080369447214@The-Edge.local>";
    String hmacKey2 = "monkey";
    byte[] hmac2 = SecurityUtils.hmac(hmacKey2.getBytes(), hmacText2.getBytes());
    byte[] hmacResult2 = new byte[] {(byte) 0x9b, (byte) 0xae, (byte) 0x52, (byte) 0xef, 
      (byte) 0x55, (byte) 0x45, (byte) 0x24, (byte) 0x91, 
      (byte) 0x36, (byte) 0x85, (byte) 0x74, (byte) 0x72, 
      (byte) 0x21, (byte) 0xbb, (byte) 0x84, (byte) 0x22};
    
    if (Arrays.equals(hmac2, hmacResult2)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + hmacText2);
      System.out.println("    Key: \t" + hmacKey2);
      System.out.println("    Res Len:\t" + hmac2.length);
      System.out.println("    Real Len:\t" + hmacResult2.length);
    }
    
    System.out.print("    Testing asymmetic symmetric key encryption\t\t");

    byte[] keySym = SecurityUtils.generateKeySymmetric();
      
    if (Arrays.equals(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic()), 
                      SecurityUtils.encryptAsymmetric(keySym, pair2.getPublic()))) {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + MathUtils.toHex(keySym));
      System.out.println("    Output 1: \t" + MathUtils.toHex(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic())));
      System.out.println("    Output 2: \t" + MathUtils.toHex(SecurityUtils.encryptAsymmetric(keySym, pair2.getPublic())));
    } else {
      System.out.println("[ PASSED ]");
    }

    System.out.print("    Testing asymmetic public writing\t\t\t");

    PublicKey k = SecurityUtils.decodePublicKey(SecurityUtils.encodePublicKey(pair.getPublic()));

    if (! Arrays.equals(k.getEncoded(), pair.getPublic().getEncoded())) {
      System.out.println("[ FAILED ]");
      System.out.println("  Output 1: \t" + MathUtils.toHex(SecurityUtils.encryptAsymmetric(keySym, k)));
      System.out.println("     Res 1: \t" + MathUtils.toHex(SecurityUtils.decryptAsymmetric(SecurityUtils.encryptAsymmetric(keySym, k), pair.getPrivate())));
      System.out.println("  Output 2: \t" + MathUtils.toHex(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic())));
      System.out.println("     Res 2: \t" + MathUtils.toHex(SecurityUtils.decryptAsymmetric(SecurityUtils.encryptAsymmetric(keySym, pair.getPublic()), pair.getPrivate())));
    } else {
      System.out.println("[ PASSED ]");
    }

    System.out.print("    Testing asymmetic private writing\t\t\t");

    PrivateKey k2 = SecurityUtils.decodePrivateKey(SecurityUtils.encodePrivateKey(pair.getPrivate()));

    if (! Arrays.equals(SecurityUtils.sign(keySym, k2), SecurityUtils.sign(keySym, pair.getPrivate()))) {
      System.out.println("[ FAILED ]");
      System.out.println("  Output 1: \t" + MathUtils.toHex(k2.getEncoded()));
      System.out.println("  Output 2: \t" + MathUtils.toHex(pair.getPrivate().getEncoded()));
    } else {
      System.out.println("[ PASSED ]");
    }

    System.out.print("    Testing asymmetic serialized writing\t\t");

    testStringEncrypted = SecurityUtils.encryptAsymmetric(testStringByte, k);
    testStringDecrypted = SecurityUtils.decryptAsymmetric(testStringEncrypted, pair.getPrivate());
    
    if (Arrays.equals(testStringByte, testStringDecrypted)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testString);
      System.out.println("    Length:\t" + testStringByte.length);
      System.out.println("    Enc Len:\t" + testStringEncrypted.length);
      System.out.println("    Dec Len:\t" + testStringDecrypted.length);
    }

    System.out.println("-------------------------------------------------------------");
  }
}
