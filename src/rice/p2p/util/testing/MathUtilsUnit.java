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
/*
 * Created on May 24, 2005
 *
 */
package rice.p2p.util.testing;

import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.util.MathUtils;

/**
 * MathUtils unit tests
 * 
 * @author jstewart
 * @param argv The command line arguments
 */
public class MathUtilsUnit {
  
  public static void main(String[] args) {
    System.out.println("MathUtils Test Suite");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Running Tests");

    System.out.print("    Testing hexadecimal conversion\t\t\t");

    byte[] testHexBytes = new byte[] {(byte) 0xa7, (byte) 0xb3, (byte) 0x00, (byte) 0x12, (byte) 0x4e};
    String result = MathUtils.toHex(testHexBytes);
    
    if (result.equals("a7b300124e")) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testHexBytes);
      System.out.println("    Output:\t" + result);
    }
    
    System.out.print("    Testing long conversion\t\t\t\t");
    long testLong = Long.parseLong("0123456789ABCDEF", 16);

    byte[] testLongByte = MathUtils.longToByteArray(testLong);

    if ((testLongByte[0] == (byte) 0x01) &&
      (testLongByte[1] == (byte) 0x23) &&
      (testLongByte[2] == (byte) 0x45) &&
      (testLongByte[3] == (byte) 0x67) &&
      (testLongByte[4] == (byte) 0x89) &&
      (testLongByte[5] == (byte) 0xAB) &&
      (testLongByte[6] == (byte) 0xCD) &&
      (testLongByte[7] == (byte) 0xEF)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + testLong);
      System.out.println("    Output:\t" + testLongByte[0] + " " + testLongByte[1] + " " +
        testLongByte[2] + " " + testLongByte[3]);
    }

    System.out.print("    Testing int->byte[]->int conversion\t\t\t");
    
    RandomSource r = new SimpleRandomSource(null);
    boolean passed = true;

      for (int n=0; n<100000; n++) {
        int l = r.nextInt();
        byte[] ar = MathUtils.intToByteArray(l);
        int res = MathUtils.byteArrayToInt(ar);
        /*
        long l = r.nextLong();
        byte[] ar = longToByteArray(l);
        long result = byteArrayToLong(ar);
        */

        if (res != l) {
          passed = false;
          System.out.println("[ FAILED ]");
          System.out.println("input:  "+l);
          System.out.print  ("byte[]: ");
          for (int i=0; i<ar.length; i++) {
            System.out.print(ar[i]+" ");
          }
          System.out.println();
          System.out.println("output: "+result);
          break;
          }
      }
      
      if (passed) System.out.println("[ PASSED ]");

    System.out.print("    Testing long->byte[]->long conversion\t\t\t");
    
    passed = true;

      for (int n=0; n<100000; n++) {
        long l = r.nextLong();
        byte[] ar = MathUtils.longToByteArray(l);
        long res = MathUtils.byteArrayToLong(ar);

        if (res != l) {
          passed = false;
          System.out.println("[ FAILED ]");
          System.out.println("input:  "+l);
          System.out.print  ("byte[]: ");
          for (int i=0; i<ar.length; i++) {
            System.out.print(ar[i]+" ");
          }
          System.out.println();
          System.out.println("output: "+result);
          break;
          }
      }
      
      if (passed) System.out.println("[ PASSED ]");
      
    System.out.println("-------------------------------------------------------------");
  }
}
