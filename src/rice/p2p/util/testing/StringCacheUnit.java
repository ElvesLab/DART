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

import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.util.*;
import java.io.*;
import java.util.*;

public class StringCacheUnit {

   public static void main(String[] argv) {
    System.out.println("StringCache Test Suite");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Initializing Tests");    
    System.out.print("    Generating string cache\t\t\t\t");    
    StringCache sc = new StringCache();
    System.out.println("[ DONE ]");
    
    System.out.print("    Generating random number generator\t\t\t");    
    RandomSource rng = new SimpleRandomSource(null);
    System.out.println("[ DONE ]");    
    
    System.out.print("    Generating test strings\t\t\t\t");    
    char[] test = new char[] {'t', 'e', 's', 't'};
    char[] test_2 = new char[] {'t', 'e', 's', 't'};
    char[] test_3 = new char[] {'t', 'e', 's', 't'};
    char[] test2 = new char[] {'t', 'e', 's', 't', '2'};
    System.out.println("[ DONE ]");

    System.out.println("-------------------------------------------------------------");
    System.out.println("  Running Tests");

    System.out.print("    Testing Simple Put\t\t\t\t");

    String s = sc.get(test);
    String s2 = sc.get(test2);
    
    if (Arrays.equals(test, s.toCharArray())) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + String.valueOf(test));
      System.out.println("    Output:\t" + s);
    }
    
    System.out.print("    Testing Double Put\t\t\t\t");
    
    String s_2 = sc.get(test_2);
    
    if (Arrays.equals(test_2, s.toCharArray()) && (s == s_2)) {
      System.out.println("[ PASSED ]");
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Input: \t" + String.valueOf(test_2));
      System.out.println("    Output:\t" + s_2 + " " + s_2.hashCode() + " " + s + " " + s.hashCode());
    }
  
    System.out.print("    Loading 10000 4-char strings\t\t\t\t");
    HashSet<String> set = new HashSet<String>();
    char[] array = new char[4];
    String t = null;
    
    for (int i=0; i<10000; i++) {
      t = sc.get(randomize(rng, array));
      
      if (! set.contains(t)) {
        set.add(t);
      } else {
        Iterator<String> j = set.iterator();
        
        while (j.hasNext()) {
          String other = (String) j.next();
          
          if (other.equals(t)) {
            if (other != t) {
              System.out.println("[ FAILED ]");
              System.out.println("    Output:\t" + t + " " + t.hashCode() + " " + other + " " + other.hashCode());
            } else {
              System.out.println("MATCH! (" + t + ")");
            }
          }
        }
      }
    }

    System.out.println("[ PASSED ]");
    
    System.out.println("-------------------------------------------------------------");
  }
  
  public static char[] randomize(RandomSource rng, char[] text) {
    byte[] data = new byte[text.length];
    rng.nextBytes(data);
    
    for (int i=0; i<data.length; i++)
      text[i] = (char) (((byte) 0x7F & data[i]) | (byte) 0x20);
    
    return text;
  }
}
