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

public class BloomFilterUnit {
  
  public static void main(String[] args) {
    RandomSource rand = new SimpleRandomSource(null);
    int k = Integer.parseInt(args[0]);
    int l = Integer.parseInt(args[1]);
    int m = Integer.parseInt(args[2]);
    int n = Integer.parseInt(args[3]);
    
    byte[][] elements = new byte[n][];
    BloomFilter filter = new BloomFilter(k, m);
    
    for (int i=0; i<elements.length; i++) {
      elements[i] = MathUtils.randomBytes(l, rand);
      filter.add(elements[i]);
    }
    
 //   System.out.println(filter.getBitSet());
    
    for (int i=0; i<elements.length; i++) {
      if (! filter.check(elements[i]))
        System.out.println("FAILURE: Element " + i + " did not exist!");
    }
    
    int count = 0;
    
    for (int i=0; i<elements.length; i++) {
      if (filter.check(MathUtils.randomBytes(l, rand)))
        count++;
    }
    
    System.out.println("FALSE POSITIVE RATE: " + count + "/" + elements.length);
  }
}