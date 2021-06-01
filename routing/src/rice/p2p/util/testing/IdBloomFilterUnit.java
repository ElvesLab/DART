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

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.*;
import rice.p2p.multiring.*;
import rice.p2p.past.gc.*;
import rice.pastry.commonapi.*;
import rice.p2p.util.*;

import java.io.IOException;
import java.util.*;

public class IdBloomFilterUnit {
  
  public static void main(String[] args) throws IOException {
    int n = 300;
    Environment env = new Environment();
    RandomSource random = env.getRandomSource();
    PastryIdFactory pFactory = new PastryIdFactory(env);
    IdFactory factory = new MultiringIdFactory(pFactory.buildRandomId(random), pFactory);
    GCIdFactory gFactory = new GCIdFactory(factory);
    
    IdSet set = gFactory.buildIdSet();
    
    for (int i=0; i<n; i++)
      set.addId(new GCId(factory.buildRandomId(random), System.currentTimeMillis()));
    
    
    System.out.println("Start: " + System.currentTimeMillis());
    IdBloomFilter filter = new IdBloomFilter(set);
    System.out.println("Done1: " + System.currentTimeMillis());
    Iterator<Id> i = set.getIterator();
    
    while (i.hasNext()) {
      if (! filter.check((Id) i.next()))
        System.out.println("FAILURE: Element did not exist!");
    }
    System.out.println("Done2: " + System.currentTimeMillis());
    
    int count = 0;
    
    for (int j=0; j<set.numElements(); j++) {
      if (filter.check(new GCId(factory.buildRandomId(random), System.currentTimeMillis())))
        count++;
    }
    System.out.println("Done3: " + System.currentTimeMillis());
    
    System.out.println("FALSE POSITIVE RATE: " + count + "/" + set.numElements());
    
    IdSet set2 = pFactory.buildIdSet();
    Id id = pFactory.buildRandomId(random);
    for (int k=0; k<16; k++) {
      set2.addId(addToId(pFactory, id, k));
    }
    
    System.out.println("Start: " + System.currentTimeMillis());
    IdBloomFilter filter2 = new IdBloomFilter(set2);
    System.out.println("Done1: " + System.currentTimeMillis());
    Iterator<Id> i2 = set2.getIterator();
    
    while (i2.hasNext()) {
      if (! filter2.check((Id) i2.next()))
        System.out.println("FAILURE: Element did not exist!");
    }
    
    for (int k=16; k<32; k++) {
      Id id2 = addToId(pFactory, id, k);
      if (filter2.check(id2))
        System.out.println("WARNING: Element existed when it probably shouldn't! " + k);
    }
    
    System.out.println("Done2: " + System.currentTimeMillis());
    
    env.destroy();
  }
  
  private static Id addToId(IdFactory FACTORY, Id id, int num) {
    byte[] bytes = id.toByteArray();
    bytes[0] += num;
    
    return FACTORY.buildId(bytes);
  }
}
