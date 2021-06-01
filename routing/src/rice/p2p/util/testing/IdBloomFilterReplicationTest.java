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

public class IdBloomFilterReplicationTest {
  
  public static int NUM = 10000;
  public static int NUM_RUNS = 100;
  
  public static RandomSource random = null; //new SimpleRandomSource();
  public static IdFactory pFactory = null;// = new PastryIdFactory();
  public static IdFactory factory = null;// = new MultiringIdFactory(pFactory.buildRandomId(random), pFactory);
  public static GCIdFactory gFactory = null; //new GCIdFactory(factory);
  
  public static Id[] SHARED = new Id[NUM];
  public static Id[] EXTRA = new Id[2 * NUM];
  
  public static IdSet remote;
  
  public static void main(String[] args) throws IOException {
    Environment env = new Environment();
    random = env.getRandomSource();
    pFactory = new PastryIdFactory(env);
    factory = new MultiringIdFactory(pFactory.buildRandomId(random), pFactory);
    gFactory = new GCIdFactory(factory);
 //   for (int bpk=7; bpk<10; bpk++) {
 //     for (int hash=2; hash<5; hash++) {
 //       IdBloomFilter.NUM_BITS_PER_KEY = bpk;
 //       IdBloomFilter.NUM_HASH_FUNCTIONS = hash;
       doConfig();
 //     }
 //   }
  }

  public static void buildShared() {
    remote = factory.buildIdSet();
    for (int i=0; i<SHARED.length; i++) {
      SHARED[i] = factory.buildRandomId(random);
      remote.addId(SHARED[i]);
    }
    
    for (int i=0; i<EXTRA.length; i++)
      EXTRA[i] = factory.buildRandomId(random);
  }
  
  public static void doConfig() {
    System.out.println();
    System.out.println("BPK: " + IdBloomFilter.NUM_BITS_PER_KEY + "\tHASH: " + IdBloomFilter.NUM_HASH_FUNCTIONS);
    System.out.print("\t\t");
    for (int i=0; i<2 * NUM; i += NUM/10)
      System.out.print(i + "\t");
    
    System.out.println("\n");
    
    for (int i=0; i<NUM; i += NUM/10) {
      System.out.print(i + "\t\t");
      //for (int j=0; j<2*NUM; j += NUM/10) 
        System.out.print(doRun(i, 10000) + "\t");

      System.out.println();
    }
  }
  
  public static double doRun(int has, int extra) {
    int total = 0;
    
    for (int i=0; i<NUM_RUNS; i++) 
      total += run(has, extra);
    
    return ((double) total)/((double)NUM_RUNS);
  }
  
  public static int run(int has, int extra) {
    buildShared();
    IdSet local = factory.buildIdSet();
    
    for (int i=0; i<has; i++)
      local.addId(SHARED[i]);
    
    for (int i=0; i<extra; i++)
      local.addId(EXTRA[i]);
    
    int count = 0;
    int missing = NUM - has;
    
    while (missing > 0) {
      count++;
      IdBloomFilter filter = new IdBloomFilter(local);
      Iterator<Id> i = remote.getIterator();
      
      while (i.hasNext()) {
        Id next = (Id) i.next();
        
        if (! filter.check(next)) {
          local.addId(next);
          missing--;
        }
      }
    }
    
    return count;
  }
}
