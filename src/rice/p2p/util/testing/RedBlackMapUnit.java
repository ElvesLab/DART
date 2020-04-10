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

@SuppressWarnings("unchecked")
public class RedBlackMapUnit {
  
  public static void main(String[] args) {
    int n = Integer.parseInt(args[0]);
    int t = Integer.parseInt(args[0]);
    
    Environment env = new Environment();
    
    RandomSource random = env.getRandomSource();
    PastryIdFactory pFactory = new PastryIdFactory(env);
    IdFactory factory = pFactory; //new MultiringIdFactory(pFactory.buildRandomId(random), pFactory);
    
    Id[] array = new Id[n];
    Long[] values = new Long[n];
    RedBlackMap map = new RedBlackMap();
    
    for (int i=0; i<array.length; i++) {
      array[i] = factory.buildRandomId(random);
      //values[i] = new Long(random.nextLong());
      
      map.put(array[i], values[i]);
    }

    
    System.out.print("MAIN MAP: ");
    printMap(map);
    
    testRemove(map);
    testSortedMap(t, map, factory, random, 2);
  }
  
  protected static void testRemove(SortedMap map) {
    Id[] sorted = (Id[]) map.keySet().toArray(new Id[0]);
    Arrays.sort(sorted);
    
    for (int i=0; i<sorted.length; i++)
      testRemove(map, sorted, sorted[i]);
  }
  
  protected static void testRemove(SortedMap map, Id[] sorted, Id remove) {
    System.out.println("REMOVING " + remove);
    map.remove(remove);
    Iterator i = map.keySet().iterator();
    int count = 0;
    
    while (i.hasNext()) {
      if (sorted[count].equals(remove))
        count++;
      
      Id next = (Id) i.next();
      
      if (! next.equals(sorted[count]))
        throw new RuntimeException("FAILURE: Remove did not expect element " + next + " expected " + sorted[count]);
      
      count++;
    }
    
    if ((count < sorted.length) && (! sorted[count].equals(remove)))
      throw new RuntimeException("FAILURE: Remove did not receive element " + sorted[count]);
    
    map.put(remove, null);
  }

  
  protected static void printMap(Id[] id) {
    for (int i=0; i<id.length; i++)
      System.out.print(" " + id[i] + ",");
    
    System.out.println();
  }
  
  protected static void printMap(SortedMap map) {
    Iterator i = map.keySet().iterator();
    
    while (i.hasNext())
      System.out.print(" " + i.next() + ",");
                       
    System.out.println();
  }
  
  protected static void testSortedMap(int iterations, SortedMap map, IdFactory factory, RandomSource random, int depth) {   
    Id[] sorted = (Id[]) map.keySet().toArray(new Id[0]);
    Arrays.sort(sorted);

    for (int i=0; i<iterations; i++) {
      Id id1 = factory.buildRandomId(random);
      Id id2 = factory.buildRandomId(random);

      testHeadMap(id1, sorted, map.headMap(id1));
      testTailMap(id2, sorted, map.tailMap(id2));
            
      if (id1.compareTo(id2) <= 0) {
        testNormalSubMap(id1, id2, sorted, map.subMap(id1, id2));
        try {
          testWrappedSubMap(id2, id1, sorted, map.subMap(id2, id1));
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing map " + map.keySet().size());
          printMap(map);
          
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.println("Testing  " + id1 + " " + id2); 
          
          throw e;
        }
      } else {
        testNormalSubMap(id2, id1, sorted, map.subMap(id2, id1));
        try {
          testWrappedSubMap(id1, id2, sorted, map.subMap(id1, id2));
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing map " + map.keySet().size());
          printMap(map);
          
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.println("Testing  " + id1 + " " + id2); 
          
          throw e;
        }
      }
      
      if (depth > 0) {
        try {
          testSortedMap(iterations, map.headMap(id1), factory, random, depth-1);
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing headMap to " + id1 + " ");
          printMap(map.headMap(id1));
          
          throw e;
        }
        
        try {
          testSortedMap(iterations, map.tailMap(id2), factory, random, depth-1);
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing tailMap from " + id2 + " ");
          printMap(map.tailMap(id2));
          
          throw e;
        }
        
        try {
          testSortedMap(iterations, map.subMap(id1, id2), factory, random, depth-1);
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing subMap from " + id1 + " to " + id2 + " ");
          printMap(map.subMap(id1, id2));
          
          throw e;
        }
        
        try {
          testSortedMap(iterations, map.subMap(id2, id1), factory, random, depth-1);
        } catch (RuntimeException e) {
          for (int j=0; j<2-depth; j++) System.out.print("  ");      
          System.out.print("Testing subMap from " + id2 + " to " + id1 + " ");
          printMap(map.subMap(id2, id1));
          
          throw e;
        }
      }
    }
  }
  
  protected static void testHeadMap(Id head, Id[] sorted, SortedMap sub) {    
    Iterator it = sub.keySet().iterator();
    int count = 0;
    
    while (it.hasNext()) {
      Id next = (Id) it.next();
      
      if (next != sorted[count]) 
        throw new RuntimeException("FAILURE: Head test expected element " + sorted[count] + " got " + next);
      
      if (next.compareTo(head) > 0)
        throw new RuntimeException("FAILURE: Head test did not expect element " + next);
      
      count++;
    }
    
    if ((count < sorted.length) && (sorted[count].compareTo(head) < 0)) 
      throw new RuntimeException("FAILURE: Head test did not receive element " + sorted[count]);
  }
  
  protected static void testTailMap(Id tail, Id[] sorted, SortedMap sub) {        
    Iterator it = sub.keySet().iterator();
    int count = getIndex(tail, sorted);
    
    while (it.hasNext()) {
      Id next = (Id) it.next();
      
      if (next != sorted[count]) 
        throw new RuntimeException("FAILURE: Tail test expected element " + sorted[count] + " got " + next);
      
      if (next.compareTo(tail) < 0)
        throw new RuntimeException("FAILURE: Tail test did not expect element " + next);
      
      count++;
    }
    
    if (count < sorted.length) 
      throw new RuntimeException("FAILURE: Tail test did not receive element " + sorted[count]);
  }  
  
  protected static void testNormalSubMap(Id from, Id to, Id[] sorted, SortedMap sub) {        
    Iterator it = sub.keySet().iterator();
    int count = getIndex(from, sorted);
    
    while (it.hasNext()) {
      Id next = (Id) it.next();
      
      if (next != sorted[count]) 
        throw new RuntimeException("FAILURE: Normal subMap test expected element " + sorted[count] + " got " + next);
      
      if ((next.compareTo(from) < 0) || (next.compareTo(to) >= 0))
        throw new RuntimeException("FAILURE: Normal subMap test did not expect element " + next);
      
      count++;
    }
    
    if ((count < sorted.length) && (sorted[count].compareTo(to) < 0))
      throw new RuntimeException("FAILURE: Normal subMap test did not receive element " + sorted[count]);
  }
  
  protected static void testWrappedSubMap(Id from, Id to, Id[] sorted, SortedMap sub) {         
    try {
    
    Iterator it = sub.keySet().iterator();
    int count = (sorted.length > 0 ? (sorted[0].compareTo(to) < 0 ? 0 : getIndex(from, sorted)) : 0);
    
    while (it.hasNext()) {
      Id next = (Id) it.next();      
      if (next != sorted[count]) 
        throw new RuntimeException("FAILURE: Wrapped subMap test expected element " + sorted[count] + " got " + next);
      
      if ((next.compareTo(from) < 0) && (next.compareTo(to) >= 0))
        throw new RuntimeException("FAILURE: Wrapped subMap test did not expect element " + next);
      
      count++;
      
      if ((count < sorted.length) && (sorted[count].compareTo(to) >= 0) && (sorted[count].compareTo(from) < 0))
        count = getIndex(from, sorted);
    }
    
     if (count < sorted.length)
       throw new RuntimeException("FAILURE: Wrapped subMap test did not receive element " + sorted[count]);
    } catch (RuntimeException e) {
      System.out.print("--> Testing wrapped from " + from + " to " + to + " in map ");
      printMap(sorted);
      System.out.print("--> In map ");
      printMap(sub);
      
      throw e;
    }
  }

  
  
  protected static int getIndex(Id id, Id[] sorted) {
    for (int i=0; i<sorted.length; i++) 
      if (id.compareTo(sorted[i]) <= 0)
        return i;
    
    return sorted.length;
  }
}
