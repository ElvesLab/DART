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

package rice.persistence.testing;

/*
 * @(#) StorageTest.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: LRUCacheTest.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.IOException;
import java.util.*;

import rice.*;
import rice.environment.Environment;
import rice.persistence.*;
import rice.p2p.commonapi.*;
import rice.pastry.commonapi.*;

/**
 * This class is a class which tests the Cache class
 * in the rice.persistence package.
 */
@SuppressWarnings("unchecked")
public class LRUCacheTest extends Test {

  protected static final int CACHE_SIZE = 100;

  private IdFactory FACTORY;
  
  private Cache cache;

  private Id[] data;

  /**
   * Builds a MemoryStorageTest
   */
  public LRUCacheTest(Environment env) {
    super(env);
    cache = new LRUCache(new MemoryStorage(FACTORY), CACHE_SIZE, env);
    FACTORY = new PastryIdFactory(env);
    
    data  = new Id[500];
    int[] x = new int[5];

    for (int i = 0; i < 500; i ++){
        x[3] = i;
        data[i] = FACTORY.buildId(x);
    }
    
  }

  public void setUp(final Continuation c) {
    final Continuation put4 = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(false))) {
          stepDone(SUCCESS);
        } else {
          stepDone(FAILURE);
        }

        sectionEnd();
        c.receiveResult(new Boolean(true));
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    final Continuation put3 = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(SUCCESS);
        } else {
          stepDone(FAILURE);
        }

        stepStart("Inserting Fourth Object (227 bytes)");
        cache.cache(data[4], null, new byte[200], put4);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };    

    final Continuation put2 = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(SUCCESS);
        } else {
          stepDone(FAILURE);
        }

        stepStart("Inserting Third Object (65 bytes)");
        cache.cache(data[3], null, new byte[38], put3);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    Continuation put1 = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(SUCCESS);
        } else {
          stepDone(FAILURE);
        }

        stepStart("Inserting Second Object (40 bytes)");
        cache.cache(data[2], null, new byte[13], put2);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    sectionStart("Inserting Objects");
    
    stepStart("Inserting First Object (30 bytes)");
    cache.cache(data[1], null, new byte[3], put1);
  }

  private void testExists(final Continuation c) {
    setUp(new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          sectionStart("Checking for Objects");
          stepStart("Checking for First Object");
          if (cache.exists(data[1])) stepDone(SUCCESS); else stepDone(FAILURE);
          stepStart("Checking for Second Object");
          if (cache.exists(data[2])) stepDone(SUCCESS); else stepDone(FAILURE);
          stepStart("Checking for Third Object");
          if (cache.exists(data[3])) stepDone(SUCCESS); else stepDone(FAILURE);
          stepStart("Checking for Fourth Object");
          if (cache.exists(data[4])) stepDone(SUCCESS); else stepDone(FAILURE);
          stepStart("Checking for Fifth Object");
          if (cache.exists(data[5])) stepDone(SUCCESS); else stepDone(FAILURE);
          sectionEnd();
          
          c.receiveResult(new Boolean(true));
        } else {
          throw new RuntimeException("SetUp did not complete correctly!");
        }
      }
      
      public void receiveException(Exception e) {
        stepException(e);
      }
    });
  }

  private void testScan(final Continuation c) {
    final Continuation handleBadScan = new Continuation() {
      public void receiveResult(Object o) {
        stepDone(FAILURE, "Query returned; should have thrown exception");
      }

      public void receiveException(Exception e) {
        stepDone(SUCCESS);

        sectionEnd();

        c.receiveResult(new Boolean(true));
      }
    };

    final Continuation query = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(SUCCESS);
        } else {
          stepDone(FAILURE);
        }

        stepStart("Requesting Scan from 3 to 6");
        IdSet result = cache.scan(FACTORY.buildIdRange(data[3], data[6]));
        
        if (result.numElements() != 1) {
          stepDone(FAILURE, "Result had " + result.numElements() + " elements, expected 1.");
          return;
        }
        
        if (! result.isMemberId(data[3])) {
          stepDone(FAILURE, "Result had incorrect element " + data[3] + ", expected 3.");
          return;
        }
        
        stepDone(SUCCESS);
        
        stepStart("Requesting Scan from 8 to 10");
        result = cache.scan(FACTORY.buildIdRange(data[8], data[10]));
        
        if (result.numElements() != 0) {
          stepDone(FAILURE, "Result had " + result.numElements() + " elements, expected 0.");
          return;
        }
        
        stepDone(SUCCESS);
        
        stepStart("Requesting Scan from 'Monkey' to 9");
        handleBadScan.receiveException(new Exception());
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    Continuation insertString = new Continuation() {
      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          sectionStart("Testing Scan");

          stepStart("Inserting String as Key");
          cache.cache(data[11], null, new byte[0], query);
        } else {
          stepException(new RuntimeException("Exists did not complete correctly."));
        }
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    testExists(insertString);
  }

  private void testRandomInserts(final Continuation c) {
    
    final int START_NUM = 10;
    final int END_NUM = 98;
    final int SKIP = 2;

    final int NUM_ELEMENTS = 17;
    final int LAST_NUM_REMAINING = 66;
    
    final Continuation checkRandom = new Continuation() {

      public void receiveResult(Object o) {
        stepStart("Checking object deletion");
        int NUM_DELETED = ((Integer) o).intValue();
        int length = cache.scan(FACTORY.buildIdRange(data[13 + START_NUM], data[13 + END_NUM + SKIP])).numElements();
        
        int desired = NUM_ELEMENTS - NUM_DELETED;
        
        if (length == desired) {
          stepDone(SUCCESS);
          
          sectionEnd();
          c.receiveResult(new Boolean(true));
        } else {
          stepDone(FAILURE, "Expected " + desired + " objects after deletes, found " + length + ".");
          return;
        }
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    
    final Continuation removeRandom = new Continuation() {
      
      private int count = LAST_NUM_REMAINING;
      private int num_deleted = 0;

      public void receiveResult(Object o) {
        if (count == LAST_NUM_REMAINING) {
          stepStart("Removing random subset of objects");
        }

        if (o.equals(new Boolean(false))) {
          stepDone(FAILURE, "Deletion of " + count + " failed.");
          return;
        }

        if (count == END_NUM) {
          stepDone(SUCCESS);
          checkRandom.receiveResult(new Integer(num_deleted));
          return;
        }

        if (environment.getRandomSource().nextBoolean()) {
          num_deleted++;
          cache.uncache(data[13 + (count += SKIP)], this);
        } else {
          count += SKIP;
          receiveResult(new Boolean(true));
        }
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };
    
    final Continuation checkExists = new Continuation() {
      public void receiveResult(Object o) {
        stepStart("Checking exists for all 50 objects");
        
        for (int count = START_NUM - SKIP; count < END_NUM; count+=SKIP) {
          Boolean b = new Boolean(cache.exists(data[13 + count]));
          
          if (b.equals(new Boolean(count < LAST_NUM_REMAINING))) {
            if (b.booleanValue()) {
              stepDone(FAILURE, "Element " + count + " did exist - should not have.");
              return;
            } else {
              stepDone(FAILURE, "Element " + count + " did not exist - should have.");
              return;
            }
          }
        }
        
        stepDone(SUCCESS);
        removeRandom.receiveResult(new Boolean(true));
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };
    
    
    final Continuation insert = new Continuation() {

      private int count = START_NUM;

      public void receiveResult(Object o) {
        if (o.equals(new Boolean(false))) {
          stepDone(FAILURE, "Insertion of " + count + " failed.");
          return;
        }
        
        if (count == START_NUM) {
          stepDone(SUCCESS);
          stepStart("Inserting 40 objects from 100 to 1000000 bytes");
        }

        if (count > END_NUM) {
          stepDone(SUCCESS);
          checkExists.receiveResult(new Boolean(true));
          return;
        }

        int num = count;
        count += SKIP;
        
        cache.cache(data[13 + num], null, new byte[num * num * num], this);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    final Continuation setSize = new Continuation() {
      public void receiveResult(Object o) {

        if (o.equals(new Boolean(false))) {
          stepDone(FAILURE, "Testing of scan failed");
          return;
        }

        sectionStart("Stress Testing");
        stepStart("Increasing cache size to 10000000 bytes");

        cache.setMaximumSize(10000000, insert);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };
  
    testScan(setSize);
  }


  private void testErrors() {
    final Continuation validateNullValue = new Continuation() {

      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(FAILURE, "Null value should return false");
          return;
        }

        stepDone(SUCCESS);

        sectionEnd();        
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    final Continuation insertNullValue = new Continuation() {

      public void receiveResult(Object o) {
        if (o.equals(new Boolean(true))) {
          stepDone(FAILURE, "Null key should return false");
          return;
        }

        stepDone(SUCCESS);

        stepStart("Inserting null value");

        cache.cache(data[12], null, null, validateNullValue);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    final Continuation insertNullKey = new Continuation() {

      public void receiveResult(Object o) {
        if (o.equals(new Boolean(false))) {
          stepDone(FAILURE, "Randon insert tests failed.");
          return;
        }

        sectionStart("Testing Error Cases");
        stepStart("Inserting null key");

        cache.cache(null, null, "null key", insertNullValue);
      }

      public void receiveException(Exception e) {
        stepException(e);
      }
    };

    testRandomInserts(insertNullKey);
  }
  
  public void start() {
    testErrors();
  }

  public static void main(String[] args) throws IOException {
    LRUCacheTest test = new LRUCacheTest(new Environment());

    test.start();
  }
}
