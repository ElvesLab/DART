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
 * @(#) PersistentStorageTest.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 * 
 * @version $Id: PersistentStorageTest.java 3613 2007-02-15 14:45:14Z jstewart $
 */
import java.io.*;
import java.util.*;

import rice.*;
import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.pastry.commonapi.*;
import rice.persistence.*;

/**
 * This class is a class which tests the PersistentStorage class
 * in the rice.persistence package.
 */
public class PersistentStorageTest extends MemoryStorageTest {

  private static IdFactory FACTORY;// = new PastryIdFactory();

  /**
   * Builds a MemoryStorageTest
   */
  public PersistentStorageTest(boolean store, Environment environment) throws IOException {
    super(store, environment);
    FACTORY = new PastryIdFactory(this.environment);
    storage = new PersistentStorage(FACTORY, "PersistentStorageTest" , ".", 20000000, environment);
  }

  public static void main(String[] args) throws IOException {
    boolean store = true;
    
    if (args.length > 0) {
      store = ! args[0].equals("-nostore");
    }
    
    PersistentStorageTest test = new PersistentStorageTest(store, new Environment());
    
    test.start();
  }
}
