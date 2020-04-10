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
 * @version $Id: GlacierPersistentStorageTest.java 3613 2007-02-15 14:45:14Z jstewart $
 */
import java.io.*;
import java.util.*;
import java.util.zip.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.glacier.*;
import rice.p2p.glacier.v2.*;
import rice.p2p.util.*;
import rice.pastry.commonapi.*;
import rice.persistence.*;

/**
 * This class is a class which tests the PersistentStorage class
 * in the rice.persistence package.
 */
public class GlacierPersistentStorageTest {
    
  File root;
  
  /**
   * Builds a MemoryStorageTest
   */
  public GlacierPersistentStorageTest(String root) throws IOException {
    this.root = new File("FreePastry-Storage-Root/" + root);
  }
  
  public void start() throws Exception {
    process(root);
  }
  
  protected void process(File file) throws Exception {
    File[] files = file.listFiles();
    
    for (int i=0; i<files.length; i++) {
      /* check each file and recurse into subdirectories */
      if (files[i].isFile() && (files[i].getName().length() > 20)) {
        ObjectInputStream objin = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(files[i]))));
        objin.readObject(); 
        
        Object o = objin.readObject();
        if (o instanceof FragmentAndManifest) {
          FragmentAndManifest fm = (FragmentAndManifest) o;
          
          int total = fm.fragment.payload.length + 24;
          
          total += fm.manifest.getObjectHash().length + fm.manifest.getSignature().length;
          total += fm.manifest.getFragmentHashes().length * fm.manifest.getFragmentHashes()[0].length;
          System.out.println(files[i].getName() + "\t" + total + "\t" + files[i].length());
        } else {
          System.out.println("ERROR: Found class " + o.getClass().getName());
        }
        objin.close();
        
      } else if (files[i].isDirectory()) {
        process(files[i]);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    GlacierPersistentStorageTest test = new GlacierPersistentStorageTest("sys08.cs.rice.edu-10001-glacier-immutable");
    
    test.start();
  }
}
