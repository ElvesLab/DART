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
package rice.pastry.direct;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.pastry.*;
import rice.pastry.direct.proximitygenerators.GenericProximityGenerator;
import rice.pastry.messaging.*;
import rice.pastry.routing.*;
// import rice.pastry.mytesting.*;
// import rice.p2p.lala.testing.Tracker;

import java.util.*;
import java.lang.*;
import java.io.*;

// This topology will read in a topology-distance matrix and the corresponding
// coordinates from input files
public class GenericNetwork<Identifier, MessageType> extends NetworkSimulatorImpl<Identifier, MessageType> {
  
  // The static variable MAXOVERLAYSIZE should be set to the n, where its input
  // is a N*N matrix
  public GenericNetwork(Environment env, String inFile) throws IOException {
    this(env, new File(inFile));  
  }
  
  public GenericNetwork(Environment env) throws IOException {
    this(env, (File)null);
  }
  
  public GenericNetwork(Environment env, File inFile) throws IOException {
    super(env, new GenericProximityGenerator(env, inFile));    
  }  
}
