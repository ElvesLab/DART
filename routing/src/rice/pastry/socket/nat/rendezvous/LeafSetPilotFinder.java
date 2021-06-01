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
package rice.pastry.socket.nat.rendezvous;

import java.util.ArrayList;
import java.util.HashSet;

import org.mpisws.p2p.transport.rendezvous.PilotFinder;

import rice.environment.random.RandomSource;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;

public class LeafSetPilotFinder implements
    PilotFinder<RendezvousSocketNodeHandle> {
  LeafSet leafSet;
  RandomSource random;
  
  
  public LeafSetPilotFinder(PastryNode pn) {
    this.leafSet = pn.getLeafSet();
    this.random = pn.getEnvironment().getRandomSource();
  }


  public RendezvousSocketNodeHandle findPilot(RendezvousSocketNodeHandle dest) {
    if (dest.canContactDirect()) throw new IllegalArgumentException("Dest "+dest+ " is not firewalled.");
    
    if (leafSet.contains(dest)) {
      // generate intermediate node possibilities
      HashSet<RendezvousSocketNodeHandle> possibleIntermediates = new HashSet<RendezvousSocketNodeHandle>();

      // Note: It's important to make sure the intermediates are in dest's leafset, which means that the distance between the indexes must be <= leafSet.maxSize()/2      
      if (leafSet.overlaps()) {
        // small ring, any will do
        for (NodeHandle foo : leafSet) {
          RendezvousSocketNodeHandle nh = (RendezvousSocketNodeHandle)foo;
          if (nh.canContactDirect()) possibleIntermediates.add(nh);          
        }
      } else {
        int index = leafSet.getIndex(dest);
        int maxDist = leafSet.maxSize()/2; 

        for (int i=-leafSet.ccwSize(); i<=leafSet.cwSize(); i++) {
          if (i != 0) { // don't select self
            if (Math.abs(index-i) <= maxDist) {
              RendezvousSocketNodeHandle nh = (RendezvousSocketNodeHandle)leafSet.get(i);
              if (nh.canContactDirect()) possibleIntermediates.add(nh);
            }
          }
        }
      }
      
      // return random one
      ArrayList<RendezvousSocketNodeHandle> list = new ArrayList<RendezvousSocketNodeHandle>(possibleIntermediates);
      
      // this is scary...
      if (list.isEmpty()) return null;
      
      return list.get(random.nextInt(list.size()));
    } else {
      // this is out of our scope
      return null;
    }
  }

}
