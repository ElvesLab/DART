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
package org.mpisws.p2p.testing.transportlayer.peerreview;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.BogusApp;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.HandleImpl;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.IdImpl;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.Player;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.util.MathUtils;
import rice.selector.TimerTask;

/**
 * Alice deviates from the protocol by sending a message that's different 
 * from the one she is supposed to send.
    Expectation: After the first audit, Alice is exposed by everyone.
    (PROOF_CONFORMANCE) 
 * @author Jeff Hoye
 *
 */
public class PRNonconform2 extends PRRegressionTest {

  public PRNonconform2() throws Exception {
    super(45000);
  }

  @Override
  public void finish() {
    // see if everyone found bob exposed
    try {
      if (recordedStatus.get(bobHandle).get(aliceHandle.id) != StatusChangeListener.STATUS_EXPOSED) {
        logger.log("Bob Didn't expose Alice");
        System.exit(1);
      }
      if (recordedStatus.get(carolHandle).get(aliceHandle.id) != StatusChangeListener.STATUS_EXPOSED) {
        logger.log("Carol Didn't expose Alice");
        System.exit(1);
      }
    } catch (Exception e) {
      // in case there is no record
      logger.logException("Failure", e);
      System.exit(1);
    }
    logger.log("Success");
    System.exit(0);    
  }
  
  @Override
  public BogusApp getBogusApp(Player player, PeerReview<HandleImpl, IdImpl> pr,
      Environment env) {
    if (player.localHandle.id.id == 1) {
      // alice
      return new BogusApp(player,pr,env) {
        int msgNum = 0;
        
        /**
         * On the 3rd message, modify one of the bits.
         */
        @Override
        protected byte[] generateMessage() {
          byte[] ret = super.generateMessage();
          msgNum++;
          if (msgNum == 3) {
            ret[ret.length-1]++;
          }
          return ret;
        }
        
        @Override
        public void notifyStatusChange(
            IdImpl id,
            int newStatus) {
          // ignore alice's stuff
          return;
        }
  
      };
    } else {
      return new BogusApp(player,pr,env) {
       @Override
        public void notifyStatusChange(
            IdImpl id,
            int newStatus) {
          if (player.localHandle.id.id == 1) return; // ignore alice's stuff
          if (logger.level <= Logger.INFO) logger.log("notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
          if (newStatus == STATUS_EXPOSED) {
            if (!id.equals(aliceHandle.id)) {
              logger.log("Node not trusted: "+id);
              System.exit(1);
            }
          }
          addStatusNotification(this.player.localHandle,id,newStatus);
        }
 
      };      
    }
  }

//  @Override
//  public void buildPlayers(Environment env) throws Exception {
//    super.buildPlayers(env);
//  }
    
  public static void main(String[] args) throws Exception {
    new PRNonconform2();
  }

}
