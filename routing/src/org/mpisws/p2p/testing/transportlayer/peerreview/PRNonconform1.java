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
 * Bob deviates from the protocol by sending an extra message.
    Expectation: After the first audit, Bob is exposed by everyone.
    (PROOF_CONFORMANCE) 
 * @author Jeff Hoye
 *
 */
public class PRNonconform1 extends PRRegressionTest {

  public PRNonconform1() throws Exception {
    super(45000);
  }

  @Override
  public void finish() {
    // see if everyone found bob exposed
    try {
      if (recordedStatus.get(aliceHandle).get(bobHandle.id) != StatusChangeListener.STATUS_EXPOSED) {
        logger.log("Alice Didn't expose bob");
        System.exit(1);
      }
      if (recordedStatus.get(carolHandle).get(bobHandle.id) != StatusChangeListener.STATUS_EXPOSED) {
        logger.log("Carol Didn't expose bob");
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
    return new BogusApp(player,pr,env) {
      
      /**
       * Make bob incorrectly send messages
       */
      @Override
      public void init() {
        super.init();
        if (player.localHandle.id.id == 2) {
          env.getSelectorManager().schedule(new TimerTask() {          
            @Override
            public void run() {
              sendMessage();
            }          
          }, 10000);
        }
      }
      
      @Override
      public void messageReceived(HandleImpl i, ByteBuffer m,
          Map<String, Object> options) throws IOException {
//        if (player.localHandle.id.id != 2) logger.logException("accepted illegal message", new Exception("stack trace"));
        if (logger.level <= Logger.INFO) logger.log("Message received: "+MathUtils.toBase64(m.array()));
      }
      
      @Override
      public void notifyStatusChange(
          IdImpl id,
          int newStatus) {
        if (player.localHandle.id.id == 2) return; // ignore bob's stuff
        if (logger.level <= Logger.INFO) logger.log("notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
        if (newStatus == STATUS_EXPOSED) {
          if (!id.equals(bobHandle.id)) {
            logger.log("Node not trusted: "+id);
            System.exit(1);
          }
        }
        addStatusNotification(this.player.localHandle,id,newStatus);
      }

    };
  }

//  @Override
//  public void buildPlayers(Environment env) throws Exception {
//    super.buildPlayers(env);
//  }
    
  public static void main(String[] args) throws Exception {
    new PRNonconform1();
  }

}
