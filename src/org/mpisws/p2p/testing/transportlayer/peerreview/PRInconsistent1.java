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
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.HandleSerializer;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.IdExtractor;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.IdImpl;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.IdSerializer;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.MyIdTL;
import org.mpisws.p2p.testing.transportlayer.peerreview.PRRegressionTest.Player;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.IndexEntryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryImpl;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.util.MathUtils;
import rice.p2p.util.RandomAccessFileIOBuffer;
import rice.selector.TimerTask;

/**
 * Bob forks his log, that is, at some point he removes the k most
    recent entries and then continues recording.
    Expectation: Bob is exposed after the next audit (PROOF_CONSISTENCY) 
    
 * @author Jeff Hoye
 *
 */
public class PRInconsistent1 extends PRRegressionTest {
  
  public PRInconsistent1() throws Exception {
    super(45000);
  }

  @Override
  protected PeerReviewImpl<HandleImpl, IdImpl> getPeerReview(Player player, MyIdTL transport, Environment env) {
    if (player.localHandle.id.id == 2) {
      return new PeerReviewImpl<HandleImpl, IdImpl>(transport, env, new HandleSerializer(), new IdSerializer(), new IdExtractor(), getIdStrTranslator()
//          ,new AuthenticatorSerializerImpl(20,96), new EvidenceSerializerImpl<HandleImpl, IdImpl>(new HandleSerializer(),
//              new IdSerializer(),transport.getHashSizeBytes(),transport.getSignatureSizeBytes())
              ) {
        
        @Override
        protected SecureHistoryFactory getSecureHistoryFactory(IdentityTransport<HandleImpl, IdImpl> transport, final Environment env) {
          return new SecureHistoryFactoryImpl(transport, env) {
            
            @Override
            protected SecureHistoryImpl makeSecureHistory(RandomAccessFileIOBuffer indexFile, RandomAccessFileIOBuffer dataFile, boolean readOnly, HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger) throws IOException {
              final ForkingSecureHistory foo = new ForkingSecureHistory(indexFile, dataFile, readOnly, hashProv, indexFactory, logger) {
                int acks = 0;
                
                @Override
                public void appendEntry(short type, boolean storeFullEntry,
                    ByteBuffer... entry) throws IOException {
                  super.appendEntry(type, storeFullEntry, entry);
//                  logger.log("appendEntry "+type);
                  // fork after the 3rd msg
                  if (type == EVT_SIGN) {
                    acks++;
                    if (acks == 3) {
                      final long idx = findLastEntry(new short[] {EVT_RECV}, Long.MAX_VALUE-10);                      
                      env.getSelectorManager().invoke(new Runnable() {                      
                        public void run() {
                          try {
                            fork(idx);                                        
                          } catch (IOException ioe) {
                            logger.logException("Error forking the history at index "+idx, ioe);
                          }
                        }                      
                      });
                    }
                  }
                }
              };
              return foo;
            }
          };
        }
      };
    } else {
      return super.getPeerReview(player, transport, env);
    }
  }

  public class ForkingSecureHistory extends SecureHistoryImpl {

    public ForkingSecureHistory(RandomAccessFileIOBuffer indexFile,
        RandomAccessFileIOBuffer dataFile, boolean readOnly,
        HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger)
        throws IOException {
      super(indexFile, dataFile, readOnly, hashProv, indexFactory, logger);
    }

    /**
     * Delete this index and everything after it
     * @param entryIndex
     */
    public void fork(long entryIndex) throws IOException {   
      IndexEntry entry = statEntry(entryIndex);
      logger.log("forking at "+entryIndex+" "+entry);
      
      dataFile.setLength(entry.getFileIndex());
      indexFile.setLength(entryIndex*indexFactory.getSerializedSize());
      
      numEntries = entryIndex; //(int)(indexFile.length()/indexFactory.getSerializedSize());      
      topEntry = statEntry(entryIndex-1);
      assert(topEntry != null);
//      nextSeq = topEntry.getSeq()+1;
    }
    
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
  public BogusApp getBogusApp(Player player, final PeerReview<HandleImpl, IdImpl> pr,
      Environment env) {
    return new BogusApp(player,pr,env) {
            
      @Override
      public void notifyStatusChange(
          IdImpl id,
          int newStatus) {
        if (player.localHandle.id.id == 2) return; // ignore bob's stuff
       // logger.log("notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
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
    new PRInconsistent1();
  }

}
