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
package org.mpisws.p2p.transport.peerreview.commitment;

import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.message.OutgoingUserDataMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;

import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.tuples.Tuple;

/**
 * We need to keep some state for each peer, including separate transmit and
 * receive queues
 */
public class PeerInfo<Handle extends RawSerializable> {
  public static final int INITIAL_CHALLENGE_INTERVAL_MICROS = 30000000;

  Handle handle;

  /**
   * The number off un-acknowledged packets, currently can only be 1 or 0
   */
  int numOutstandingPackets;
  long lastTransmit;
  long currentTimeout;
  int retransmitsSoFar;
  long lastChallenge;
  long currentChallengeInterval;
  /**
   * The first message hasn't been acknowledged, the rest haven't been sent.
   */
  LinkedList<OutgoingUserDataMessage<Handle>> xmitQueue;
  LinkedList<Tuple<UserDataMessage<Handle>, Map<String, Object>>> recvQueue;
  boolean isReceiving;
  
  public PeerInfo(Handle handle) {
    this.handle = handle;
    lastTransmit = 0;
    xmitQueue = new LinkedList<OutgoingUserDataMessage<Handle>>();
    recvQueue = new LinkedList<Tuple<UserDataMessage<Handle>,Map<String,Object>>>();
    currentTimeout = 0;
    retransmitsSoFar = 0;
    lastChallenge = -1;
    currentChallengeInterval = INITIAL_CHALLENGE_INTERVAL_MICROS;
    isReceiving = false;
  }

  public Handle getHandle() {
    return handle;
  }

  public long getLastTransmit() {
    return lastTransmit;
  }

  public long getCurrentTimeout() {
    return currentTimeout;
  }

  public int getRetransmitsSoFar() {
    return retransmitsSoFar;
  }

  public long getLastChallenge() {
    return lastChallenge;
  }

  public long getCurrentChallengeInterval() {
    return currentChallengeInterval;
  }

  public LinkedList<OutgoingUserDataMessage<Handle>> getXmitQueue() {
    return xmitQueue;
  }

  public LinkedList<Tuple<UserDataMessage<Handle>,Map<String,Object>>> getRecvQueue() {
    return recvQueue;
  }

  public boolean isReceiving() {
    return isReceiving;
  }

  public void clearXmitQueue() {
    xmitQueue.clear();
  }
}
