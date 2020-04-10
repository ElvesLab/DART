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
package rice.pastry.peerreview;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.peerreview.WitnessListener;

import rice.Continuation;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.pastry.NodeHandle;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.Id;
import rice.pastry.NodeSet;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PRawMessage;
import rice.pastry.routing.RouteMessage;
import rice.selector.TimerTask;

public class FetchLeafsetApp extends PastryAppl {
  public static final int APP_ID = 0xf80d17e8;
  
  protected Map<Id, Tuple<TimerTask,Collection<Continuation<Collection<NodeHandle>,Exception>>>> pendingLookups =
    new HashMap<Id, Tuple<TimerTask,Collection<Continuation<Collection<NodeHandle>,Exception>>>>();
  
  
  protected int numNeighbors;
  protected byte routeMsgVersion;
  
  public FetchLeafsetApp(final PastryNode pn, int numNeighbors) {
    super(pn, null, APP_ID, new MessageDeserializer() {
      public Message deserialize(InputBuffer buf, short type, int priority, rice.p2p.commonapi.NodeHandle sender) throws IOException {
        switch(type) {
        case FetchLeafsetRequest.TYPE:
          if (sender == null) throw new IOException("Sender is null for FetchLeafsetRequest");
          return new FetchLeafsetRequest((rice.pastry.NodeHandle)sender,Id.build(buf));
        case FetchLeafsetResponse.TYPE:
          return new FetchLeafsetResponse(buf,pn,(rice.pastry.NodeHandle)sender);
        }      
        throw new IOException("Unknown type:"+type);
      }
    });
    routeMsgVersion = (byte)thePastryNode.getEnvironment().getParameters().getInt("pastry_protocol_router_routeMsgVersion");
    this.numNeighbors = numNeighbors;
  }

  @Override
  public void messageForAppl(Message msg) {
    PRawMessage m = (PRawMessage)msg;
    switch(m.getType()) {
    case FetchLeafsetRequest.TYPE:
      FetchLeafsetRequest req = (FetchLeafsetRequest)m;
      logger.log("getNeighbors("+req.subject+") sending response");

      thePastryNode.send(req.getSender(),new FetchLeafsetResponse(req.subject,thePastryNode.getLeafSet()),null,null);
      return;
    case FetchLeafsetResponse.TYPE:
      handleResponse((FetchLeafsetResponse)m);
      return;
    }
  }

  protected void handleResponse(FetchLeafsetResponse response) {
    logger.log("handleResponse("+response.subject+")");
    Tuple<TimerTask, Collection<Continuation<Collection<NodeHandle>, Exception>>> foo = pendingLookups.remove(response.subject);
    if (foo == null) return;
    foo.a().cancel();
    
    NodeSet ns = response.leafSet.replicaSet(response.subject, numNeighbors+1); // don't use self as the neighbor, so, need an extra
    Collection<NodeHandle> ret = ns.getCollection(); 
    for (Continuation<Collection<NodeHandle>, Exception> c : foo.b()) {
      c.receiveResult(ret);
    }
  }
  
  /**
   * Add to the pendingLookups.  If no pending lookups for the id, Schedule retries.
   * 
   * @param subject
   * @param continuation
   */
  public void getNeighbors(final Id subject,
      Continuation<Collection<NodeHandle>, Exception> continuation) {
    
    logger.log("getNeighbors("+subject+")");
    
    Tuple<TimerTask, Collection<Continuation<Collection<NodeHandle>, Exception>>> foo = pendingLookups.get(subject);

    boolean startTask = false;
    if (foo == null) {
      startTask = true;
      foo = new Tuple<TimerTask, Collection<Continuation<Collection<NodeHandle>,Exception>>>(
        new TimerTask() {      
          @Override
          public void run() {
            logger.log("getNeighbors("+subject+") sending fetch");
            thePastryNode.getRouter().route(new RouteMessage(subject,new FetchLeafsetRequest(thePastryNode.getLocalHandle(),subject),routeMsgVersion));
          }      
        },
      new ArrayList<Continuation<Collection<NodeHandle>,Exception>>());      
      pendingLookups.put(subject,foo);
    }
    
    foo.b().add(continuation);
    
    // go now, and retry every 3 seconds
    if (startTask) {
      thePastryNode.getEnvironment().getSelectorManager().schedule(foo.a(),0,3000);     
    }
  }
}
