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

import java.io.IOException;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.rendezvous.RendezvousContact;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketNodeHandle;

/**
 * Maintains RendezvousInfo with the NodeHandle
 * 
 * @author Jeff Hoye
 */
public class RendezvousSocketNodeHandle extends SocketNodeHandle implements RendezvousContact {
  /**
   * Internet Routable (or proper port forwarding)
   */
  public static final byte CONTACT_DIRECT = 0;
  
  /**
   * Not Internet routable
   */
  public static final byte CONTACT_FIREWALLED = 1;

  private byte contactStatus;
  
  RendezvousSocketNodeHandle(MultiInetSocketAddress eisa, long epoch, Id id, PastryNode node, byte contactStatus) {
    super(eisa, epoch, id, node);
    this.contactStatus = contactStatus; 
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    super.serialize(buf);
    buf.writeByte(contactStatus);
  }

  public boolean canContactDirect() {
    return contactStatus != CONTACT_FIREWALLED;
  }

  static SocketNodeHandle build(InputBuffer buf, PastryNode local) throws IOException {
    MultiInetSocketAddress eaddr = MultiInetSocketAddress.build(buf);
    long epoch = buf.readLong();
    Id nid = Id.build(buf);
    byte contactStatus = buf.readByte();
    return new RendezvousSocketNodeHandle(eaddr, epoch, nid, local, contactStatus);
  }

  public byte getContactStatus() {
    return contactStatus;
  }
  
  public String toString() {
    String s = "[RSNH: " + nodeId + "/" + eaddress;
    if (!canContactDirect()) s+="(FIREWALLED)";
    s+= "]";
    return s;
  }
  
}
