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

package rice.p2p.scribe.messaging;

import java.io.IOException;
import java.util.*;

// import rice.replay.*;
import rice.pastry.PastryNode;
import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.JavaSerializedScribeContent;
import rice.p2p.scribe.rawserialization.RawScribeContent;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * @(#) AnycastMessage.java The anycast message.
 * 
 * @version $Id: AnycastMessage.java,v 1.7 2005/03/11 00:58:02 jeffh Exp $
 * @author Alan Mislove
 */
public class AnycastFailureMessage extends ScribeMessage {
  public static final short TYPE = 11;

  // This is the same content that was sent in the anycast message
  public RawScribeContent content;

  // public void dump(ReplayBuffer buffer, PastryNode pn) {
  // buffer.appendByte(rice.pastry.commonapi.PastryEndpointMessage.idScribeAnycastFailureMessage);
  // super.dump(buffer,pn);
  // if(content == null) {
  // buffer.appendByte(Verifier.NULL);
  // } else {
  // buffer.appendByte(Verifier.NONNULL);
  // content.dump(buffer,pn);
  // }
  // }
  //
  //    
  // public AnycastFailureMessage(ReplayBuffer buffer, PastryNode pn) {
  // super(buffer,pn);
  // if(buffer.getByte() == Verifier.NULL) {
  // content = null;
  // }else {
  // content = Verifier.restoreScribeContent(buffer,pn);
  // }
  // //System.out.println("Constructed AnycastFailureMessage: " + this);
  // }
  public AnycastFailureMessage(NodeHandle source, Topic topic, ScribeContent content) {
    this(source, topic, content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content));
  }

  public AnycastFailureMessage(NodeHandle source, Topic topic, RawScribeContent content) {
    super(source, topic);
    this.content = content;
  }

  public ScribeContent getContent() {
    return content;
  }

  public String toString() {
    String s = "AnycastFailureMessage: ";
    s = s + "Source= " + source + " Topic= " + topic + "ScribeContent= "
        + content;
    return s;
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    super.serialize(buf); 
    
    buf.writeShort(content.getType());
    content.serialize(buf);      
  }
   
  public static AnycastFailureMessage build(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer scd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new AnycastFailureMessage(buf, endpoint, scd);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
  
  /**
   * Private because it should only be called from build(), if you need to extend this,
   * make sure to build a serializeHelper() like in AnycastMessage/SubscribeMessage, and properly handle the 
   * version number.
   */
  private AnycastFailureMessage(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer cd) throws IOException {
    super(buf, endpoint);
    
    // this can be done lazilly to be more efficient, must cache remaining bits, endpoint, cd, and implement own InputBuffer
    short contentType = buf.readShort();
    if (contentType == 0) {
      content = new JavaSerializedScribeContent(cd.deserializeScribeContent(buf, endpoint, contentType));
    } else {
      content = (RawScribeContent)cd.deserializeScribeContent(buf, endpoint, contentType); 
    }
    
  }
}
