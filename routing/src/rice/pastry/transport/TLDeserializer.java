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
package rice.pastry.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.commonapi.RawMessageDeserializer;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.pastry.NodeHandleFactory;
import rice.pastry.messaging.PRawMessage;

public class TLDeserializer implements RawMessageDeserializer, Deserializer {
  Map<Integer, MessageDeserializer> deserializers;
  NodeHandleFactory nodeHandleFactory;
  
  protected Environment environment;
  protected Logger logger;
  
  public TLDeserializer(NodeHandleFactory nodeHandleFactory, Environment env) {
    this.environment = env;
    this.nodeHandleFactory = nodeHandleFactory;
    this.deserializers = new HashMap<Integer, MessageDeserializer>();
    this.logger = environment.getLogManager().getLogger(TLDeserializer.class, null);
  }
  
  public RawMessage deserialize(InputBuffer buf, NodeHandle sender) throws IOException {
//    InputBuffer buf = new SimpleInputBuffer(b.array(), b.position());    
    
    int address = buf.readInt();
//    boolean hasSender = buf.readBoolean();
    byte priority = buf.readByte();
    short type = buf.readShort();
//    logger.log("addr:"+address+" sndr:"+hasSender+" pri:"+priority+" type:"+type);
//    NodeHandle sender = null;
//    if (hasSender) {
//      sender = nodeHandleFactory.readNodeHandle(buf);
//    }

    // TODO: Think about how to make this work right.  Maybe change the default deserializer?
    MessageDeserializer deserializer = getDeserializer(address);
    if (deserializer == null) {
      throw new IOException("Unknown address:"+address);  // TODO: Make UnknownAddressException
    }
    
    Message msg = deserializer.deserialize(buf, type, priority, sender);
    if (msg == null) {
      if (logger.level <= Logger.WARNING) logger.log("Deserialized message to null! d:"+deserializer+" a:"+address+" t:"+type+" p:"+priority+" s:"+sender+" b:"+buf); 
    }
    if (logger.level <= Logger.FINER) logger.log("deserialize():"+msg);
    return (RawMessage)msg;
  }

  public void serialize(RawMessage m, OutputBuffer o) throws IOException {
    PRawMessage msg = (PRawMessage)m;
    int address = msg.getDestination();
    o.writeInt(address);
    
//    NodeHandle sender = msg.getSender();
//    boolean hasSender = (sender != null);
//    o.writeBoolean(hasSender);

    
    // range check priority
    int priority = msg.getPriority();
    if (priority > Byte.MAX_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+msg+" was "+priority+".");
    if (priority < Byte.MIN_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+msg+" was "+priority+".");
    o.writeByte((byte)priority);
    
    short type = msg.getType();        
    o.writeShort(type);
    
//    if (isRouteMessage()) {
//      RouteMessage rm = (RouteMessage)msg; 
//      sendOpts = rm.getOptions(); 
//      rmSubAddress = rm.getAuxAddress();
//      rmSubType = rm.getInternalType();
//    }

    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // + NodeHandle sender +
    // + +
    // ... flexable size
    // + +
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    if (hasSender) {
//      msg.getSender().serialize(o);
//    }
        
    msg.serialize(o);
  }
  
  public void clearDeserializer(int address) {
    deserializers.remove(address);
  }

  public MessageDeserializer getDeserializer(int address) {
    return deserializers.get(address);
  }

  public void setDeserializer(int address, MessageDeserializer md) {
    deserializers.put(address, md);
  }
}
