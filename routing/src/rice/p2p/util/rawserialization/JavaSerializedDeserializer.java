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
/*
 * Created on Feb 21, 2006
 */
package rice.p2p.util.rawserialization;

import java.io.*;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.PastryNode;

/**
 * Handles "old" java serialized messages for programming convienience
 * and reverse compatability.
 * 
 * @author Jeff Hoye
 */
public class JavaSerializedDeserializer implements MessageDeserializer {

  protected Endpoint endpoint;
  private boolean deserializeOnlyTypeZero = true;
  
  public JavaSerializedDeserializer(Endpoint endpoint) {
    this.endpoint = endpoint;
  }
  
  public void setAlwaysUseJavaSerialization(boolean val) {
    deserializeOnlyTypeZero = !val; 
  }
  
  public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
    if (deserializeOnlyTypeZero && (type != 0)) throw new IllegalArgumentException("Type must be zero, was "+type+".  See http://freepastry.org/FreePastry/extendingRawMessages.html for more information."); 
    

    Object o = null;
    try {
      byte[] array = new byte[buf.bytesRemaining()];
      buf.read(array);
      
      ObjectInputStream ois = new JavaDeserializer(new ByteArrayInputStream(array), endpoint);
      
      o = ois.readObject();
      Message ret = (Message)o;

      return ret;
    } catch (StreamCorruptedException sce) {
      if (!deserializeOnlyTypeZero)
        throw new RuntimeException("Not a java serialized message!  See http://freepastry.org/FreePastry/extendingRawMessages.html for more information.", sce);
      else 
        throw sce;
//    } catch (ClassCastException e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "PANIC: Serialized message was not a pastry message!");
//      throw new IOException("Message recieved " + o + " was not a pastry message - closing channel.");
    } catch (ClassNotFoundException e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "PANIC: Unknown class type in serialized message!");
      throw new RuntimeException("Unknown class type in message - closing channel.", e);
//    } catch (InvalidClassException e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "PANIC: Serialized message was an invalid class! " + e.getMessage());
//      throw new IOException("Invalid class in message - closing channel.");
//    } catch (IllegalStateException e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "PANIC: Serialized message caused an illegal state exception! " + e.getMessage());
//      throw new IOException("Illegal state from deserializing message - closing channel.");
////    } catch (NullPointerException e) {
////      if (logger.level <= Logger.SEVERE) logger.logException(
////          "PANIC: Serialized message caused a null pointer exception! " , e);
////      
////      return null;
//    } catch (Exception e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "PANIC: Serialized message caused exception! " + e.getMessage());
//      throw new IOException("Exception from deserializing message - closing channel.");
    }
  }

  
  
}
