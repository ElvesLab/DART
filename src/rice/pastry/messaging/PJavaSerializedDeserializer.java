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
 * Created on Mar 17, 2006
 */
package rice.pastry.messaging;

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.pastry.*;
import rice.pastry.NodeHandle;
import rice.pastry.messaging.Message;

/**
 * The purpose of this class is just for programming convienience to disambiguate
 * between rice.p2p.commonapi and rice.pastry with the interfaces/classes
 *   Message
 *   NodeHandle
 *   
 * @author Jeff Hoye
 */
public abstract class PJavaSerializedDeserializer extends JavaSerializedDeserializer {

  public PJavaSerializedDeserializer(PastryNode pn) {
    super(pn);
  }

  public abstract Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException;
  
  public rice.p2p.commonapi.Message deserialize(InputBuffer buf, short type, int priority, rice.p2p.commonapi.NodeHandle sender) throws IOException {
    rice.p2p.commonapi.Message ret = deserialize(buf, type, priority, (NodeHandle)sender);
    if (ret == null) return super.deserialize(buf, type, priority, sender);
    return ret;
  }

}
