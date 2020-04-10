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
 * Created on Mar 21, 2006
 */
package rice.p2p.past.rawserialization;

import java.io.*;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.past.*;
import rice.p2p.util.rawserialization.JavaSerializationException;

public class JavaSerializedPastContent implements RawPastContent {
  public static final short TYPE = 0;
  
  public PastContent content;
  
  public JavaSerializedPastContent(PastContent content) {
    this.content = content;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
  
      // write out object and find its length
      oos.writeObject(content);
      oos.close();
      
      byte[] temp = baos.toByteArray();
      buf.writeInt(temp.length);
      buf.write(temp, 0, temp.length);
  //    System.out.println("JavaSerializedPastContent.serialize() "+content+" length:"+temp.length);
  //    new Exception("Stack Trace").printStackTrace();
    } catch (IOException ioe) {
      throw new JavaSerializationException(content, ioe);
    }    
  }

  public short getType() {
    return TYPE;
  }
  
  public String toString() {
    return "JSPC ["+content+"]"; 
  }

  public PastContent getContent() {
    return content;
  }

  public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
    return content.checkInsert(id, existingContent);
  }

  public PastContentHandle getHandle(Past local) {
    return content.getHandle(local);
  }

  public Id getId() {
    return content.getId();
  }

  public boolean isMutable() {
    return content.isMutable();
  }
}
